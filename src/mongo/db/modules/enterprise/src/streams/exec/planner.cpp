/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/planner.h"

#include <any>
#include <boost/none.hpp>
#include <memory>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/change_stream.hpp>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/oid.h"
#include "mongo/db/change_stream_options_gen.h"
#include "mongo/db/feature_flag.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_merge_modes_gen.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/db/pipeline/lite_parsed_pipeline.h"
#include "mongo/db/service_context.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/serialization_context.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_source_external_api_stub.h"
#include "streams/exec/document_source_validate_stub.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/documents_data_source_operator.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/limit_operator.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/lookup_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/redact_operator.h"
#include "streams/exec/replace_root_operator.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/set_operator.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/sort_operator.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/timeseries_emit_operator.h"
#include "streams/exec/unwind_operator.h"
#include "streams/exec/util.h"
#include "streams/exec/validate_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

// Following static asserts ensure that SerializationOptions.serializeForCloning serialization
// option is still supported by the stages we care about.
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceMatch::clone), decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(std::is_same_v<decltype(&DocumentSourceSingleDocumentTransformation::clone),
                                   decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceRedact::clone), decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceUnwind::clone), decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceGroup::clone), decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceSort::clone), decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceLimit::clone), decltype(&DocumentSource::clone)>);
MONGO_STATIC_ASSERT(
    std::is_same_v<decltype(&DocumentSourceMerge::clone), decltype(&DocumentSource::clone)>);

namespace {

constexpr auto kConnectionNameField = "connectionName"_sd;
constexpr auto kKafkaConnectionType = "kafka"_sd;
constexpr auto kAtlasConnectionType = "atlas"_sd;
constexpr auto kDocumentsField = "documents"_sd;
constexpr auto kNoOpSinkOperatorConnectionName = "__noopSink"_sd;
constexpr auto kCollectSinkOperatorConnectionName = "__collectSink"_sd;
constexpr auto kTimeseriesField = "timeseries"_sd;

enum class StageType {
    kAddFields,
    kMatch,
    kProject,
    kRedact,
    kReplaceRoot,
    kSet,
    kUnwind,
    kMerge,
    kTumblingWindow,
    kHoppingWindow,
    kSessionWindow,
    kValidate,
    kLookUp,
    kGroup,
    kSort,
    kCount,  // This gets converted into DocumentSourceGroup and DocumentSourceProject.
    kLimit,
    kEmit,
    kExternalAPI,
};

// Encapsulates traits of a stage.
struct StageTraits {
    StageType type;
    // Whether the stage is allowed in the main/outer pipeline.
    bool allowedInMainPipeline{false};
    // Whether the stage is allowed in the inner pipeline of a window stage.
    bool allowedInWindowPipeline{false};
};

mongo::stdx::unordered_map<std::string, StageTraits> stageTraits =
    stdx::unordered_map<std::string, StageTraits>{
        {"$addFields", {StageType::kAddFields, true, true}},
        {"$match", {StageType::kMatch, true, true}},
        {"$project", {StageType::kProject, true, true}},
        {"$redact", {StageType::kRedact, true, true}},
        {"$replaceRoot", {StageType::kReplaceRoot, true, true}},
        {"$replaceWith", {StageType::kReplaceRoot, true, true}},
        {"$set", {StageType::kSet, true, true}},
        {"$unset", {StageType::kProject, true, true}},
        {"$unwind", {StageType::kUnwind, true, true}},
        {"$merge", {StageType::kMerge, true, false}},
        {"$tumblingWindow", {StageType::kTumblingWindow, true, false}},
        {"$hoppingWindow", {StageType::kHoppingWindow, true, false}},
        {"$sessionWindow", {StageType::kSessionWindow, true, false}},
        {"$validate", {StageType::kValidate, true, true}},
        {"$lookup", {StageType::kLookUp, true, true}},
        {"$group", {StageType::kGroup, false, true}},
        {"$sort", {StageType::kSort, false, true}},
        {"$count", {StageType::kCount, false, true}},
        {"$limit", {StageType::kLimit, false, true}},
        {"$emit", {StageType::kEmit, true, false}},
        {"$externalAPI", {StageType::kExternalAPI, true, true}},
    };

// Default fast checkpoint interval: 5 minutes.
static constexpr mongo::stdx::chrono::milliseconds kFastCheckpointInterval{5 * 60 * 1000};
// Default slow checkpoint interval: 60 minutes. Used when there is a window serializing its state
// in the execution plan.
static constexpr mongo::stdx::chrono::milliseconds kSlowCheckpointInterval{60 * 60 * 1000};

// Verifies that a stage specified in the input pipeline is a valid stage.
void enforceStageConstraints(const std::string& name, bool isMainPipeline) {
    auto it = stageTraits.find(name);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Unsupported stage: " << name,
            it != stageTraits.end());

    const auto& stageInfo = it->second;
    if (isMainPipeline) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << name
                              << " stage is only permitted in the inner pipeline of a window stage",
                stageInfo.allowedInMainPipeline);
    } else {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << name
                              << " stage is not permitted in the inner pipeline of a window stage",
                stageInfo.allowedInWindowPipeline);
    }
}

// Constructs ValidateOperator::Options.
ValidateOperator::Options makeValidateOperatorOptions(Context* context, BSONObj bsonOptions) {
    auto options = ValidateOptions::parse(IDLParserContext("validate"), bsonOptions);

    std::unique_ptr<MatchExpression> validator;
    if (!options.getValidator().isEmpty()) {
        auto statusWithMatcher =
            MatchExpressionParser::parse(options.getValidator(), context->expCtx);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "failed to parse validator: " << options.getValidator(),
                statusWithMatcher.isOK());
        validator = std::move(statusWithMatcher.getValue());
    } else {
        validator = std::make_unique<AlwaysTrueMatchExpression>();
    }

    if (options.getValidationAction() == mongo::StreamsValidationActionEnum::Dlq) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "DLQ must be specified if validation action is dlq.",
                bool(context->dlq));
    }

    return {std::move(validator), options.getValidationAction()};
}

// Translates MergeOperatorSpec into DocumentSourceMergeSpec.
boost::intrusive_ptr<DocumentSource> makeDocumentSourceMerge(
    const MergeOperatorSpec& mergeOpSpec, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    // TODO: Support kFail whenMatched/whenNotMatched mode.
    static const stdx::unordered_set<MergeWhenMatchedModeEnum> supportedWhenMatchedModes{
        {MergeWhenMatchedModeEnum::kKeepExisting,
         MergeWhenMatchedModeEnum::kMerge,
         MergeWhenMatchedModeEnum::kPipeline,
         MergeWhenMatchedModeEnum::kReplace}};
    static const stdx::unordered_set<MergeWhenNotMatchedModeEnum> supportedWhenNotMatchedModes{
        {MergeWhenNotMatchedModeEnum::kDiscard, MergeWhenNotMatchedModeEnum::kInsert}};

    if (mergeOpSpec.getWhenMatched()) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Unsupported whenMatched mode: ",
                supportedWhenMatchedModes.contains(mergeOpSpec.getWhenMatched()->mode));
    }
    if (mergeOpSpec.getWhenNotMatched()) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Unsupported whenNotMatched mode: ",
                supportedWhenNotMatchedModes.contains(*mergeOpSpec.getWhenNotMatched()));
    }

    DocumentSourceMergeSpec docSourceMergeSpec;
    // Use a dummy target namespace kNoDbCollNamespaceString since it's not used.
    auto dummyTargetNss = NamespaceStringUtil::deserialize(
        /*tenantId=*/boost::none, kNoDbCollNamespaceString, SerializationContext());
    auto whenMatched = mergeOpSpec.getWhenMatched() ? mergeOpSpec.getWhenMatched()->mode
                                                    : DocumentSourceMerge::kDefaultWhenMatched;
    auto whenNotMatched =
        mergeOpSpec.getWhenNotMatched().value_or(DocumentSourceMerge::kDefaultWhenNotMatched);
    auto pipeline =
        mergeOpSpec.getWhenMatched() ? mergeOpSpec.getWhenMatched()->pipeline : boost::none;
    std::set<FieldPath> dummyMergeOnFields{"_id"};
    return DocumentSourceMerge::create(std::move(dummyTargetNss),
                                       expCtx,
                                       whenMatched,
                                       whenNotMatched,
                                       mergeOpSpec.getLet(),
                                       pipeline,
                                       std::move(dummyMergeOnFields),
                                       /*collectionPlacementVersion*/ boost::none,
                                       /*allowMergeOnNullishValues*/ true);
}

// Utility to construct a map of auth options from 'authOptions' for a Kafka connection.
mongo::stdx::unordered_map<std::string, std::string> constructKafkaAuthConfig(
    const KafkaAuthOptions& authOptions) {
    mongo::stdx::unordered_map<std::string, std::string> authConfig;
    if (auto saslMechanism = authOptions.getSaslMechanism(); saslMechanism) {
        authConfig.emplace("sasl.mechanism", KafkaAuthSaslMechanism_serializer(*saslMechanism));
    }
    if (auto saslUsername = authOptions.getSaslUsername(); saslUsername) {
        authConfig.emplace("sasl.username", *saslUsername);
    }
    if (auto saslPassword = authOptions.getSaslPassword(); saslPassword) {
        authConfig.emplace("sasl.password", *saslPassword);
    }
    if (auto securityProtocol = authOptions.getSecurityProtocol(); securityProtocol) {
        authConfig.emplace("security.protocol",
                           KafkaAuthSecurityProtocol_serializer(*securityProtocol));
    }
    if (auto caCertificatePath = authOptions.getCaCertificatePath(); caCertificatePath) {
        authConfig.emplace("ssl.ca.location", *caCertificatePath);
    }
    if (auto tlsAlgorithm = authOptions.getValidateTLSAlgorithm(); tlsAlgorithm) {
        authConfig.emplace("ssl.endpoint.identification.algorithm",
                           KafkaTLSValidationAlgorithm_serializer(*tlsAlgorithm).toString());
    }
    return authConfig;
}

int64_t parseAllowedLateness(const boost::optional<StreamTimeDuration>& param) {
    // From the spec, 3 seconds is the default allowed lateness.
    int64_t allowedLatenessMs = 3 * 1000;
    if (param) {
        auto unit = param->getUnit();
        auto size = param->getSize();
        allowedLatenessMs = toMillis(unit, size);
    }

    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Maximum allowedLateness is 30 minutes",
            allowedLatenessMs <= 30 * 60 * 1000);

    return allowedLatenessMs;
}

boost::intrusive_ptr<mongo::Expression> parseStringOrObjectExpression(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    std::variant<mongo::BSONObj, std::string> exprToParse) {
    auto expression = std::visit(
        OverloadedVisitor{[&](const BSONObj& bson) {
                              return Expression::parseExpression(
                                  expCtx.get(), std::move(bson), expCtx->variablesParseState);
                          },
                          [&](const std::string& str) -> boost::intrusive_ptr<Expression> {
                              return ExpressionFieldPath::parse(
                                  expCtx.get(), std::move(str), expCtx->variablesParseState);
                          }},
        exprToParse);
    return expression;
}

mongo::JsonStringFormat parseJsonStringFormat(
    boost::optional<KafkaEmitJsonStringFormatEnum> exprToParse) {
    mongo::JsonStringFormat returnValue;
    if (!exprToParse) {
        return mongo::JsonStringFormat::ExtendedRelaxedV2_0_0;
    }
    if (*exprToParse == KafkaEmitJsonStringFormatEnum::CanonicalJson) {
        returnValue = mongo::JsonStringFormat::ExtendedCanonicalV2_0_0;
    } else {
        returnValue = mongo::JsonStringFormat::ExtendedRelaxedV2_0_0;
    }
    return returnValue;
}

std::unique_ptr<DocumentTimestampExtractor> createTimestampExtractor(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    boost::optional<std::variant<mongo::BSONObj, std::string>> timeField) {
    if (timeField) {
        return std::make_unique<DocumentTimestampExtractor>(
            expCtx, parseStringOrObjectExpression(expCtx, *timeField));
    } else {
        return nullptr;
    }
}

// Utility which configures options common to all $source stages.
SourceOperator::Options getSourceOperatorOptions(boost::optional<StringData> tsFieldName,
                                                 DocumentTimestampExtractor* timestampExtractor) {
    SourceOperator::Options options;
    if (tsFieldName) {
        options.timestampOutputFieldName = tsFieldName->toString();
        uassert(7756300,
                "'tsFieldOverride' cannot be a dotted path",
                options.timestampOutputFieldName.find('.') == std::string::npos);
    } else {
        options.timestampOutputFieldName = kDefaultTimestampOutputFieldName;
    }
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << ChangeStreamSourceOptions::kTsFieldOverrideFieldName
                          << " cannot be empty",
            !options.timestampOutputFieldName.empty());
    options.timestampExtractor = timestampExtractor;
    return options;
}

// helper function to avoid repetitive code to check if flags have been initialized.
boost::optional<int64_t> getFeatureFlagValue(
    const boost::optional<StreamProcessorFeatureFlags>& flags, const FeatureFlagDefinition& ff) {
    if (flags) {
        boost::optional<int64_t> returnValue;
        if (flags->isOverridden(ff)) {
            return flags->getFeatureFlagValue(ff).getInt();
        }
    }
    return boost::optional<int64_t>{};
}

void configureContextStreamMetaFieldName(Context* context, StringData streamMetaFieldName) {
    // Use metadata field only when the field name is not empty string.
    if (!streamMetaFieldName.empty()) {
        context->streamMetaFieldName = streamMetaFieldName.toString();
    }
}
}  // namespace

Planner::Planner(Context* context, Options options)
    : _context(context), _options(std::move(options)), _nextOperatorId(_options.minOperatorId) {}

void Planner::appendOperator(std::unique_ptr<Operator> oper) {
    if (!_operators.empty()) {
        _operators.back()->addOutput(oper.get(), 0);
    }
    _operators.push_back(std::move(oper));
}

void Planner::planInMemorySource(const BSONObj& sourceSpec,
                                 bool useWatermarks,
                                 bool sendIdleMessages) {
    auto options =
        GeneratedDataSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);
    dassert(options.getConnectionName() == kTestMemoryConnectionName);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());
    configureContextStreamMetaFieldName(_context, options.getStreamMetaFieldName());

    boost::optional<StringData> tsFieldName = options.getTsFieldName();
    if (!tsFieldName) {
        tsFieldName = options.getTsFieldOverride();
    }
    InMemorySourceOperator::Options internalOptions(
        getSourceOperatorOptions(std::move(tsFieldName), _timestampExtractor.get()));
    internalOptions.useWatermarks = useWatermarks;
    internalOptions.sendIdleMessages = sendIdleMessages;

    auto oper = std::make_unique<InMemorySourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planSampleSolarSource(const BSONObj& sourceSpec,
                                    bool useWatermarks,
                                    bool sendIdleMessages) {
    auto options =
        GeneratedDataSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());
    configureContextStreamMetaFieldName(_context, options.getStreamMetaFieldName());

    boost::optional<StringData> tsFieldName = options.getTsFieldName();
    if (!tsFieldName) {
        tsFieldName = options.getTsFieldOverride();
    }
    SampleDataSourceOperator::Options internalOptions(
        getSourceOperatorOptions(std::move(tsFieldName), _timestampExtractor.get()));
    internalOptions.useWatermarks = useWatermarks;
    internalOptions.sendIdleMessages = sendIdleMessages;
    auto oper = std::make_unique<SampleDataSourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planDocumentsSource(const BSONObj& sourceSpec,
                                  bool useWatermarks,
                                  bool sendIdleMessages) {
    auto options =
        DocumentsDataSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());
    configureContextStreamMetaFieldName(_context, options.getStreamMetaFieldName());

    boost::optional<StringData> tsFieldName = options.getTsFieldName();
    if (!tsFieldName) {
        tsFieldName = options.getTsFieldOverride();
    }
    DocumentsDataSourceOperator::Options internalOptions(
        getSourceOperatorOptions(std::move(tsFieldName), _timestampExtractor.get()));
    internalOptions.useWatermarks = useWatermarks;
    internalOptions.sendIdleMessages = sendIdleMessages;
    internalOptions.documents = std::visit(
        OverloadedVisitor{
            [](const std::vector<BSONObj>& bsonDocs) {
                std::vector<Document> docs;
                docs.reserve(bsonDocs.size());
                for (auto& bsonDoc : bsonDocs) {
                    docs.emplace_back(std::move(bsonDoc));
                }
                return docs;
            },
            [&](const BSONObj& bsonExpr) {
                auto expCtx = _context->expCtx;
                auto expr = Expression::parseExpression(
                    expCtx.get(), bsonExpr, expCtx->variablesParseState);
                auto docsArray = expr->evaluate({}, &expCtx->variables);
                uassert(8243600,
                        str::stream()
                            << "The documents list expression does not evaluate to an array.",
                        docsArray.isArray());
                std::vector<Document> docs;
                docs.reserve(docsArray.getArray().size());
                for (const auto& doc : docsArray.getArray()) {
                    uassert(8243601,
                            str::stream() << "The documents list expression does not evaluate to "
                                             "an array of objects.",
                            doc.isObject());
                    docs.emplace_back(doc.getDocument());
                }
                return docs;
            }},
        options.getDocuments());
    auto oper = std::make_unique<DocumentsDataSourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planKafkaSource(const BSONObj& sourceSpec,
                              const KafkaConnectionOptions& baseOptions,
                              bool useWatermarks,
                              bool sendIdleMessages) {
    auto options = KafkaSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());
    configureContextStreamMetaFieldName(_context, options.getStreamMetaFieldName());

    boost::optional<StringData> tsFieldName = options.getTsFieldName();
    if (!tsFieldName) {
        tsFieldName = options.getTsFieldOverride();
    }
    KafkaConsumerOperator::Options internalOptions(
        getSourceOperatorOptions(std::move(tsFieldName), _timestampExtractor.get()));

    internalOptions.bootstrapServers = std::string{baseOptions.getBootstrapServers()};
    std::visit(
        OverloadedVisitor{
            [&](const std::string& str) { internalOptions.topicNames.push_back(str); },
            [&](const std::vector<std::string>& strVec) { internalOptions.topicNames = strVec; }},
        options.getTopic());

    if (options.getTestOnlyPartitionCount()) {
        std::visit(
            OverloadedVisitor{
                [&](int32_t partitionCount) {
                    uassert(ErrorCodes::StreamProcessorInvalidOptions,
                            fmt::format("Expected topicNames size to be: 1, instead found: {}",
                                        internalOptions.topicNames.size()),
                            internalOptions.topicNames.size() == 1);
                    for (int i = 0; i < partitionCount; i++) {
                        internalOptions.testOnlyTopicPartitions.emplace_back(
                            internalOptions.topicNames[0], i);
                    }
                },
                [&](const std::vector<int32_t>& partitionIds) {
                    uassert(ErrorCodes::StreamProcessorInvalidOptions,
                            fmt::format(
                                "mismatch between partitionId count - {} and topicNames size - {}",
                                partitionIds.size(),
                                internalOptions.topicNames.size()),
                            partitionIds.size() == internalOptions.topicNames.size());
                    int idx = 0;
                    for (const auto& topic : internalOptions.topicNames) {
                        int numPartitions = partitionIds[idx++];
                        for (int i = 0; i < numPartitions; i++) {
                            internalOptions.testOnlyTopicPartitions.emplace_back(topic, i);
                        }
                    }
                }},
            *options.getTestOnlyPartitionCount());
    }

    if (auto auth = baseOptions.getAuth(); auth) {
        internalOptions.authConfig = constructKafkaAuthConfig(*auth);
    }

    // The default is to start processing at the current end of topic.
    internalOptions.startOffset = RdKafka::Topic::OFFSET_END;
    auto config = options.getConfig();
    if (config) {
        auto autoOffsetReset = config->getAutoOffsetReset();
        if (autoOffsetReset == KafkaSourceAutoOffsetResetEnum::Smallest ||
            autoOffsetReset == KafkaSourceAutoOffsetResetEnum::Earliest ||
            autoOffsetReset == KafkaSourceAutoOffsetResetEnum::Beginning) {
            internalOptions.startOffset = RdKafka::Topic::OFFSET_BEGINNING;
        }
        internalOptions.keyFormat = config->getKeyFormat();
        internalOptions.keyFormatError = config->getKeyFormatError();
    }

    auto groupIdDefined = config && config->getGroupId();
    if (groupIdDefined) {
        internalOptions.consumerGroupId = std::string{*config->getGroupId()};
    } else {
        internalOptions.consumerGroupId =
            fmt::format("asp-{}-consumer", _context->streamProcessorId);
    }
    _context->kafkaConsumerGroup = internalOptions.consumerGroupId;

    auto enableAutoCommitDefined = config && config->getEnableAutoCommit();
    if (_context->isEphemeral && !groupIdDefined) {
        internalOptions.enableAutoCommit =
            (enableAutoCommitDefined) ? *config->getEnableAutoCommit() : false;
    } else {
        internalOptions.enableAutoCommit =
            (enableAutoCommitDefined) ? *config->getEnableAutoCommit() : true;
    }
    internalOptions.useWatermarks = useWatermarks;
    internalOptions.sendIdleMessages = sendIdleMessages;
    if (internalOptions.useWatermarks) {
        if (auto partitionIdleTimeout = options.getPartitionIdleTimeout()) {
            internalOptions.partitionIdleTimeoutMs = stdx::chrono::milliseconds(
                toMillis(partitionIdleTimeout->getUnit(), partitionIdleTimeout->getSize()));
        }
    }

    _eventDeserializer = std::make_unique<JsonEventDeserializer>();
    internalOptions.deserializer = _eventDeserializer.get();

    if (baseOptions.getIsTestKafka() && *baseOptions.getIsTestKafka()) {
        internalOptions.isTest = true;
    }

    if (baseOptions.getGwproxyEndpoint()) {
        internalOptions.gwproxyEndpoint = baseOptions.getGwproxyEndpoint()->toString();
    }

    if (baseOptions.getGwproxyKey()) {
        internalOptions.gwproxyKey = baseOptions.getGwproxyKey()->toString();
    }

    auto oper = std::make_unique<KafkaConsumerOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planChangeStreamSource(const BSONObj& sourceSpec,
                                     const AtlasConnectionOptions& atlasOptions,
                                     bool useWatermarks,
                                     bool sendIdleMessages) {
    auto options = ChangeStreamSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());
    configureContextStreamMetaFieldName(_context, options.getStreamMetaFieldName());

    MongoCxxClientOptions clientOptions(atlasOptions);
    clientOptions.svcCtx = _context->expCtx->opCtx->getServiceContext();

    auto db = options.getDb();
    if (db) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Cannot specify an empty database name to $source when configuring a change stream",
                !db->empty());
        clientOptions.database = db->toString();
    }

    if (auto coll = options.getColl(); coll) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "If coll is specified, db must be specified.",
                db);
        if (std::holds_alternative<std::string>(*coll)) {
            auto singleColl = std::get<std::string>(*coll);
            clientOptions.collection = singleColl;
        } else {
            clientOptions.collectionList = std::move(std::get<std::vector<std::string>>(*coll));
        }
    }

    boost::optional<StringData> tsFieldName = options.getTsFieldName();
    if (!tsFieldName) {
        tsFieldName = options.getTsFieldOverride();
    }
    ChangeStreamSourceOperator::Options internalOptions(
        getSourceOperatorOptions(std::move(tsFieldName), _timestampExtractor.get()),
        std::move(clientOptions));

    if (useWatermarks) {
        internalOptions.useWatermarks = true;
        internalOptions.sendIdleMessages = sendIdleMessages;
    }

    auto config = options.getConfig();
    if (config) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "startAfter and startAtOperationTime cannot both be set",
                !(config->getStartAfter() && config->getStartAtOperationTime()));

        if (auto startAfter = config->getStartAfter()) {
            internalOptions.userSpecifiedStartingPoint = startAfter->toBSON();
        }

        if (auto startAtOperationTime = config->getStartAtOperationTime()) {
            if (std::holds_alternative<mongo::Date_t>(*startAtOperationTime)) {
                internalOptions.userSpecifiedStartingPoint = Timestamp(
                    duration_cast<mongo::Seconds>(
                        std::get<mongo::Date_t>(*startAtOperationTime).toDurationSinceEpoch()),
                    0);
            } else {
                internalOptions.userSpecifiedStartingPoint =
                    std::get<mongo::Timestamp>(*startAtOperationTime);
            }
        }

        if (auto fullDocument = config->getFullDocument(); fullDocument) {
            internalOptions.fullDocumentMode = *fullDocument;
        }

        if (auto fullDocumentOnly = config->getFullDocumentOnly()) {
            uassert(
                ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "fullDocumentOnly is set to true, fullDocument mode can either be "
                                 "updateLookup or required",
                internalOptions.fullDocumentMode == mongo::FullDocumentModeEnum::kUpdateLookup ||
                    internalOptions.fullDocumentMode == mongo::FullDocumentModeEnum::kRequired);
            internalOptions.fullDocumentOnly = *fullDocumentOnly;
        }

        if (auto fullDocumentBeforeChange = config->getFullDocumentBeforeChange();
            fullDocumentBeforeChange) {
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    "fullDocumentBeforeChange is set, so fullDocumentOnly should not be set.",
                    fullDocumentBeforeChange == FullDocumentBeforeChangeModeEnum::kOff ||
                        !internalOptions.fullDocumentOnly);
            internalOptions.fullDocumentBeforeChangeMode = *fullDocumentBeforeChange;
        }

        if (auto pipeline = config->getPipeline(); pipeline) {
            internalOptions.pipeline = std::move(*pipeline);
        }
    }

    auto oper = std::make_unique<ChangeStreamSourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planSource(const BSONObj& spec, bool useWatermarks, bool sendIdleMessages) {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Invalid $source " << spec,
            spec.firstElementFieldName() == StringData(kSourceStageName) &&
                spec.firstElement().isABSONObj());

    auto sourceSpec = spec.firstElement().Obj();
    // We special case documents list $source since it doesn't require a connection.
    if (sourceSpec.hasElement(kDocumentsField)) {
        planDocumentsSource(sourceSpec, useWatermarks, sendIdleMessages);
        return;
    }

    // Read connectionName field.
    auto connectionField = sourceSpec.getField(kConnectionNameField);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "$source must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Invalid connectionName in " << kSourceStageName << " " << sourceSpec,
            _context->connections.contains(connectionName));

    const auto& connection = _context->connections.at(connectionName);
    switch (connection.getType()) {
        case ConnectionTypeEnum::Kafka: {
            auto options = KafkaConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                         connection.getOptions());
            planKafkaSource(sourceSpec, options, useWatermarks, sendIdleMessages);
            break;
        };
        case ConnectionTypeEnum::SampleSolar: {
            planSampleSolarSource(sourceSpec, useWatermarks, sendIdleMessages);
            break;
        };
        case ConnectionTypeEnum::InMemory: {
            planInMemorySource(sourceSpec, useWatermarks, sendIdleMessages);
            break;
        };
        case ConnectionTypeEnum::Atlas: {
            // We currently assume that an atlas connection implies a change stream $source.
            auto connOptions = AtlasConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                             connection.getOptions());
            planChangeStreamSource(sourceSpec, connOptions, useWatermarks, sendIdleMessages);
            break;
        };
        default:
            uasserted(ErrorCodes::StreamProcessorInvalidOptions,
                      "Only kafka, sample_solar, and atlas source connection type is supported");
    }
}

void Planner::planMergeSink(const BSONObj& spec) {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == StringData(kMergeStageName) &&
                spec.firstElement().isABSONObj());

    auto mergeObj = spec.firstElement().Obj();
    auto mergeOpSpec = MergeOperatorSpec::parse(IDLParserContext("MergeOperatorSpec"), mergeObj);
    auto mergeIntoAtlas =
        AtlasCollection::parse(IDLParserContext("AtlasCollection"), mergeOpSpec.getInto());
    std::string connectionName(mergeIntoAtlas.getConnectionName().toString());

    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Unknown connection name " << connectionName,
            _context->connections.contains(connectionName));

    const auto& connection = _context->connections.at(connectionName);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Only atlas merge connection type is currently supported",
            connection.getType() == ConnectionTypeEnum::Atlas);
    auto atlasOptions = AtlasConnectionOptions::parse(IDLParserContext("AtlasConnectionOptions"),
                                                      connection.getOptions());
    if (_context->expCtx->mongoProcessInterface) {
        dassert(dynamic_cast<StubMongoProcessInterface*>(
            _context->expCtx->mongoProcessInterface.get()));
    }

    auto mergeExpressionCtx =
        make_intrusive<ExpressionContext>(_context->opCtx.get(),
                                          std::unique_ptr<CollatorInterface>(nullptr),
                                          NamespaceString(DatabaseName::kLocal));

    MongoCxxClientOptions clientOptions(atlasOptions);
    clientOptions.svcCtx = _context->expCtx->opCtx->getServiceContext();
    mergeExpressionCtx->mongoProcessInterface =
        std::make_shared<MongoDBProcessInterface>(clientOptions);

    auto documentSource = makeDocumentSourceMerge(mergeOpSpec, mergeExpressionCtx);

    boost::optional<std::set<FieldPath>> onFieldPaths;
    if (mergeOpSpec.getOn()) {
        onFieldPaths.emplace();
        for (const auto& field : *mergeOpSpec.getOn()) {
            const auto [_, inserted] = onFieldPaths->insert(FieldPath(field));
            uassert(8186211,
                    str::stream() << "Found a duplicate field in the $merge.on list: '" << field
                                  << "'",
                    inserted);
        }
    }

    auto specificSource = dynamic_cast<DocumentSourceMerge*>(documentSource.get());
    dassert(specificSource);
    MergeOperator::Options options{.documentSource = specificSource,
                                   .db = mergeIntoAtlas.getDb(),
                                   .coll = mergeIntoAtlas.getColl(),
                                   .onFieldPaths = std::move(onFieldPaths),
                                   .mergeExpCtx = std::move(mergeExpressionCtx)};
    auto oper = std::make_unique<MergeOperator>(_context, std::move(options));
    oper->setOperatorId(_nextOperatorId++);

    invariant(!_operators.empty());
    _pipeline.push_back(std::move(documentSource));
    appendOperator(std::move(oper));
}

void Planner::planEmitSink(const BSONObj& spec) {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == StringData(kEmitStageName) &&
                spec.firstElement().isABSONObj());

    auto sinkSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sinkSpec.getField(kConnectionNameField);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "$emit must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    std::unique_ptr<SinkOperator> sinkOperator;
    if (connectionName == kTestLogConnectionName) {
        sinkOperator = std::make_unique<LogSinkOperator>(_context);
        sinkOperator->setOperatorId(_nextOperatorId++);
    } else if (connectionName == kTestMemoryConnectionName) {
        sinkOperator = std::make_unique<InMemorySinkOperator>(_context, /*numInputs*/ 1);
        sinkOperator->setOperatorId(_nextOperatorId++);
    } else if (connectionName == kNoOpSinkOperatorConnectionName) {
        sinkOperator = std::make_unique<NoOpSinkOperator>(_context);
        sinkOperator->setOperatorId(_nextOperatorId++);
    } else if (connectionName == kCollectSinkOperatorConnectionName) {
        sinkOperator = std::make_unique<CollectOperator>(_context, /*numInputs*/ 1);
        sinkOperator->setOperatorId(_nextOperatorId++);
    } else {
        // 'connectionName' must be in '_context->connections'.
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "Invalid connectionName in " << kEmitStageName << " " << sinkSpec,
                _context->connections.contains(connectionName));
        auto connection = _context->connections.at(connectionName);
        if (connection.getType() == ConnectionTypeEnum::Kafka) {
            auto baseOptions = KafkaConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                             connection.getOptions());

            // TODO(SERVER-94318): This logic was added because the IDL parser throws an unclear
            // error message (see SERVER-94317). This block can be removed once that ticket is
            // resolved.
            const auto kafkaConfig = sinkSpec.getField(KafkaSinkOptions::kConfigFieldName);
            if (kafkaConfig.ok()) {
                IDLParserContext("$emit").checkAndAssertTypes(kafkaConfig,
                                                              std::array<BSONType, 1>{Object});
                const auto kafkaConfigObj = kafkaConfig.Obj();
                const auto kafkaHeader =
                    kafkaConfigObj.getField(KafkaSinkConfigOptions::kHeadersFieldName);
                if (kafkaHeader.ok()) {
                    IDLParserContext("$emit.config")
                        .checkAndAssertTypes(kafkaHeader, std::array<BSONType, 2>{Object, String});
                }
            }

            auto options = KafkaSinkOptions::parse(IDLParserContext(kEmitStageName), sinkSpec);
            KafkaEmitOperator::Options kafkaEmitOptions;
            kafkaEmitOptions.topicName = options.getTopic();
            kafkaEmitOptions.bootstrapServers = baseOptions.getBootstrapServers().toString();
            if (auto auth = baseOptions.getAuth(); auth) {
                kafkaEmitOptions.authConfig = constructKafkaAuthConfig(*auth);
            }
            if (baseOptions.getGwproxyEndpoint()) {
                kafkaEmitOptions.gwproxyEndpoint = baseOptions.getGwproxyEndpoint()->toString();
            }
            if (baseOptions.getGwproxyKey()) {
                kafkaEmitOptions.gwproxyKey = baseOptions.getGwproxyKey()->toString();
            }
            if (options.getConfig()) {
                kafkaEmitOptions.key = options.getConfig()->getKey()
                    ? parseStringOrObjectExpression(_context->expCtx,
                                                    *options.getConfig()->getKey())
                    : nullptr;
                if (kafkaEmitOptions.key) {
                    uassert(
                        ErrorCodes::StreamProcessorInvalidOptions,
                        "Expected config.keyFormat to be specified when config.key is specified",
                        options.getConfig()->getKeyFormat());
                    kafkaEmitOptions.keyFormat = *options.getConfig()->getKeyFormat();
                }
                kafkaEmitOptions.headers = options.getConfig()->getHeaders()
                    ? parseStringOrObjectExpression(_context->expCtx,
                                                    *options.getConfig()->getHeaders())
                    : nullptr;

                if (options.getConfig()->getCompressionType()) {
                    kafkaEmitOptions.compressionType = *options.getConfig()->getCompressionType();
                }

                if (options.getConfig()->getAcks()) {
                    kafkaEmitOptions.acks = *options.getConfig()->getAcks();
                }
            }
            kafkaEmitOptions.jsonStringFormat = options.getConfig()
                ? parseJsonStringFormat(options.getConfig()->getOutputFormat())
                : mongo::JsonStringFormat::ExtendedRelaxedV2_0_0;
            sinkOperator =
                std::make_unique<KafkaEmitOperator>(_context, std::move(kafkaEmitOptions));
            sinkOperator->setOperatorId(_nextOperatorId++);
        } else {
            // $emit to TimeSeries collection
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream() << "Expected Atlas connection for " << kEmitStageName << " "
                                  << sinkSpec,
                    connection.getType() == ConnectionTypeEnum::Atlas);

            auto timeseriesOptions =
                TimeseriesSinkOptions::parse(IDLParserContext("TimeseriesSinkOptions"), sinkSpec);

            auto atlasOptions = AtlasConnectionOptions::parse(
                IDLParserContext("AtlasConnectionOptions"), connection.getOptions());

            MongoCxxClientOptions options(atlasOptions);
            options.svcCtx = _context->opCtx->getServiceContext();
            options.database = timeseriesOptions.getDb().toString();
            options.collection = timeseriesOptions.getColl().toString();
            TimeseriesEmitOperator::Options internalOptions{.clientOptions = std::move(options),
                                                            .timeseriesSinkOptions =
                                                                std::move(timeseriesOptions)};
            sinkOperator =
                std::make_unique<TimeseriesEmitOperator>(_context, std::move(internalOptions));
            sinkOperator->setOperatorId(_nextOperatorId++);
        }
    }

    appendOperator(std::move(sinkOperator));
}

BSONObj Planner::planTumblingWindow(DocumentSource* source) {
    verifyOneWindowStage();
    auto windowSource = dynamic_cast<DocumentSourceTumblingWindowStub*>(source);
    invariant(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();

    auto options = TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    auto offset = options.getOffset();
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Window interval size must be greater than 0.",
            interval.getSize() > 0);

    WindowAssigner::Options windowingOptions;
    windowingOptions.size = interval.getSize();
    windowingOptions.sizeUnit = interval.getUnit();
    windowingOptions.slide = interval.getSize();
    windowingOptions.slideUnit = interval.getUnit();
    windowingOptions.offsetFromUtc = offset ? offset->getOffsetFromUtc() : 0;
    windowingOptions.offsetUnit = offset ? offset->getUnit() : StreamTimeUnitEnum::Millisecond;
    windowingOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    const auto& idleTimeout = options.getIdleTimeout();
    if (idleTimeout) {
        windowingOptions.idleTimeoutSize = idleTimeout->getSize();
        windowingOptions.idleTimeoutUnit = idleTimeout->getUnit();
    }

    _windowPlanningInfo.emplace();
    _windowPlanningInfo->stubDocumentSource = source;
    _windowPlanningInfo->windowAssigner =
        std::make_unique<WindowAssigner>(std::move(windowingOptions));

    std::vector<mongo::BSONObj> ownedPipeline;
    bool needMaintainStreamMeta = true;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
        if (stageName == DocumentSourceGroup::kStageName) {
            needMaintainStreamMeta = false;
        }
    }
    // Window stages will destroy all the stream metadata if the metadata has not been projected
    // into the documents, so we need to project stream metadata prior to sink stage. The only
    // exception is $group because in that case the documents are reshaped and we are not
    // responsible for keeping the original metadata contents.
    if (needMaintainStreamMeta) {
        _context->projectStreamMetaPriorToSinkStage = true;
    }

    auto [pipeline, pipelineRewriter] = preparePipeline(std::move(ownedPipeline));
    if (_options.shouldOptimize) {
        // If we're planning the user pipeline and there's no window aware stage,
        // create a dummy window aware limit to maintain window semantics.
        // Otherwise, if we require metadata to be projected and the first window stage is
        // not window aware, we add a dummy limit operator at the beginning of the pipeline so that
        // the window related metadata can be projected.
        if (_windowPlanningInfo->numWindowAwareStages == 0 ||
            (_context->streamMetaFieldName && _context->projectStreamMetaPriorToSinkStage &&
             !isWindowAwareStage(pipeline->getSources().front()->getSourceName()))) {
            pipeline->addInitialSource(
                DocumentSourceLimit::create(_context->expCtx, std::numeric_limits<int64_t>::max()));
            ++_windowPlanningInfo->numWindowAwareStages;
        }
    }
    auto optimizedPipeline = planPipeline(*pipeline, std::move(pipelineRewriter));

    invariant(_windowPlanningInfo->numWindowAwareStages ==
              _windowPlanningInfo->numWindowAwareStagesPlanned);
    _windowPlanningInfo.reset();
    auto val = getFeatureFlagValue(_context->featureFlags, FeatureFlags::kCheckpointDurationInMs);
    if (val) {
        _context->checkpointInterval = std::chrono::milliseconds(val.get());
    } else {
        _context->checkpointInterval = kSlowCheckpointInterval;
    }
    return serializedWindowStage(
        kTumblingWindowStageName, bsonOptions, std::move(optimizedPipeline));
}

mongo::BSONObj Planner::serializedWindowStage(const std::string& stageName,
                                              mongo::BSONObj spec,
                                              std::vector<mongo::BSONObj> optimizedInnerPipeline) {
    // Return the supplied spec, replace pipeline with the optimized inner pipeline.
    Document stageDoc(spec);
    MutableDocument mutableStage(stageDoc);
    std::vector<Value> innerPipelineArray;
    for (const auto& stage : optimizedInnerPipeline) {
        innerPipelineArray.push_back(Value(stage));
    }
    mutableStage[TumblingWindowOptions::kPipelineFieldName] = Value(innerPipelineArray);
    return BSON(stageName << mutableStage.freeze().toBson());
}

BSONObj Planner::planHoppingWindow(DocumentSource* source) {
    verifyOneWindowStage();
    auto windowSource = dynamic_cast<DocumentSourceHoppingWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();

    auto options = HoppingWindowOptions::parse(IDLParserContext("hoppingWindow"), bsonOptions);
    auto windowInterval = options.getInterval();
    auto hopInterval = options.getHopSize();
    auto offset = options.getOffset();
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Window interval size must be greater than 0.",
            windowInterval.getSize() > 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Window hopSize size must be greater than 0.",
            hopInterval.getSize() > 0);

    WindowAssigner::Options windowingOptions;
    windowingOptions.size = windowInterval.getSize();
    windowingOptions.sizeUnit = windowInterval.getUnit();
    windowingOptions.slide = hopInterval.getSize();
    windowingOptions.slideUnit = hopInterval.getUnit();
    windowingOptions.offsetFromUtc = offset ? offset->getOffsetFromUtc() : 0;
    windowingOptions.offsetUnit = offset ? offset->getUnit() : StreamTimeUnitEnum::Millisecond;
    windowingOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    const auto& idleTimeout = options.getIdleTimeout();
    if (idleTimeout) {
        windowingOptions.idleTimeoutSize = idleTimeout->getSize();
        windowingOptions.idleTimeoutUnit = idleTimeout->getUnit();
    }
    // TODO: what about offset.

    _windowPlanningInfo.emplace();
    _windowPlanningInfo->stubDocumentSource = source;
    _windowPlanningInfo->windowAssigner =
        std::make_unique<WindowAssigner>(std::move(windowingOptions));

    std::vector<mongo::BSONObj> ownedPipeline;
    bool needMaintainStreamMeta = true;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
        if (stageName == DocumentSourceGroup::kStageName) {
            needMaintainStreamMeta = false;
        }
    }
    // Window stages will destroy all the stream metadata if the metadata has not been projected
    // into the documents, so we need to project stream metadata prior to sink stage. The only
    // exception is $group because in that case the documents are reshaped and we are not
    // responsible for keeping the original metadata contents.
    if (needMaintainStreamMeta) {
        _context->projectStreamMetaPriorToSinkStage = true;
    }

    auto [pipeline, pipelineRewriter] = preparePipeline(std::move(ownedPipeline));
    if (_options.shouldOptimize) {
        // If we're planning the user pipeline and there's no window aware stage,
        // create a dummy window aware limit to maintain window semantics.
        // Otherwise, if we require metadata to be projected and the first window stage is
        // not window aware, we add a dummy limit operator at the beginning of the pipeline so that
        // the window related metadata can be projected.
        if ((_windowPlanningInfo->numWindowAwareStages == 0 ||
             (_context->streamMetaFieldName && _context->projectStreamMetaPriorToSinkStage &&
              !isWindowAwareStage(pipeline->getSources().front()->getSourceName())))) {
            pipeline->addInitialSource(
                DocumentSourceLimit::create(_context->expCtx, std::numeric_limits<int64_t>::max()));
            ++_windowPlanningInfo->numWindowAwareStages;
        }
    }
    auto executionPlan = planPipeline(*pipeline, std::move(pipelineRewriter));

    invariant(_windowPlanningInfo->numWindowAwareStages ==
              _windowPlanningInfo->numWindowAwareStagesPlanned);
    _windowPlanningInfo.reset();
    auto val = getFeatureFlagValue(_context->featureFlags, FeatureFlags::kCheckpointDurationInMs);
    if (val) {
        _context->checkpointInterval = std::chrono::milliseconds(val.get());
    } else {
        _context->checkpointInterval = kSlowCheckpointInterval;
    }
    return serializedWindowStage(kHoppingWindowStageName, bsonOptions, std::move(executionPlan));
}

void Planner::prependDummyLimitOperator(mongo::Pipeline* pipeline) {
    invariant(_windowPlanningInfo);
    pipeline->addInitialSource(
        DocumentSourceLimit::create(_context->expCtx, std::numeric_limits<int64_t>::max()));
    ++_windowPlanningInfo->numWindowAwareStages;
}

BSONObj Planner::planSessionWindow(DocumentSource* source) {
    verifyOneWindowStage();
    auto windowSource = dynamic_cast<DocumentSourceSessionWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();

    auto options = SessionWindowOptions::parse(IDLParserContext("sessionWindow"), bsonOptions);

    auto gap = options.getGap();
    boost::intrusive_ptr<mongo::Expression> partitionBy =
        parseStringOrObjectExpression(_context->expCtx, options.getPartitionBy());

    SessionWindowAssigner::Options windowingOptions(WindowAssigner::Options{});

    windowingOptions.gapSize = gap.getSize();
    windowingOptions.gapUnit = gap.getUnit();
    windowingOptions.partitionBy = partitionBy;

    _windowPlanningInfo.emplace();
    _windowPlanningInfo->stubDocumentSource = source;
    _windowPlanningInfo->windowAssigner =
        std::make_unique<SessionWindowAssigner>(std::move(windowingOptions));
    _windowPlanningInfo->isSessionWindow = true;

    std::vector<mongo::BSONObj> ownedPipeline;
    bool needMaintainStreamMeta = true;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
        if (stageName == DocumentSourceGroup::kStageName) {
            needMaintainStreamMeta = false;
        }
    }

    // Window stages will destroy all the stream metadata if the metadata has not been projected
    // into the documents, so we need to project stream metadata prior to sink stage. The only
    // exception is $group because in that case the documents are reshaped and we are not
    // responsible for keeping the original metadata contents.
    if (needMaintainStreamMeta) {
        _context->projectStreamMetaPriorToSinkStage = true;
    }

    auto [pipeline, pipelineRewriter] = preparePipeline(std::move(ownedPipeline));

    if (_options.shouldOptimize) {
        if (_windowPlanningInfo->numBlockingWindowAwareStages == 0) {
            // If there's either no window aware stages, prepend dummy sort operator.
            // You need a blocking window aware operator (i.e. sort) in the pipeline so that
            // _stream_meta.window.start/end values are finalized by the time the document is
            // output.
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    "The $sessionWindow.pipeline isn't supported, there must be a $group or $sort "
                    "in the pipeline.",
                    false);
        } else if (_windowPlanningInfo->streamMetaDependencyBeforeOrAtFirstBlocking) {
            // Else, if there is a read dependency on stream meta before the first blocking, prepend
            // dummy sort. A blocking operator must precede any operator with a _stream_meta
            // dependency so that the _stream_meta.window.start/end values are finalized by the time
            // they are read.
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    "The $sessionWindow.pipeline isn't supported, the pipeline cannot use "
                    "_stream_meta.window until after the first $group or $sort.",
                    false);
        } else if (_windowPlanningInfo->limitBeforeFirstBlocking) {
            // Else, if there's a limit operator before the first blocking window aware operator,
            // prepend dummy sort. A blocking operator must precede a limit operator with limit <
            // INF for window merge to work.
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    "The $sessionWindow.pipeline isn't supported, there cannot be a $limit before "
                    "the first $group or $sort.",
                    false);
        } else if (!isWindowAwareStage(pipeline->getSources().front()->getSourceName())) {
            // Else, if the first operator is NOT window aware, prepend dummy limit
            // If the first operator has a filtering effect (ex. match), a limit operator with limit
            // = INF needs to precede it so documents are not filtered out before they effect
            // session window boundaries.
            prependDummyLimitOperator(pipeline.get());
        }
    }

    auto optimizedPipeline = planPipeline(*pipeline, std::move(pipelineRewriter));

    invariant(_windowPlanningInfo->numWindowAwareStages ==
              _windowPlanningInfo->numWindowAwareStagesPlanned);
    _windowPlanningInfo.reset();
    auto val = getFeatureFlagValue(_context->featureFlags, FeatureFlags::kCheckpointDurationInMs);
    if (val) {
        _context->checkpointInterval = std::chrono::milliseconds(val.get());
    } else {
        _context->checkpointInterval = kSlowCheckpointInterval;
    }
    return serializedWindowStage(
        kSessionWindowStageName, bsonOptions, std::move(optimizedPipeline));
}

mongo::BSONObj Planner::planLookUp(mongo::DocumentSourceLookUp* documentSource,
                                   mongo::BSONObj serializedPlan) {
    auto& lookupPlanningInfo = _lookupPlanningInfos.back();
    auto& stageObj =
        lookupPlanningInfo.rewrittenLookupStages.at(lookupPlanningInfo.numLookupStagesPlanned++)
            .first;
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Invalid lookup spec: " << stageObj,
            isLookUpStage(stageObj.firstElementFieldName()) &&
                stageObj.firstElement().isABSONObj());

    auto lookupObj = stageObj.firstElement().Obj();
    auto fromField = lookupObj[kFromFieldName];
    LookUpOperator::Options options{.documentSource = documentSource};
    if (fromField) {
        auto fromFieldObj = fromField.Obj();
        auto lookupFromAtlas =
            AtlasCollection::parse(IDLParserContext("AtlasCollection"), fromFieldObj);
        std::string connectionName(lookupFromAtlas.getConnectionName().toString());

        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "Unknown connection name " << connectionName,
                _context->connections.contains(connectionName));

        const auto& connection = _context->connections.at(connectionName);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "Only atlas connection type is currently supported for $lookup",
                connection.getType() == ConnectionTypeEnum::Atlas);
        auto atlasOptions = AtlasConnectionOptions::parse(
            IDLParserContext("AtlasConnectionOptions"), connection.getOptions());

        MongoCxxClientOptions clientOptions(atlasOptions);
        clientOptions.svcCtx = _context->expCtx->opCtx->getServiceContext();
        auto foreignMongoDBClient = std::make_shared<MongoDBProcessInterface>(clientOptions);

        options.foreignMongoDBClient = std::move(foreignMongoDBClient);
        options.foreignNs = getNamespaceString(lookupFromAtlas.getDb(), lookupFromAtlas.getColl());
    }

    auto oper = std::make_unique<LookUpOperator>(_context, std::move(options));
    oper->setOperatorId(_nextOperatorId++);
    appendOperator(std::move(oper));

    // Rewrite the lookup pipeline from Server style to Stream style.
    lookupObj = serializedPlan.firstElement().Obj();
    BSONObjBuilder lookupBuilder;
    BSONObjBuilder builder(lookupBuilder.subobjStart(kLookUpStageName));
    for (auto elem : lookupObj) {
        if (elem.fieldNameStringData() == kFromFieldName && fromField) {
            builder.append(fromField);
            continue;
        }
        builder.append(elem);
    }
    builder.doneFast();
    return lookupBuilder.obj();
}

void Planner::planGroup(mongo::DocumentSource* source) {
    auto specificSource = dynamic_cast<DocumentSourceGroup*>(source);
    dassert(specificSource);
    tassert(8358100, "Expected _windowPlannerInfo to be set.", _windowPlanningInfo);
    ++_windowPlanningInfo->numWindowAwareStagesPlanned;

    WindowAwareOperator::Options baseOptions;
    baseOptions.windowAssigner = std::move(_windowPlanningInfo->windowAssigner);
    baseOptions.sendWindowSignals = (_windowPlanningInfo->numWindowAwareStagesPlanned <
                                     _windowPlanningInfo->numWindowAwareStages);
    baseOptions.isSessionWindow = _windowPlanningInfo->isSessionWindow;

    GroupOperator::Options options(std::move(baseOptions));
    options.documentSource = specificSource;
    auto oper = std::make_unique<GroupOperator>(_context, std::move(options));
    oper->setOperatorId(_nextOperatorId++);
    appendOperator(std::move(oper));
}

void Planner::planSort(mongo::DocumentSource* source) {
    auto specificSource = dynamic_cast<DocumentSourceSort*>(source);
    dassert(specificSource);
    tassert(8358101, "Expected _windowPlannerInfo to be set.", _windowPlanningInfo);
    ++_windowPlanningInfo->numWindowAwareStagesPlanned;

    WindowAwareOperator::Options baseOptions;
    baseOptions.windowAssigner = std::move(_windowPlanningInfo->windowAssigner);
    baseOptions.sendWindowSignals = (_windowPlanningInfo->numWindowAwareStagesPlanned <
                                     _windowPlanningInfo->numWindowAwareStages);
    baseOptions.isSessionWindow = _windowPlanningInfo->isSessionWindow;

    SortOperator::Options options(std::move(baseOptions));
    options.documentSource = specificSource;
    auto oper = std::make_unique<SortOperator>(_context, std::move(options));
    oper->setOperatorId(_nextOperatorId++);
    appendOperator(std::move(oper));
}

void Planner::planLimit(mongo::DocumentSource* source) {
    auto specificSource = dynamic_cast<DocumentSourceLimit*>(source);
    tassert(ErrorCodes::InternalError, "Expected a DocumentSourceLimit", specificSource);
    int64_t limitValue = specificSource->getLimit();

    tassert(8358102, "Expected _windowPlannerInfo to be set.", _windowPlanningInfo);
    ++_windowPlanningInfo->numWindowAwareStagesPlanned;

    WindowAwareOperator::Options baseOptions;
    baseOptions.windowAssigner = std::move(_windowPlanningInfo->windowAssigner);
    baseOptions.sendWindowSignals = (_windowPlanningInfo->numWindowAwareStagesPlanned <
                                     _windowPlanningInfo->numWindowAwareStages);
    baseOptions.isSessionWindow = _windowPlanningInfo->isSessionWindow;

    LimitOperator::Options options(std::move(baseOptions));
    options.limit = limitValue;
    auto oper = std::make_unique<LimitOperator>(_context, std::move(options));
    oper->setOperatorId(_nextOperatorId++);
    appendOperator(std::move(oper));
}

std::pair<std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter>,
          std::unique_ptr<PipelineRewriter>>
Planner::preparePipeline(std::vector<mongo::BSONObj> stages) {
    auto pipelineRewriter = std::make_unique<PipelineRewriter>(std::move(stages));
    stages = pipelineRewriter->rewrite();

    // Set resolved namespaces in the ExpressionContext. Currently this is only needed to
    // satisfy the getResolvedNamespace() call in DocumentSourceLookup constructor.
    LiteParsedPipeline liteParsedPipeline(_context->expCtx->ns, stages);
    auto pipelineInvolvedNamespaces = liteParsedPipeline.getInvolvedNamespaces();
    StringMap<ExpressionContext::ResolvedNamespace> resolvedNamespaces;
    for (auto& involvedNs : pipelineInvolvedNamespaces) {
        resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
    }
    _context->expCtx->setResolvedNamespaces(std::move(resolvedNamespaces));
    auto pipeline = Pipeline::parse(stages, _context->expCtx);
    if (_options.shouldOptimize) {
        pipeline->optimizePipeline();
    }

    // Count the number of window aware stages in the pipeline.
    if (_windowPlanningInfo) {
        for (const auto& stage : pipeline->getSources()) {
            if (isWindowAwareStage(stage->getSourceName())) {
                ++_windowPlanningInfo->numWindowAwareStages;
                if (isBlockingWindowAwareStage(stage->getSourceName())) {
                    ++_windowPlanningInfo->numBlockingWindowAwareStages;
                } else if (stage->getSourceName() == kLimitStageName) {
                    _windowPlanningInfo->limitBeforeFirstBlocking |=
                        _windowPlanningInfo->numBlockingWindowAwareStages == 0;
                }
            }
        }
    }

    // Analyze dependencies of stream metadata. We need to project stream meta prior to the sink
    // stage if there is explict dependency..
    if (_context->streamMetaFieldName) {
        int blockingWindowAwareOperators = 0;
        bool hasStreamMetaDependency = false;
        for (const auto& stage : pipeline->getSources()) {
            DepsTracker deps;
            auto depsState = stage->getDependencies(&deps);
            if (depsState == DepsTracker::State::NOT_SUPPORTED) {
                // If the dependency checking is not supported, we assume there is stream metadata
                // dependency to be safe.
                hasStreamMetaDependency = true;
            } else {
                if (deps.needWholeDocument) {
                    // If the stage references $$ROOT then this flag will be set and we should see
                    // it as depending on stream metadata.
                    hasStreamMetaDependency = true;
                }
                for (const auto& field : deps.fields) {
                    if (FieldPath(field).front() == *_context->streamMetaFieldName) {
                        hasStreamMetaDependency = true;
                        break;
                    }
                }
            }
            auto modPaths = stage->getModifiedPaths();
            if (modPaths.type == DocumentSource::GetModPathsReturn::Type::kNotSupported) {
                // If the modified path checking is not supported, we assume there is stream
                // metadata dependency to be safe.
                hasStreamMetaDependency = true;
            } else if (modPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet) {
                if (modPaths.canModify(FieldPath(*_context->streamMetaFieldName))) {
                    hasStreamMetaDependency = true;
                }
            }

            _context->projectStreamMetaPriorToSinkStage |= hasStreamMetaDependency;

            if (isBlockingWindowAwareStage(stage->getSourceName())) {
                ++blockingWindowAwareOperators;
            }
            if (_windowPlanningInfo && hasStreamMetaDependency &&
                blockingWindowAwareOperators <= 1) {
                _windowPlanningInfo->streamMetaDependencyBeforeOrAtFirstBlocking = true;
            }
        }
    }

    return {std::move(pipeline), std::move(pipelineRewriter)};
}

std::vector<BSONObj> Planner::planPipeline(mongo::Pipeline& pipeline,
                                           std::unique_ptr<PipelineRewriter> pipelineRewriter) {
    LookUpPlanningInfo lookupPlanningInfo;
    lookupPlanningInfo.rewrittenLookupStages = pipelineRewriter->getRewrittenLookupStages();
    _lookupPlanningInfos.push_back(std::move(lookupPlanningInfo));

    std::vector<BSONObj> optimizedPipeline;
    optimizedPipeline.reserve(pipeline.getSources().size());

    auto serialize = [&](const boost::intrusive_ptr<DocumentSource>& stage) {
        // Serialize the stage and add it to the execution plan.
        // Window and lookup stages require some special handling and don't use this block.
        std::vector<Value> serializedStage;
        SerializationOptions opts{.serializeForCloning = true};
        stage->serializeToArray(serializedStage, opts);

        // TODO(SERVER-92447): Remove the flag check.
        auto useExecutionPlanFromCheckpoint =
            _context->featureFlags
                ->getFeatureFlagValue(FeatureFlags::kUseExecutionPlanFromCheckpoint)
                .getBool();
        if (useExecutionPlanFromCheckpoint && *useExecutionPlanFromCheckpoint) {
            tassert(8358103,
                    "Expected serializeToArray to return a single BSONObj.",
                    serializedStage.size() == 1);
        } else if (serializedStage.size() != 1) {
            LOGV2_WARNING(9012802,
                          "SerializeToArray returned more than one BSONObj.",
                          "context"_attr = _context);
        }
        return serializedStage[0].getDocument().toBson();
    };

    for (const auto& stage : pipeline.getSources()) {
        const auto& stageInfo = stageTraits[stage->getSourceName()];

        switch (stageInfo.type) {
            case StageType::kAddFields: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource =
                    dynamic_cast<DocumentSourceSingleDocumentTransformation*>(stage.get());
                dassert(specificSource);
                SingleDocumentTransformationOperator::Options options{.documentSource =
                                                                          specificSource};
                auto oper = std::make_unique<AddFieldsOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kSet: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource =
                    dynamic_cast<DocumentSourceSingleDocumentTransformation*>(stage.get());
                dassert(specificSource);
                SingleDocumentTransformationOperator::Options options{.documentSource =
                                                                          specificSource};
                auto oper = std::make_unique<SetOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kMatch: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource = dynamic_cast<DocumentSourceMatch*>(stage.get());
                dassert(specificSource);
                uassert(ErrorCodes::StreamProcessorInvalidOptions,
                        "Cannot use $text in $match stage in Atlas Stream Processing.",
                        !specificSource->isTextQuery());
                MatchOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<MatchOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kProject: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource =
                    dynamic_cast<DocumentSourceSingleDocumentTransformation*>(stage.get());
                dassert(specificSource);
                SingleDocumentTransformationOperator::Options options{.documentSource =
                                                                          specificSource};
                auto oper = std::make_unique<ProjectOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kRedact: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource = dynamic_cast<DocumentSourceRedact*>(stage.get());
                dassert(specificSource);
                RedactOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<RedactOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kReplaceRoot: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource =
                    dynamic_cast<DocumentSourceSingleDocumentTransformation*>(stage.get());
                dassert(specificSource);
                SingleDocumentTransformationOperator::Options options{.documentSource =
                                                                          specificSource};
                auto oper = std::make_unique<ReplaceRootOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kUnwind: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource = dynamic_cast<DocumentSourceUnwind*>(stage.get());
                dassert(specificSource);
                UnwindOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<UnwindOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kValidate: {
                optimizedPipeline.push_back(serialize(stage));
                auto specificSource = dynamic_cast<DocumentSourceValidateStub*>(stage.get());
                dassert(specificSource);
                auto options = makeValidateOperatorOptions(_context, specificSource->bsonOptions());
                auto oper = std::make_unique<ValidateOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kGroup: {
                optimizedPipeline.push_back(serialize(stage));
                planGroup(stage.get());
                break;
            }
            case StageType::kSort: {
                optimizedPipeline.push_back(serialize(stage));
                planSort(stage.get());
                break;
            }
            case StageType::kLimit: {
                optimizedPipeline.push_back(serialize(stage));
                planLimit(stage.get());
                break;
            }
            case StageType::kTumblingWindow: {
                optimizedPipeline.push_back(planTumblingWindow(stage.get()));
                break;
            }
            case StageType::kHoppingWindow: {
                optimizedPipeline.push_back(planHoppingWindow(stage.get()));
                break;
            }
            case StageType::kSessionWindow: {
                auto featureFlags = _context->featureFlags;
                auto sessionWindowEnabled = featureFlags
                    ? featureFlags->getFeatureFlagValue(FeatureFlags::kEnableSessionWindow)
                          .getBool()
                    : boost::none;
                uassert(ErrorCodes::StreamProcessorInvalidOptions,
                        "Unsupported stage: $sessionWindow",
                        sessionWindowEnabled && *sessionWindowEnabled);
                optimizedPipeline.push_back(planSessionWindow(stage.get()));
                break;
            }
            case StageType::kLookUp: {
                auto lookupSource = dynamic_cast<DocumentSourceLookUp*>(stage.get());
                dassert(lookupSource);
                optimizedPipeline.push_back(planLookUp(lookupSource, serialize(stage)));
                break;
            }
            case StageType::kExternalAPI: {
                auto enabled = _context->featureFlags
                    ? _context->featureFlags
                          ->getFeatureFlagValue(FeatureFlags::kEnableExternalAPIOperator)
                          .getBool()
                    : boost::none;
                uassert(ErrorCodes::StreamProcessorInvalidOptions,
                        "Unsupported stage: $externalAPI",
                        enabled && *enabled);
                // TODO(SERVER-95029): Implement initial implementation of $external API operator.
                break;
            }
            default:
                MONGO_UNREACHABLE;
        }
    }

    _lookupPlanningInfos.pop_back();
    _pipeline.splice(_pipeline.end(), std::move(pipeline.getSources()));
    return optimizedPipeline;
}

std::unique_ptr<OperatorDag> Planner::plan(const std::vector<BSONObj>& bsonPipeline) {
    // Set the checkpoint interval. This might be modified if we're planning window stages.
    // If there are no windows in the pipeline or we are using "fast mode" window checkpointing,
    // checkpoints are small. So we write a checkpoint every 5 minutes.
    // If we are using "slow mode" window checkpointing, checkpoints might be large, so we
    // checkpoint every 1 hour.
    auto val = getFeatureFlagValue(_context->featureFlags, FeatureFlags::kCheckpointDurationInMs);
    if (val) {
        _context->checkpointInterval = std::chrono::milliseconds(val.get());
    } else {
        _context->checkpointInterval = kFastCheckpointInterval;
    }

    std::unique_ptr<OperatorDag> result;
    try {
        result = planInner(bsonPipeline);
    } catch (const DBException& e) {
        if (e.code() == ErrorCodes::InternalError || e.code() == ErrorCodes::UnknownError) {
            // We don't expect these errors (even if the user's pipeline is bad), so we throw
            // them as is.
            uasserted(e.code(), e.reason());
        }

        // Other than the error codes above, we treat DBExceptions in plan()
        // as a user error: StreamProcessorInvalidOptions.
        uasserted(ErrorCodes::StreamProcessorInvalidOptions, e.toString());
    }

    if (_options.shouldValidateModifyRequest) {
        tassert(ErrorCodes::InternalError,
                "shouldOptimize should be true when validating a pipeline edit.",
                _options.shouldOptimize);
        tassert(ErrorCodes::InternalError,
                "restoredCheckpointUserPipeline should be set when validating a pipeline edit.",
                _context->restoredCheckpointInfo &&
                    !_context->restoredCheckpointInfo->userPipeline.empty());
        validatePipelineModify(_context->restoredCheckpointInfo->userPipeline, bsonPipeline);
    }

    return result;
}

std::unique_ptr<OperatorDag> Planner::planInner(const std::vector<BSONObj>& bsonPipeline) {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Pipeline must have at least one stage",
            !bsonPipeline.empty());
    std::string firstStageName(bsonPipeline.begin()->firstElementFieldNameStringData());
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "First stage must be " << kSourceStageName
                          << ", found: " << firstStageName,
            isSourceStage(firstStageName));
    std::string lastStageName(bsonPipeline.rbegin()->firstElementFieldNameStringData());
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "The last stage in the pipeline must be $merge or $emit.",
            isSinkStage(lastStageName) || _context->isEphemeral);

    // Validate each stage BSONObj is well formatted.
    for (const auto& stage : bsonPipeline) {
        // This is the same error that LiteParsedDocumentSource will throw for aggregate.
        uassert(8661200,
                "A pipeline stage specification object must contain exactly one field.",
                stage.nFields() == 1);
    }

    std::vector<BSONObj> optimizedPipeline;

    // Get the $source BSON.
    auto current = bsonPipeline.begin();
    if (current != bsonPipeline.end() &&
        isSourceStage(current->firstElementFieldNameStringData())) {
        // Build the DAG, start with the $source
        auto sourceSpec = *current;

        // We only use watermarks when the pipeline contains a window stage.
        bool useWatermarks{false};
        // We only send idle watermarks if the window idleTimeout is set.
        bool sendIdleMessages{false};
        for (const BSONObj& stage : bsonPipeline) {
            const auto& name = stage.firstElementFieldNameStringData();
            if (isWindowStage(name)) {
                useWatermarks = true;
                auto windowOptions = stage.getField(name);
                sendIdleMessages = windowOptions.type() == BSONType::Object &&
                    (windowOptions.Obj().hasElement(HoppingWindowOptions::kIdleTimeoutFieldName) ||
                     windowOptions.Obj().hasElement(TumblingWindowOptions::kIdleTimeoutFieldName));
                break;
            }
        }

        // Create the source operator
        optimizedPipeline.push_back(sourceSpec);
        planSource(sourceSpec, useWatermarks, sendIdleMessages);
        ++current;
    }

    // Get the middle stages until we hit a sink stage
    std::vector<BSONObj> middleStages;
    while (current != bsonPipeline.end() &&
           !isSinkStage(current->firstElementFieldNameStringData())) {
        std::string stageName(current->firstElementFieldNameStringData());
        enforceStageConstraints(stageName, true /* isMainPipeline */);

        middleStages.emplace_back(*current);
        ++current;
    }

    // Then everything between the source and the $merge/$emit
    if (!middleStages.empty()) {
        auto [pipeline, pipelineRewriter] = preparePipeline(std::move(middleStages));
        auto pipelineExecutionPlan = planPipeline(*pipeline, std::move(pipelineRewriter));
        optimizedPipeline.insert(optimizedPipeline.end(),
                                 std::make_move_iterator(pipelineExecutionPlan.begin()),
                                 std::make_move_iterator(pipelineExecutionPlan.end()));
    }

    // After the loop above, current is either pointing to a sink
    // or the end.
    BSONObj sinkSpec;
    if (current == bsonPipeline.end()) {
        // We're at the end of the bsonPipeline and we have not found a sink stage.
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "The last stage in the pipeline must be $merge or $emit.",
                _context->isEphemeral);
        // In the ephemeral case, we append a NoOpSink to handle the sample requests.
        sinkSpec =
            BSON(kEmitStageName << BSON(kConnectionNameField << kNoOpSinkOperatorConnectionName));
    } else {
        sinkSpec = *current;
        invariant(isSinkStage(sinkSpec.firstElementFieldNameStringData()));
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "No stages are allowed after a $merge or $emit stage.",
                std::next(current) == bsonPipeline.end());
    }

    if (!sinkSpec.isEmpty()) {
        optimizedPipeline.push_back(sinkSpec);

        auto sinkStageName = sinkSpec.firstElementFieldNameStringData();
        if (isMergeStage(sinkStageName)) {
            planMergeSink(sinkSpec);
        } else {
            dassert(isEmitStage(sinkStageName));
            planEmitSink(sinkSpec);
        }
    }

    OperatorDag::Options options;
    options.inputPipeline = bsonPipeline;
    options.optimizedPipeline = std::move(optimizedPipeline);
    options.pipeline = std::move(_pipeline);
    options.timestampExtractor = std::move(_timestampExtractor);
    options.eventDeserializer = std::move(_eventDeserializer);
    auto dag = make_unique<OperatorDag>(std::move(options), std::move(_operators));

    // Validate the operator IDs in the dag.
    for (size_t idx = 0; idx < dag->operators().size(); ++idx) {
        const auto& op = dag->operators()[idx];
        // TODO(SERVER-91717): Promote this to a tassert once it's rolled out globally
        // and we're sure no existing processors will run into it.
        dassert(op->getOperatorId() == OperatorId(idx));
        if (op->getOperatorId() != OperatorId(idx)) {
            LOGV2_WARNING(9012801,
                          "Operator had unexpected operatorId",
                          "context"_attr = _context,
                          "name"_attr = op->getName(),
                          "index"_attr = idx,
                          "operatorId"_attr = op->getOperatorId());
        }
    }

    return dag;
}

std::vector<ParsedConnectionInfo> Planner::parseConnectionInfo(
    const std::vector<BSONObj>& pipeline) {
    std::vector<ParsedConnectionInfo> connectionNames;
    for (auto& stage : pipeline) {
        uassert(mongo::ErrorCodes::StreamProcessorInvalidOptions,
                str::stream()
                    << "A pipeline stage specification object must contain exactly one field: "
                    << stage,
                stage.nFields() == 1);
        auto stageName = stage.firstElementFieldNameStringData();

        auto addConnectionName = [&](StringData stage, const Document& doc, const FieldPath& fp) {
            auto connectionField = doc.getNestedField(fp);
            uassert(
                mongo::ErrorCodes::StreamProcessorInvalidOptions,
                str::stream() << "Stage spec must contain a 'connectionName' string field in it: "
                              << stage,
                connectionField.getType() == String);
            ParsedConnectionInfo info{connectionField.getString()};
            info.setStage(stage.toString());
            connectionNames.push_back(std::move(info));
        };
        auto getSpecDoc = [](StringData stageName, const BSONObj& stageBson) {
            uassert(mongo::ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream() << stageName << " specification must be an object.",
                    stageBson.firstElement().isABSONObj());
            return Document(stageBson.firstElement().Obj());
        };

        if (isSourceStage(stageName)) {
            auto spec = getSpecDoc(stageName, stage);
            // We special case $source.documents because it doesn't require a connection.
            if (spec[kDocumentsField].missing()) {
                addConnectionName(stageName, spec, FieldPath(kConnectionNameField));
            }
        } else if (isEmitStage(stageName)) {
            auto spec = getSpecDoc(stageName, stage);
            addConnectionName(stageName, spec, FieldPath(kConnectionNameField));
        } else if (isMergeStage(stageName)) {
            auto spec = getSpecDoc(stageName, stage);
            addConnectionName(stageName,
                              spec,
                              FieldPath((str::stream() << MergeOperatorSpec::kIntoFieldName << "."
                                                       << kConnectionNameField)
                                            .ss.str()));
        } else if (isLookUpStage(stageName)) {
            auto spec = getSpecDoc(stageName, stage);
            // If the 'from' field does not exist in the $lookup stage, there is no connection
            // information to retrieve. This scenario is valid when a 'pipeline' field is defined.
            if (!spec.getField(kFromFieldName).missing()) {
                addConnectionName(
                    stageName,
                    spec,
                    FieldPath(
                        (str::stream() << kFromFieldName << "." << kConnectionNameField).ss.str()));
            }
        } else if (isWindowStage(stageName)) {
            uassert(mongo::ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream()
                        << "A pipeline stage specification object must contain exactly one field: "
                        << stageName,
                    stage.firstElement().isABSONObj());
            auto specBson = stage.firstElement().Obj();
            std::vector<mongo::BSONObj> windowPipeline;
            if (stageName == kTumblingWindowStageName) {
                windowPipeline =
                    TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), specBson)
                        .getPipeline();
            } else if (stageName == kHoppingWindowStageName) {
                windowPipeline =
                    HoppingWindowOptions::parse(IDLParserContext("hoppingWindow"), specBson)
                        .getPipeline();
            } else {
                invariant(stageName == kSessionWindowStageName);
                windowPipeline =
                    SessionWindowOptions::parse(IDLParserContext("sessionWindow"), specBson)
                        .getPipeline();
            }

            for (const auto& windowStage : windowPipeline) {
                uassert(
                    mongo::ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream()
                        << "A pipeline stage specification object must contain exactly one field: "
                        << windowStage,
                    windowStage.nFields() == 1);
                auto windowStageName = windowStage.firstElementFieldNameStringData();
                if (isLookUpStage(windowStageName)) {
                    auto windowStageSpec = getSpecDoc(windowStageName, windowStage);
                    // If the 'from' field does not exist in the $lookup stage, there is no
                    // connection information to retrieve. This scenario is valid when a 'pipeline'
                    // field is defined.
                    if (!windowStageSpec.getField(kFromFieldName).missing()) {
                        addConnectionName(windowStageName,
                                          windowStageSpec,
                                          FieldPath((str::stream() << kFromFieldName << "."
                                                                   << kConnectionNameField)
                                                        .ss.str()));
                    }
                }
            }
        }
    }
    return connectionNames;
}

void Planner::verifyOneWindowStage() {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Only one window stage is allowed in a pipeline.",
            !_hasWindow);
    _hasWindow = true;
}

void Planner::validatePipelineModify(const std::vector<mongo::BSONObj>& oldUserPipeline,
                                     const std::vector<mongo::BSONObj>& newUserPipeline) {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Pipeline must have at least 1 stage.",
            !oldUserPipeline.empty() && !newUserPipeline.empty());

    // Validate the old and new $source exactly match.
    const auto& oldSourceSpec = oldUserPipeline[0];
    const auto& newSourceSpec = newUserPipeline[0];
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "resumeFromCheckpoint must be false to modify a stream processor's $source stage",
            SimpleBSONObjComparator::kInstance.evaluate(oldSourceSpec == newSourceSpec));

    auto hasWindow = [](const std::vector<BSONObj>& pipeline) {
        for (const auto& stage : pipeline) {
            if (isWindowStage(stage.firstElementFieldNameStringData())) {
                return true;
            }
        }
        return false;
    };
    bool oldHasWindow = hasWindow(oldUserPipeline);
    bool newHasWindow = hasWindow(newUserPipeline);
    if (oldHasWindow && newHasWindow) {
        // TODO(SERVER-94179): Remove this restriction. Change this to validate the
        // window type and boundary has not changed. Also, we need to validate
        // the source offsets still exist.
        uasserted(ErrorCodes::StreamProcessorInvalidOptions,
                  "resumeFromCheckpoint must be false to modify a stream processor with a window");
    }
    if (!oldHasWindow && newHasWindow) {
        // TODO(SERVER-95185): Support adding a window stage with resumeFromCheckpoint=true.
        uasserted(ErrorCodes::StreamProcessorInvalidOptions,
                  "resumeFromCheckpoint must be false to add a window to a stream processor");
    }
    if (oldHasWindow && !newHasWindow) {
        uasserted(ErrorCodes::StreamProcessorInvalidOptions,
                  "resumeFromCheckpoint must be false to remove a window from a stream processor");
    }
}

};  // namespace streams
