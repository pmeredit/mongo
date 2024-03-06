/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/planner.h"

#include <boost/none.hpp>
#include <memory>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/change_stream.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/change_stream_options_gen.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_merge_modes_gen.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/db/pipeline/lite_parsed_pipeline.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/serialization_context.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_source_validate_stub.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/document_timestamp_extractor.h"
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
#include "streams/exec/test_constants.h"
#include "streams/exec/unwind_operator.h"
#include "streams/exec/util.h"
#include "streams/exec/validate_operator.h"
#include "streams/exec/window_aware_group_operator.h"
#include "streams/exec/window_aware_limit_operator.h"
#include "streams/exec/window_aware_sort_operator.h"
#include "streams/exec/window_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

constexpr auto kConnectionNameField = "connectionName"_sd;
constexpr auto kKafkaConnectionType = "kafka"_sd;
constexpr auto kAtlasConnectionType = "atlas"_sd;
constexpr auto kNoOpSinkOperatorConnectionName = "__noopSink"_sd;
constexpr auto kCollectSinkOperatorConnectionName = "__collectSink"_sd;

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
    kValidate,
    kLookUp,
    kGroup,
    kSort,
    kCount,  // This gets converted into DocumentSourceGroup and DocumentSourceProject.
    kLimit,
    kEmit,
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
        {"$validate", {StageType::kValidate, true, true}},
        {"$lookup", {StageType::kLookUp, true, true}},
        {"$group", {StageType::kGroup, false, true}},
        {"$sort", {StageType::kSort, false, true}},
        {"$count", {StageType::kCount, false, true}},
        {"$limit", {StageType::kLimit, false, true}},
        {"$emit", {StageType::kEmit, true, false}},
    };

// Default fast checkpoint interval: 5 minutes.
static constexpr mongo::stdx::chrono::milliseconds kFastCheckpointInterval{5 * 60 * 1000};
// Default slow checkpoint interval: 60 minutes. Used when there is a window serializing its state
// in the execution plan.
static constexpr mongo::stdx::chrono::milliseconds kSlowCheckpointInterval{60 * 60 * 1000};

// Verifies that a stage specified in the input pipeline is a valid stage.
void enforceStageConstraints(const std::string& name, bool isMainPipeline) {
    auto it = stageTraits.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Unsupported stage: " << name,
            it != stageTraits.end());

    const auto& stageInfo = it->second;
    if (isMainPipeline) {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << name
                              << " stage is not permitted in the inner pipeline of a window stage",
                stageInfo.allowedInMainPipeline);
    } else {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << name
                              << " stage is only permitted in the inner pipeline of a window stage",
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
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "failed to parse validator: " << options.getValidator(),
                statusWithMatcher.isOK());
        validator = std::move(statusWithMatcher.getValue());
    } else {
        validator = std::make_unique<AlwaysTrueMatchExpression>();
    }

    if (options.getValidationAction() == mongo::StreamsValidationActionEnum::Dlq) {
        uassert(ErrorCodes::InvalidOptions,
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
        uassert(ErrorCodes::InvalidOptions,
                "Unsupported whenMatched mode: ",
                supportedWhenMatchedModes.contains(mergeOpSpec.getWhenMatched()->mode));
    }
    if (mergeOpSpec.getWhenNotMatched()) {
        uassert(ErrorCodes::InvalidOptions,
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
                                       /*collectionPlacementVersion*/ boost::none);
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

    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Maximum allowedLateness is 30 minutes",
            allowedLatenessMs <= 30 * 60 * 1000);

    return allowedLatenessMs;
}

std::unique_ptr<DocumentTimestampExtractor> createTimestampExtractor(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    boost::optional<mongo::BSONObj> timeField) {
    if (timeField) {
        return std::make_unique<DocumentTimestampExtractor>(
            expCtx,
            Expression::parseExpression(
                expCtx.get(), std::move(*timeField), expCtx->variablesParseState));
    } else {
        return nullptr;
    }
}

// Utility which configures options common to all $source stages.
SourceOperator::Options getSourceOperatorOptions(boost::optional<StringData> tsFieldOverride,
                                                 DocumentTimestampExtractor* timestampExtractor) {
    SourceOperator::Options options;
    if (tsFieldOverride) {
        options.timestampOutputFieldName = tsFieldOverride->toString();
        uassert(7756300,
                "'tsFieldOverride' cannot be a dotted path",
                options.timestampOutputFieldName.find('.') == std::string::npos);
    } else {
        options.timestampOutputFieldName = kDefaultTimestampOutputFieldName;
    }
    uassert(ErrorCodes::InternalError,
            str::stream() << ChangeStreamSourceOptions::kTsFieldOverrideFieldName
                          << " cannot be empty",
            !options.timestampOutputFieldName.empty());

    options.timestampExtractor = timestampExtractor;
    return options;
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

    InMemorySourceOperator::Options internalOptions(
        getSourceOperatorOptions(options.getTsFieldOverride(), _timestampExtractor.get()));
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

    SampleDataSourceOperator::Options internalOptions(
        getSourceOperatorOptions(options.getTsFieldOverride(), _timestampExtractor.get()));
    internalOptions.useWatermarks = useWatermarks;
    internalOptions.sendIdleMessages = sendIdleMessages;
    auto oper = std::make_unique<SampleDataSourceOperator>(_context, std::move(internalOptions));
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

    KafkaConsumerOperator::Options internalOptions(
        getSourceOperatorOptions(options.getTsFieldOverride(), _timestampExtractor.get()));

    internalOptions.bootstrapServers = std::string{baseOptions.getBootstrapServers()};
    internalOptions.topicName = std::string{options.getTopic()};
    internalOptions.testOnlyNumPartitions = options.getTestOnlyPartitionCount();

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
    }

    if (config && config->getGroupId()) {
        internalOptions.consumerGroupId = std::string{*config->getGroupId()};
    } else {
        internalOptions.consumerGroupId =
            fmt::format("asp-{}-consumer", _context->streamProcessorId);
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


    MongoCxxClientOptions clientOptions(atlasOptions);
    clientOptions.svcCtx = _context->expCtx->opCtx->getServiceContext();

    auto db = options.getDb();
    uassert(ErrorCodes::InvalidOptions,
            "Cannot specify a non-empty database name to $source when configuring a change stream",
            !db.empty());

    clientOptions.database = db.toString();
    if (auto coll = options.getColl(); coll) {
        clientOptions.collection = coll->toString();
    }

    ChangeStreamSourceOperator::Options internalOptions(
        getSourceOperatorOptions(options.getTsFieldOverride(), _timestampExtractor.get()),
        std::move(clientOptions));

    if (useWatermarks) {
        internalOptions.useWatermarks = true;
        internalOptions.sendIdleMessages = sendIdleMessages;
    }

    auto config = options.getConfig();
    if (config) {
        uassert(ErrorCodes::InvalidOptions,
                "startAfter and startAtOperationTime cannot both be set",
                !(config->getStartAfter() && config->getStartAtOperationTime()));

        if (auto startAfter = config->getStartAfter()) {
            internalOptions.userSpecifiedStartingPoint = startAfter->toBSON();
        }

        if (auto startAtOperationTime = config->getStartAtOperationTime()) {
            internalOptions.userSpecifiedStartingPoint = *startAtOperationTime;
        }

        if (auto fullDocument = config->getFullDocument(); fullDocument) {
            internalOptions.fullDocumentMode = *fullDocument;
        }

        if (auto fullDocumentOnly = config->getFullDocumentOnly()) {
            uassert(
                ErrorCodes::InvalidOptions,
                str::stream() << "fullDocumentOnly is set to true, fullDocument mode can either be "
                                 "updateLookup or required",
                internalOptions.fullDocumentMode == mongo::FullDocumentModeEnum::kUpdateLookup ||
                    internalOptions.fullDocumentMode == mongo::FullDocumentModeEnum::kRequired);
            internalOptions.fullDocumentOnly = *fullDocumentOnly;
        }

        if (auto fullDocumentBeforeChange = config->getFullDocumentBeforeChange();
            fullDocumentBeforeChange) {
            uassert(ErrorCodes::InvalidOptions,
                    "fullDocumentBeforeChange is set, so fullDocumentOnly should not be set.",
                    fullDocumentBeforeChange == FullDocumentBeforeChangeModeEnum::kOff ||
                        !internalOptions.fullDocumentOnly);
            internalOptions.fullDocumentBeforeChangeMode = *fullDocumentBeforeChange;
        }
    }

    auto oper = std::make_unique<ChangeStreamSourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planSource(const BSONObj& spec, bool useWatermarks, bool sendIdleMessages) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid $source " << spec,
            spec.firstElementFieldName() == StringData(kSourceStageName) &&
                spec.firstElement().isABSONObj());

    auto sourceSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sourceSpec.getField(kConnectionNameField);
    uassert(ErrorCodes::InvalidOptions,
            "$source must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    uassert(ErrorCodes::InvalidOptions,
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
            uasserted(ErrorCodes::InvalidOptions,
                      "Only kafka, sample_solar, and atlas source connection type is supported");
    }
}

void Planner::planMergeSink(const BSONObj& spec) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == StringData(kMergeStageName) &&
                spec.firstElement().isABSONObj());

    auto mergeObj = spec.firstElement().Obj();
    auto mergeOpSpec = MergeOperatorSpec::parse(IDLParserContext("MergeOperatorSpec"), mergeObj);
    auto mergeIntoAtlas =
        AtlasCollection::parse(IDLParserContext("AtlasCollection"), mergeOpSpec.getInto());
    std::string connectionName(mergeIntoAtlas.getConnectionName().toString());

    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Unknown connection name " << connectionName,
            _context->connections.contains(connectionName));

    const auto& connection = _context->connections.at(connectionName);
    uassert(ErrorCodes::InvalidOptions,
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
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == StringData(kEmitStageName) &&
                spec.firstElement().isABSONObj());

    auto sinkSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sinkSpec.getField(kConnectionNameField);
    uassert(ErrorCodes::InvalidOptions,
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
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Invalid connectionName in " << kEmitStageName << " " << sinkSpec,
                _context->connections.contains(connectionName));

        auto connection = _context->connections.at(connectionName);
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Expected kafka connection for " << kEmitStageName << " "
                              << sinkSpec,
                connection.getType() == ConnectionTypeEnum::Kafka);

        auto baseOptions = KafkaConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                         connection.getOptions());
        auto options = KafkaSinkOptions::parse(IDLParserContext(kEmitStageName), sinkSpec);
        KafkaEmitOperator::Options kafkaEmitOptions;
        kafkaEmitOptions.topicName = options.getTopic();
        kafkaEmitOptions.bootstrapServers = baseOptions.getBootstrapServers().toString();
        if (auto auth = baseOptions.getAuth(); auth) {
            kafkaEmitOptions.authConfig = constructKafkaAuthConfig(*auth);
        }

        sinkOperator = std::make_unique<KafkaEmitOperator>(_context, std::move(kafkaEmitOptions));
        sinkOperator->setOperatorId(_nextOperatorId++);
    }

    appendOperator(std::move(sinkOperator));
}

void Planner::planTumblingWindow(DocumentSource* source) {
    auto windowSource = dynamic_cast<DocumentSourceTumblingWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();

    auto options = TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    auto offset = options.getOffset();
    uassert(ErrorCodes::InvalidOptions,
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
    _windowPlanningInfo->windowingOptions = std::move(windowingOptions);

    std::vector<mongo::BSONObj> ownedPipeline;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
    }

    planInner(ownedPipeline);
    invariant(_windowPlanningInfo->numWindowAwareStages ==
              _windowPlanningInfo->numWindowAwareStagesPlanned);
    if (_windowPlanningInfo->numWindowAwareStages == 0) {
        // Add a dummy limit stage operator to the plan to add windowing metadata to _stream_meta.
        planLimit(/*source*/ nullptr);
    }
    _windowPlanningInfo.reset();
    _context->checkpointInterval = kSlowCheckpointInterval;
}

void Planner::planHoppingWindow(DocumentSource* source) {
    auto windowSource = dynamic_cast<DocumentSourceHoppingWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();

    auto options = HoppingWindowOptions::parse(IDLParserContext("hoppingWindow"), bsonOptions);
    auto windowInterval = options.getInterval();
    auto hopInterval = options.getHopSize();
    uassert(ErrorCodes::InvalidOptions,
            "Window interval size must be greater than 0.",
            windowInterval.getSize() > 0);
    uassert(ErrorCodes::InvalidOptions,
            "Window hopSize size must be greater than 0.",
            hopInterval.getSize() > 0);

    WindowAssigner::Options windowingOptions;
    windowingOptions.size = windowInterval.getSize();
    windowingOptions.sizeUnit = windowInterval.getUnit();
    windowingOptions.slide = hopInterval.getSize();
    windowingOptions.slideUnit = hopInterval.getUnit();
    windowingOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    const auto& idleTimeout = options.getIdleTimeout();
    if (idleTimeout) {
        windowingOptions.idleTimeoutSize = idleTimeout->getSize();
        windowingOptions.idleTimeoutUnit = idleTimeout->getUnit();
    }
    // TODO: what about offset.

    _windowPlanningInfo.emplace();
    _windowPlanningInfo->stubDocumentSource = source;
    _windowPlanningInfo->windowingOptions = std::move(windowingOptions);

    std::vector<mongo::BSONObj> ownedPipeline;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
    }

    planInner(ownedPipeline);
    invariant(_windowPlanningInfo->numWindowAwareStages ==
              _windowPlanningInfo->numWindowAwareStagesPlanned);
    if (_windowPlanningInfo->numWindowAwareStages == 0) {
        // Add a dummy limit stage operator to the plan to add windowing metadata to _stream_meta.
        planLimit(/*source*/ nullptr);
    }
    _windowPlanningInfo.reset();
    _context->checkpointInterval = kSlowCheckpointInterval;
}

void Planner::planTumblingWindowLegacy(DocumentSource* source) {
    auto windowSource = dynamic_cast<DocumentSourceTumblingWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();
    // Reserve the next OperatorId for this WindowOperator.
    auto operatorId = _nextOperatorId++;

    auto options = TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    auto offset = options.getOffset();
    auto size = interval.getSize();
    auto allowedLateness = options.getAllowedLateness();

    std::vector<mongo::BSONObj> ownedPipeline;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
    }
    uassert(ErrorCodes::InvalidOptions, "Window interval size must be greater than 0.", size > 0);

    std::pair<OperatorId, OperatorId> minMaxOperatorIds;
    minMaxOperatorIds.first = _nextOperatorId;

    Planner::Options plannerOptions;
    plannerOptions.planMainPipeline = false;
    plannerOptions.minOperatorId = _nextOperatorId;
    auto planner = std::make_unique<Planner>(_context, std::move(plannerOptions));
    auto operatorDag = planner->plan(ownedPipeline);

    _nextOperatorId += operatorDag->operators().size();
    minMaxOperatorIds.second = _nextOperatorId - 1;
    invariant(minMaxOperatorIds.second >= minMaxOperatorIds.first);

    WindowOperator::Options windowOpOptions;
    windowOpOptions.size = size;
    windowOpOptions.sizeUnit = interval.getUnit();
    windowOpOptions.slide = size;
    windowOpOptions.slideUnit = interval.getUnit();
    windowOpOptions.offsetFromUtc = offset ? offset->getOffsetFromUtc() : 0;
    windowOpOptions.offsetUnit = offset ? offset->getUnit() : StreamTimeUnitEnum::Millisecond;
    windowOpOptions.pipeline = std::move(ownedPipeline);
    windowOpOptions.minMaxOperatorIds = std::move(minMaxOperatorIds);
    const auto& idleTimeout = options.getIdleTimeout();
    if (idleTimeout) {
        windowOpOptions.idleTimeoutSize = idleTimeout->getSize();
        windowOpOptions.idleTimeoutUnit = idleTimeout->getUnit();
    }
    windowOpOptions.allowedLatenessMs = parseAllowedLateness(allowedLateness);
    auto oper = std::make_unique<WindowOperator>(_context, std::move(windowOpOptions));
    oper->setOperatorId(operatorId);
    appendOperator(std::move(oper));
}

void Planner::planHoppingWindowLegacy(DocumentSource* source) {
    auto windowSource = dynamic_cast<DocumentSourceHoppingWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();
    // Reserve the next OperatorId for this WindowOperator.
    auto operatorId = _nextOperatorId++;

    auto options = HoppingWindowOptions::parse(IDLParserContext("hoppingWindow"), bsonOptions);
    auto windowInterval = options.getInterval();
    auto hopInterval = options.getHopSize();
    auto allowedLateness = options.getAllowedLateness();
    uassert(ErrorCodes::InvalidOptions,
            "Window interval size must be greater than 0.",
            windowInterval.getSize() > 0);
    uassert(ErrorCodes::InvalidOptions,
            "Window hopSize size must be greater than 0.",
            hopInterval.getSize() > 0);
    std::vector<mongo::BSONObj> ownedPipeline;
    for (auto& stageObj : options.getPipeline()) {
        std::string stageName(stageObj.firstElementFieldNameStringData());
        enforceStageConstraints(stageName, /*isMainPipeline*/ false);
        ownedPipeline.push_back(std::move(stageObj).getOwned());
    }

    std::pair<OperatorId, OperatorId> minMaxOperatorIds;
    minMaxOperatorIds.first = _nextOperatorId;

    Planner::Options plannerOptions;
    plannerOptions.planMainPipeline = false;
    plannerOptions.minOperatorId = _nextOperatorId;
    auto planner = std::make_unique<Planner>(_context, std::move(plannerOptions));
    auto operatorDag = planner->plan(ownedPipeline);

    _nextOperatorId += operatorDag->operators().size();
    minMaxOperatorIds.second = _nextOperatorId - 1;
    invariant(minMaxOperatorIds.second > minMaxOperatorIds.first);

    WindowOperator::Options windowOpOptions;
    windowOpOptions.size = windowInterval.getSize();
    windowOpOptions.sizeUnit = windowInterval.getUnit();
    windowOpOptions.slide = hopInterval.getSize();
    windowOpOptions.slideUnit = hopInterval.getUnit();
    windowOpOptions.pipeline = std::move(ownedPipeline);
    windowOpOptions.minMaxOperatorIds = std::move(minMaxOperatorIds);
    const auto& idleTimeout = options.getIdleTimeout();
    if (idleTimeout) {
        windowOpOptions.idleTimeoutSize = idleTimeout->getSize();
        windowOpOptions.idleTimeoutUnit = idleTimeout->getUnit();
    }
    windowOpOptions.allowedLatenessMs = parseAllowedLateness(allowedLateness);
    auto oper = std::make_unique<WindowOperator>(_context, std::move(windowOpOptions));
    oper->setOperatorId(operatorId);
    appendOperator(std::move(oper));
}

void Planner::planLookUp(mongo::DocumentSourceLookUp* documentSource) {
    auto& lookupPlanningInfo = _lookupPlanningInfos.back();
    auto& stageObj =
        lookupPlanningInfo.rewrittenLookupStages.at(lookupPlanningInfo.numLookupStagesPlanned++)
            .first;
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid lookup spec: " << stageObj,
            isLookUpStage(stageObj.firstElementFieldName()) &&
                stageObj.firstElement().isABSONObj());

    auto lookupObj = stageObj.firstElement().Obj();
    auto fromField = lookupObj[kFromFieldName];
    auto fromFieldObj = fromField.Obj();
    auto lookupFromAtlas =
        AtlasCollection::parse(IDLParserContext("AtlasCollection"), fromFieldObj);
    std::string connectionName(lookupFromAtlas.getConnectionName().toString());

    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Unknown connection name " << connectionName,
            _context->connections.contains(connectionName));

    const auto& connection = _context->connections.at(connectionName);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Only atlas connection type is currently supported for $lookup",
            connection.getType() == ConnectionTypeEnum::Atlas);
    auto atlasOptions = AtlasConnectionOptions::parse(IDLParserContext("AtlasConnectionOptions"),
                                                      connection.getOptions());

    MongoCxxClientOptions clientOptions(atlasOptions);
    clientOptions.svcCtx = _context->expCtx->opCtx->getServiceContext();
    auto foreignMongoDBClient = std::make_shared<MongoDBProcessInterface>(clientOptions);

    LookUpOperator::Options options{
        .documentSource = documentSource,
        .foreignMongoDBClient = std::move(foreignMongoDBClient),
        .foreignNs = getNamespaceString(lookupFromAtlas.getDb(), lookupFromAtlas.getColl())};
    auto oper = std::make_unique<LookUpOperator>(_context, std::move(options));
    oper->setOperatorId(_nextOperatorId++);
    appendOperator(std::move(oper));
}

void Planner::planGroup(mongo::DocumentSource* source) {
    auto specificSource = dynamic_cast<DocumentSourceGroup*>(source);
    dassert(specificSource);
    if (_windowPlanningInfo) {
        ++_windowPlanningInfo->numWindowAwareStagesPlanned;

        WindowAwareOperator::Options baseOptions;
        if (_windowPlanningInfo->numWindowAwareStagesPlanned == 1) {
            baseOptions.windowAssigner =
                std::make_unique<WindowAssigner>(_windowPlanningInfo->windowingOptions);
        }
        baseOptions.sendWindowCloseSignal = (_windowPlanningInfo->numWindowAwareStagesPlanned <
                                             _windowPlanningInfo->numWindowAwareStages);

        WindowAwareGroupOperator::Options options(std::move(baseOptions));
        options.documentSource = specificSource;
        auto oper = std::make_unique<WindowAwareGroupOperator>(_context, std::move(options));
        oper->setOperatorId(_nextOperatorId++);
        appendOperator(std::move(oper));
    } else {
        // TODO: Remove this after unnested window pipeline is fully rolled out in production.
        GroupOperator::Options options{.documentSource = specificSource};
        auto oper = std::make_unique<GroupOperator>(_context, std::move(options));
        oper->setOperatorId(_nextOperatorId++);
        appendOperator(std::move(oper));
    }
}

void Planner::planSort(mongo::DocumentSource* source) {
    auto specificSource = dynamic_cast<DocumentSourceSort*>(source);
    dassert(specificSource);
    if (_windowPlanningInfo) {
        ++_windowPlanningInfo->numWindowAwareStagesPlanned;

        WindowAwareOperator::Options baseOptions;
        if (_windowPlanningInfo->numWindowAwareStagesPlanned == 1) {
            baseOptions.windowAssigner =
                std::make_unique<WindowAssigner>(_windowPlanningInfo->windowingOptions);
        }
        baseOptions.sendWindowCloseSignal = (_windowPlanningInfo->numWindowAwareStagesPlanned <
                                             _windowPlanningInfo->numWindowAwareStages);

        WindowAwareSortOperator::Options options(std::move(baseOptions));
        options.documentSource = specificSource;
        auto oper = std::make_unique<WindowAwareSortOperator>(_context, std::move(options));
        oper->setOperatorId(_nextOperatorId++);
        appendOperator(std::move(oper));
    } else {
        // TODO: Remove this after unnested window pipeline is fully rolled out in production.
        SortOperator::Options options{.documentSource = specificSource};
        auto oper = std::make_unique<SortOperator>(_context, std::move(options));
        oper->setOperatorId(_nextOperatorId++);
        appendOperator(std::move(oper));
    }
}

void Planner::planLimit(mongo::DocumentSource* source) {
    int64_t limitValue{std::numeric_limits<int64_t>::max()};
    if (source) {
        auto specificSource = dynamic_cast<DocumentSourceLimit*>(source);
        dassert(specificSource);
        limitValue = specificSource->getLimit();
    }

    if (_windowPlanningInfo) {
        ++_windowPlanningInfo->numWindowAwareStagesPlanned;

        WindowAwareOperator::Options baseOptions;
        if (_windowPlanningInfo->numWindowAwareStagesPlanned == 1) {
            baseOptions.windowAssigner =
                std::make_unique<WindowAssigner>(_windowPlanningInfo->windowingOptions);
        }
        baseOptions.sendWindowCloseSignal = (_windowPlanningInfo->numWindowAwareStagesPlanned <
                                             _windowPlanningInfo->numWindowAwareStages);

        WindowAwareLimitOperator::Options options(std::move(baseOptions));
        options.limit = limitValue;
        auto oper = std::make_unique<WindowAwareLimitOperator>(_context, std::move(options));
        oper->setOperatorId(_nextOperatorId++);
        appendOperator(std::move(oper));
    } else {
        // TODO: Remove this after unnested window pipeline is fully rolled out in production.
        auto oper = std::make_unique<LimitOperator>(_context, limitValue);
        oper->setOperatorId(_nextOperatorId++);
        appendOperator(std::move(oper));
    }
}

void Planner::planPipeline(const mongo::Pipeline& pipeline) {
    if (_windowPlanningInfo) {
        static const stdx::unordered_set<std::string> windowAwareStages{
            {"$group", "$sort", "$limit"}};
        for (const auto& stage : pipeline.getSources()) {
            if (windowAwareStages.contains(stage->getSourceName())) {
                ++_windowPlanningInfo->numWindowAwareStages;
            }
        }
    }

    for (const auto& stage : pipeline.getSources()) {
        const auto& stageInfo = stageTraits[stage->getSourceName()];

        switch (stageInfo.type) {
            case StageType::kAddFields: {
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
                auto specificSource = dynamic_cast<DocumentSourceMatch*>(stage.get());
                dassert(specificSource);
                MatchOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<MatchOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kProject: {
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
                auto specificSource = dynamic_cast<DocumentSourceRedact*>(stage.get());
                dassert(specificSource);
                RedactOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<RedactOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kReplaceRoot: {
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
                auto specificSource = dynamic_cast<DocumentSourceUnwind*>(stage.get());
                dassert(specificSource);
                UnwindOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<UnwindOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kValidate: {
                auto specificSource = dynamic_cast<DocumentSourceValidateStub*>(stage.get());
                dassert(specificSource);
                auto options = makeValidateOperatorOptions(_context, specificSource->bsonOptions());
                auto oper = std::make_unique<ValidateOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kGroup: {
                planGroup(stage.get());
                break;
            }
            case StageType::kSort: {
                planSort(stage.get());
                break;
            }
            case StageType::kLimit: {
                planLimit(stage.get());
                break;
            }
            case StageType::kTumblingWindow: {
                if (_options.unnestWindowPipeline) {
                    planTumblingWindow(stage.get());
                } else {
                    planTumblingWindowLegacy(stage.get());
                }
                break;
            }
            case StageType::kHoppingWindow: {
                if (_options.unnestWindowPipeline) {
                    planHoppingWindow(stage.get());
                } else {
                    planHoppingWindowLegacy(stage.get());
                }
                break;
            }
            case StageType::kLookUp: {
                auto lookupSource = dynamic_cast<DocumentSourceLookUp*>(stage.get());
                dassert(lookupSource);
                planLookUp(lookupSource);
                break;
            }
            default:
                MONGO_UNREACHABLE;
        }
    }
}

std::unique_ptr<OperatorDag> Planner::plan(const std::vector<BSONObj>& bsonPipeline) {
    // Set the checkpoint interval. This might be modified if we're planning window stages.
    // If there are no windows in the pipeline or we are using "fast mode" window checkpointing,
    // checkpoints are small. So we write a checkpoint every 5 minutes.
    // If we are using "slow mode" window checkpointing, checkpoints might be large, so we
    // checkpoint every 1 hour.
    _context->checkpointInterval = kFastCheckpointInterval;

    planInner(bsonPipeline);

    OperatorDag::Options options;
    options.bsonPipeline = bsonPipeline;
    options.pipeline = std::move(_pipeline);
    options.timestampExtractor = std::move(_timestampExtractor);
    options.eventDeserializer = std::move(_eventDeserializer);
    return make_unique<OperatorDag>(std::move(options), std::move(_operators));
}

void Planner::planInner(const std::vector<BSONObj>& bsonPipeline) {
    if (planningMainPipeline()) {
        uassert(ErrorCodes::InvalidOptions,
                "Pipeline must have at least one stage",
                !bsonPipeline.empty());
        std::string firstStageName(bsonPipeline.begin()->firstElementFieldNameStringData());
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "First stage must be " << kSourceStageName
                              << ", found: " << firstStageName,
                isSourceStage(firstStageName));
        std::string lastStageName(bsonPipeline.rbegin()->firstElementFieldNameStringData());
        uassert(ErrorCodes::InvalidOptions,
                "The last stage in the pipeline must be $merge or $emit.",
                isSinkStage(lastStageName) || _context->isEphemeral);
    }

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
        planSource(sourceSpec, useWatermarks, sendIdleMessages);
        ++current;
    }

    // Get the middle stages until we hit a sink stage
    std::vector<BSONObj> middleStages;
    while (current != bsonPipeline.end() &&
           !isSinkStage(current->firstElementFieldNameStringData())) {
        std::string stageName(current->firstElementFieldNameStringData());
        enforceStageConstraints(stageName, planningMainPipeline());

        middleStages.emplace_back(*current);
        ++current;
    }

    // Then everything between the source and the $merge/$emit
    if (!middleStages.empty()) {
        auto pipelineRewriter = std::make_unique<PipelineRewriter>(std::move(middleStages));
        middleStages = pipelineRewriter->rewrite();

        LookUpPlanningInfo lookupPlanningInfo;
        lookupPlanningInfo.rewrittenLookupStages = pipelineRewriter->getRewrittenLookupStages();
        _lookupPlanningInfos.push_back(std::move(lookupPlanningInfo));

        // Set resolved namespaces in the ExpressionContext. Currently this is only needed to
        // satisfy the getResolvedNamespace() call in DocumentSourceLookup constructor.
        LiteParsedPipeline liteParsedPipeline(_context->expCtx->ns, middleStages);
        auto pipelineInvolvedNamespaces = liteParsedPipeline.getInvolvedNamespaces();
        StringMap<ExpressionContext::ResolvedNamespace> resolvedNamespaces;
        for (auto& involvedNs : pipelineInvolvedNamespaces) {
            resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
        }
        _context->expCtx->setResolvedNamespaces(std::move(resolvedNamespaces));
        auto pipeline = Pipeline::parse(middleStages, _context->expCtx);
        pipeline->optimizePipeline();

        planPipeline(*pipeline);
        _pipeline.splice(_pipeline.end(), std::move(pipeline->getSources()));

        _lookupPlanningInfos.pop_back();
    }

    // After the loop above, current is either pointing to a sink
    // or the end.
    BSONObj sinkSpec;
    if (current == bsonPipeline.end()) {
        // We're at the end of the bsonPipeline and we have not found a sink stage.
        if (!planningMainPipeline()) {
            if (!_options.unnestWindowPipeline) {
                // In the window inner pipeline case, we append a CollectOperator to collect the
                // documents emitted at the end of the pipeline.
                sinkSpec = BSON(kEmitStageName << BSON(kConnectionNameField
                                                       << kCollectSinkOperatorConnectionName));
            }
        } else {
            uassert(ErrorCodes::InvalidOptions,
                    "The last stage in the pipeline must be $merge or $emit.",
                    _context->isEphemeral);
            // In the ephemeral case, we append a NoOpSink to handle the sample requests.
            sinkSpec = BSON(kEmitStageName
                            << BSON(kConnectionNameField << kNoOpSinkOperatorConnectionName));
        }
    } else {
        sinkSpec = *current;
        invariant(isSinkStage(sinkSpec.firstElementFieldNameStringData()));
        uassert(ErrorCodes::InvalidOptions,
                "No stages are allowed after a $merge or $emit stage.",
                std::next(current) == bsonPipeline.end());
    }

    if (!sinkSpec.isEmpty()) {
        auto sinkStageName = sinkSpec.firstElementFieldNameStringData();
        if (isMergeStage(sinkStageName)) {
            planMergeSink(sinkSpec);
        } else {
            dassert(isEmitStage(sinkStageName));
            planEmitSink(sinkSpec);
        }
    }
}

};  // namespace streams
