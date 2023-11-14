/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/planner.h"

#include <memory>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/change_stream.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/change_stream_options_gen.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
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
    bool allowedInWindowInnerPipeline{false};
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
                stageInfo.allowedInWindowInnerPipeline);
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
DocumentSourceMergeSpec buildDocumentSourceMergeSpec(MergeOperatorSpec mergeOpSpec) {
    // TODO: Support kFail whenMatched/whenNotMatched mode.
    static const stdx::unordered_set<MergeWhenMatchedModeEnum> supportedWhenMatchedModes{
        {MergeWhenMatchedModeEnum::kKeepExisting,
         MergeWhenMatchedModeEnum::kMerge,
         MergeWhenMatchedModeEnum::kReplace}};
    static const stdx::unordered_set<MergeWhenNotMatchedModeEnum> supportedWhenNotMatchedModes{
        {MergeWhenNotMatchedModeEnum::kDiscard, MergeWhenNotMatchedModeEnum::kInsert}};

    DocumentSourceMergeSpec docSourceMergeSpec;
    // Set the dummy target namespace of "$nodb.$nocoll$" since it's not used.
    docSourceMergeSpec.setTargetNss(NamespaceStringUtil::deserialize(
        /*tenantId=*/boost::none, "$nodb$.$nocoll$", SerializationContext()));
    docSourceMergeSpec.setOn(mergeOpSpec.getOn());
    if (mergeOpSpec.getWhenMatched()) {
        uassert(ErrorCodes::InvalidOptions,
                "Unsupported whenMatched mode: ",
                supportedWhenMatchedModes.contains(*mergeOpSpec.getWhenMatched()));
        docSourceMergeSpec.setWhenMatched(MergeWhenMatchedPolicy{*mergeOpSpec.getWhenMatched()});
    }
    if (mergeOpSpec.getWhenNotMatched()) {
        uassert(ErrorCodes::InvalidOptions,
                "Unsupported whenNotMatched mode: ",
                supportedWhenNotMatchedModes.contains(*mergeOpSpec.getWhenNotMatched()));
        docSourceMergeSpec.setWhenNotMatched(mergeOpSpec.getWhenNotMatched());
    }
    return docSourceMergeSpec;
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
    return authConfig;
}

int64_t parseAllowedLateness(const boost::optional<StreamTimeDuration>& param) {
    // From the spec, 3 seconds is the default allowed lateness.
    int64_t allowedLatenessMs = 3000;
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

void Planner::validateByName(const std::string& name) {
    enforceStageConstraints(name, _options.planMainPipeline);
}

void Planner::appendOperator(std::unique_ptr<Operator> oper) {
    if (!_operators.empty()) {
        _operators.back()->addOutput(oper.get(), 0);
    }
    _operators.push_back(std::move(oper));
}

void Planner::planInMemorySource(const BSONObj& sourceSpec, bool useWatermarks) {
    auto options =
        GeneratedDataSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);
    dassert(options.getConnectionName() == kTestMemoryConnectionName);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());

    InMemorySourceOperator::Options internalOptions(
        getSourceOperatorOptions(options.getTsFieldOverride(), _timestampExtractor.get()));
    internalOptions.useWatermarks = useWatermarks;

    if (useWatermarks) {
        internalOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    }

    auto oper = std::make_unique<InMemorySourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planSampleSolarSource(const BSONObj& sourceSpec, bool useWatermarks) {
    auto options =
        GeneratedDataSourceOptions::parse(IDLParserContext(kSourceStageName), sourceSpec);

    _timestampExtractor = createTimestampExtractor(_context->expCtx, options.getTimeField());

    SampleDataSourceOperator::Options internalOptions(
        getSourceOperatorOptions(options.getTsFieldOverride(), _timestampExtractor.get()));
    internalOptions.useWatermarks = useWatermarks;

    if (useWatermarks) {
        internalOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    }

    auto oper = std::make_unique<SampleDataSourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planKafkaSource(const BSONObj& sourceSpec,
                              const KafkaConnectionOptions& baseOptions,
                              bool useWatermarks) {
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
    if (config && config->getStartAt() == KafkaSourceStartAtEnum::Earliest) {
        internalOptions.startOffset = RdKafka::Topic::OFFSET_BEGINNING;
    }
    internalOptions.useWatermarks = useWatermarks;
    if (internalOptions.useWatermarks) {
        internalOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
        if (auto idlenessTimeout = options.getIdlenessTimeout()) {
            internalOptions.idlenessTimeoutMs = stdx::chrono::milliseconds(
                toMillis(idlenessTimeout->getUnit(), idlenessTimeout->getSize()));
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
                                     bool useWatermarks) {
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
        internalOptions.allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    }

    uassert(ErrorCodes::InvalidOptions,
            "startAfter and startAtOperationTime cannot both be set",
            !(options.getStartAfter() && options.getStartAtOperationTime()));

    if (auto startAfter = options.getStartAfter()) {
        internalOptions.userSpecifiedStartingPoint = startAfter->toBSON();
    }

    if (auto startAtOperationTime = options.getStartAtOperationTime()) {
        internalOptions.userSpecifiedStartingPoint = *startAtOperationTime;
    }

    if (auto fullDocument = options.getFullDocument(); fullDocument) {
        internalOptions.fullDocumentMode = *options.getFullDocument();
    }

    if (auto fullDocumentOnly = options.getFullDocumentOnly()) {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "fullDocumentOnly is set to true, fullDocument mode can either be "
                                 "updateLookup or required",
                internalOptions.fullDocumentMode == mongo::FullDocumentModeEnum::kUpdateLookup ||
                    internalOptions.fullDocumentMode == mongo::FullDocumentModeEnum::kRequired);
        internalOptions.fullDocumentOnly = *fullDocumentOnly;
    }

    auto oper = std::make_unique<ChangeStreamSourceOperator>(_context, std::move(internalOptions));
    oper->setOperatorId(_nextOperatorId++);
    invariant(_operators.empty());
    appendOperator(std::move(oper));
}

void Planner::planSource(const BSONObj& spec, bool useWatermarks) {
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
            planKafkaSource(sourceSpec, options, useWatermarks);
            break;
        };
        case ConnectionTypeEnum::SampleSolar: {
            planSampleSolarSource(sourceSpec, useWatermarks);
            break;
        };
        case ConnectionTypeEnum::InMemory: {
            planInMemorySource(sourceSpec, useWatermarks);
            break;
        };
        case ConnectionTypeEnum::Atlas: {
            // We currently assume that an atlas connection implies a change stream $source.
            auto connOptions = AtlasConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                             connection.getOptions());
            planChangeStreamSource(sourceSpec, connOptions, useWatermarks);
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

    MongoCxxClientOptions clientOptions(atlasOptions);
    clientOptions.svcCtx = _context->expCtx->opCtx->getServiceContext();
    _context->expCtx->mongoProcessInterface =
        std::make_shared<MongoDBProcessInterface>(clientOptions);

    auto documentSourceMerge = DocumentSourceMerge::parse(
        _context->expCtx, BSON("$merge" << buildDocumentSourceMergeSpec(mergeOpSpec).toBSON()));
    dassert(documentSourceMerge.size() == 1);

    auto documentSource = std::move(documentSourceMerge.front());
    documentSourceMerge.pop_front();

    auto specificSource = dynamic_cast<DocumentSourceMerge*>(documentSource.get());
    dassert(specificSource);
    MergeOperator::Options options{.documentSource = specificSource,
                                   .db = mergeIntoAtlas.getDb(),
                                   .coll = mergeIntoAtlas.getColl()};
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
    // Reserve the next OperatorId for this WindowOperator.
    auto operatorId = _nextOperatorId++;

    auto options = TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    auto offset = options.getOffset();
    auto size = interval.getSize();
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
    auto oper = std::make_unique<WindowOperator>(_context, std::move(windowOpOptions));
    oper->setOperatorId(operatorId);
    appendOperator(std::move(oper));
}

void Planner::planHoppingWindow(DocumentSource* source) {
    auto windowSource = dynamic_cast<DocumentSourceHoppingWindowStub*>(source);
    dassert(windowSource);
    BSONObj bsonOptions = windowSource->bsonOptions();
    // Reserve the next OperatorId for this WindowOperator.
    auto operatorId = _nextOperatorId++;

    auto options = HoppingWindowOptions::parse(IDLParserContext("hoppingWindow"), bsonOptions);
    auto windowInterval = options.getInterval();
    auto hopInterval = options.getHopSize();
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
    auto oper = std::make_unique<WindowOperator>(_context, std::move(windowOpOptions));
    oper->setOperatorId(operatorId);
    appendOperator(std::move(oper));
}

void Planner::planLookUp(const BSONObj& stageObj, mongo::DocumentSourceLookUp* documentSource) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid lookup spec: " << stageObj,
            isLookUpStage(stageObj.firstElementFieldName()) &&
                stageObj.firstElement().isABSONObj());
    uassert(ErrorCodes::InvalidOptions,
            "$lookup must specify values for 'localField' and 'foreignField' fields",
            documentSource->getLocalField() && documentSource->getForeignField());
    uassert(ErrorCodes::InvalidOptions,
            "$lookup must not specify values for 'let' and 'pipeline' fields",
            documentSource->getLetVariables().empty() && !documentSource->hasPipeline());

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

void Planner::planPipeline(const mongo::Pipeline& pipeline) {
    std::vector<std::pair<mongo::BSONObj, mongo::BSONObj>> rewrittenLookupStages;
    if (_pipelineRewriter) {
        rewrittenLookupStages = _pipelineRewriter->getRewrittenLookupStages();
    }

    size_t numLookupStagesSeen{0};
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
                auto specificSource = dynamic_cast<DocumentSourceGroup*>(stage.get());
                dassert(specificSource);
                GroupOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<GroupOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kSort: {
                auto specificSource = dynamic_cast<DocumentSourceSort*>(stage.get());
                dassert(specificSource);
                SortOperator::Options options{.documentSource = specificSource};
                auto oper = std::make_unique<SortOperator>(_context, std::move(options));
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kLimit: {
                auto specificSource = dynamic_cast<DocumentSourceLimit*>(stage.get());
                dassert(specificSource);
                auto oper = std::make_unique<LimitOperator>(_context, specificSource->getLimit());
                oper->setOperatorId(_nextOperatorId++);
                appendOperator(std::move(oper));
                break;
            }
            case StageType::kTumblingWindow: {
                planTumblingWindow(stage.get());
                break;
            }
            case StageType::kHoppingWindow: {
                planHoppingWindow(stage.get());
                break;
            }
            case StageType::kLookUp: {
                auto lookupSource = dynamic_cast<DocumentSourceLookUp*>(stage.get());
                dassert(lookupSource);
                auto& inputLookupSpec = rewrittenLookupStages.at(numLookupStagesSeen++).first;
                planLookUp(inputLookupSpec, lookupSource);
                break;
            }
            default:
                MONGO_UNREACHABLE;
        }
    }
    invariant(numLookupStagesSeen == rewrittenLookupStages.size());
}

std::unique_ptr<OperatorDag> Planner::plan(const std::vector<BSONObj>& bsonPipeline) {
    if (_options.planMainPipeline) {
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

    // We only use watermarks when the pipeline contains a window stage.
    bool useWatermarks{false};
    if (auto it =
            std::find_if(bsonPipeline.begin(),
                         bsonPipeline.end(),
                         [](const BSONObj& stageSpec) {
                             return isWindowStage(stageSpec.firstElementFieldNameStringData());
                         });
        it != bsonPipeline.end()) {
        useWatermarks = true;
    }

    // Get the $source BSON.
    auto current = bsonPipeline.begin();
    if (current != bsonPipeline.end() &&
        isSourceStage(current->firstElementFieldNameStringData())) {
        // Build the DAG, start with the $source
        auto sourceSpec = *current;
        // Create the source operator
        planSource(sourceSpec, useWatermarks);
        ++current;
    }

    // Get the middle stages until we hit a sink stage
    std::vector<BSONObj> middleStages;
    while (current != bsonPipeline.end() &&
           !isSinkStage(current->firstElementFieldNameStringData())) {
        std::string stageName(current->firstElementFieldNameStringData());
        validateByName(stageName);

        middleStages.emplace_back(*current);
        ++current;
    }

    // Then everything between the source and the $merge/$emit
    if (!middleStages.empty()) {
        invariant(!_pipelineRewriter);
        _pipelineRewriter = std::make_unique<PipelineRewriter>(std::move(middleStages));
        middleStages = _pipelineRewriter->rewrite();

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
        invariant(_pipeline.empty());
        _pipeline = std::move(pipeline->getSources());
    }

    // After the loop above, current is either pointing to a sink
    // or the end.
    BSONObj sinkSpec;
    if (current == bsonPipeline.end()) {
        // We're at the end of the bsonPipeline and we have not found a sink stage.
        if (!_options.planMainPipeline) {
            // In the window inner pipeline case, we append a CollectOperator to collect the
            // documents emitted at the end of the pipeline.
            sinkSpec = BSON(kEmitStageName
                            << BSON(kConnectionNameField << kCollectSinkOperatorConnectionName));
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

    OperatorDag::Options options;
    options.bsonPipeline = bsonPipeline;
    options.pipeline = std::move(_pipeline);
    options.timestampExtractor = std::move(_timestampExtractor);
    options.eventDeserializer = std::move(_eventDeserializer);
    return make_unique<OperatorDag>(std::move(options), std::move(_operators));
}

};  // namespace streams
