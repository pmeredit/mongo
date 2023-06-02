/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include <memory>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/change_stream.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/change_stream_options_gen.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_merge_modes_gen.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/namespace_string_util.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace std;
using namespace mongo;

namespace {

constexpr auto kConnectionNameField = "connectionName"_sd;
constexpr auto kIntoField = "into"_sd;
constexpr auto kKafkaConnectionType = "kafka"_sd;
constexpr auto kAtlasConnectionType = "atlas"_sd;
constexpr auto kNoOpSinkOperatorConnectionName = "__noopSink"_sd;

bool isWindowStage(StringData name) {
    return name == DocumentSourceTumblingWindowStub::kStageName ||
        name == DocumentSourceHoppingWindowStub::kStageName;
}

bool isSourceStage(StringData name) {
    return name == Parser::kSourceStageName;
}

bool isEmitStage(StringData name) {
    return name == Parser::kEmitStageName;
}

bool isMergeStage(StringData name) {
    return name == Parser::kMergeStageName;
}

bool isSinkStage(StringData name) {
    return isEmitStage(name) || isMergeStage(name);
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

    auto mergeIntoAtlas =
        MergeIntoAtlas::parse(IDLParserContext("MergeIntoAtlas"), mergeOpSpec.getInto());
    DocumentSourceMergeSpec docSourceMergeSpec;
    docSourceMergeSpec.setTargetNss(NamespaceStringUtil::parseNamespaceFromRequest(
        mergeIntoAtlas.getDb(), mergeIntoAtlas.getColl()));
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

struct SinkParseResult {
    boost::intrusive_ptr<DocumentSource> documentSource;
    std::unique_ptr<SinkOperator> sinkOperator;
};

SinkParseResult fromEmitSpec(const BSONObj& spec,
                             const boost::intrusive_ptr<ExpressionContext>& expCtx,
                             OperatorFactory* operatorFactory,
                             const stdx::unordered_map<std::string, Connection>& connectionObjs) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == Parser::kEmitStageName &&
                spec.firstElement().isABSONObj());

    auto sinkSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sinkSpec.getField(kConnectionNameField);
    uassert(ErrorCodes::InvalidOptions,
            "$emit must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    SinkParseResult result;
    if (connectionName == kTestLogConnectionName) {
        result.sinkOperator = std::make_unique<LogSinkOperator>();
    } else if (connectionName == kTestMemoryConnectionName) {
        result.sinkOperator = std::make_unique<InMemorySinkOperator>(1);
    } else if (connectionName == kNoOpSinkOperatorConnectionName) {
        result.sinkOperator = std::make_unique<NoOpSinkOperator>();
    } else {
        uasserted(ErrorCodes::InvalidOptions,
                  str::stream() << "Invalid " << Parser::kEmitStageName << sinkSpec);
    }
    return result;
}

SinkParseResult fromMergeSpec(const BSONObj& spec,
                              const boost::intrusive_ptr<ExpressionContext>& expCtx,
                              OperatorFactory* operatorFactory,
                              const stdx::unordered_map<std::string, Connection>& connectionObjs) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == Parser::kMergeStageName &&
                spec.firstElement().isABSONObj());

    auto mergeObj = spec.firstElement().Obj();
    auto mergeOpSpec = MergeOperatorSpec::parse(IDLParserContext("MergeOperatorSpec"), mergeObj);
    auto mergeIntoAtlas =
        MergeIntoAtlas::parse(IDLParserContext("MergeIntoAtlas"), mergeOpSpec.getInto());
    std::string connectionName(mergeIntoAtlas.getConnectionName().toString());

    if (connectionObjs.contains(connectionName)) {
        const auto& connection = connectionObjs.at(connectionName);
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Only atlas merge connection type is currently supported",
                connection.getType() == ConnectionTypeEnum::Atlas);
        auto options = AtlasConnectionOptions::parse(IDLParserContext("AtlasConnectionOptions"),
                                                     connection.getOptions());
        if (expCtx->mongoProcessInterface) {
            dassert(dynamic_cast<StubMongoProcessInterface*>(expCtx->mongoProcessInterface.get()));
        }
        expCtx->mongoProcessInterface =
            std::make_shared<MongoDBProcessInterface>(MongoDBProcessInterface::Options{
                .svcCtx = expCtx->opCtx->getServiceContext(),
                .mongodbUri = options.getUri().toString(),
                .database = DatabaseNameUtil::serialize(mergeIntoAtlas.getDb()),
                .collection = mergeIntoAtlas.getColl().toString()});
        auto documentSourceMerge = DocumentSourceMerge::parse(
            expCtx, BSON("$merge" << buildDocumentSourceMergeSpec(mergeOpSpec).toBSON()));
        dassert(documentSourceMerge.size() == 1);

        SinkParseResult result;
        result.documentSource = std::move(documentSourceMerge.front());
        documentSourceMerge.pop_front();
        result.sinkOperator = operatorFactory->toSinkOperator(result.documentSource.get());
        return result;
    } else {
        uasserted(ErrorCodes::InvalidOptions,
                  str::stream() << "Invalid " << Parser::kMergeStageName << mergeObj);
    }
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

// TODO SERVER-77563: Consider supporting additional change stream options.
mongocxx::options::change_stream constructChangeStreamOptions(
    const ChangeStreamSourceOptions& options) {
    mongocxx::options::change_stream changeStreamOptions;

    if (auto resumeAfter = options.getResumeAfter(); resumeAfter) {
        changeStreamOptions.resume_after(toBsoncxxDocument(resumeAfter->toBSON()));
    }

    if (auto startAfter = options.getStartAfter(); startAfter) {
        changeStreamOptions.start_after(toBsoncxxDocument(startAfter->toBSON()));
    }

    if (auto startAtOperationTime = options.getStartAtOperationTime(); startAtOperationTime) {
        // TODO SERVER-77563: Determine if there is a way to do this conversion directly.
        bsoncxx::types::b_timestamp bsonTimestamp;
        bsonTimestamp.timestamp = startAtOperationTime->getSecs();
        bsonTimestamp.increment = startAtOperationTime->getInc();
        changeStreamOptions.start_at_operation_time(std::move(bsonTimestamp));
    }

    return changeStreamOptions;
}

struct SourceParseResult {
    std::unique_ptr<SourceOperator> sourceOperator;
    std::unique_ptr<DocumentTimestampExtractor> timestampExtractor;
    std::unique_ptr<EventDeserializer> eventDeserializer;
};

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

// Utility which configures options common to all $source stages, including:
// - deadLetterQueue
// - timestampOutputFieldName
// - timestampExtractor
void configureSourceOperatorOptions(DeadLetterQueue* dlq,
                                    boost::optional<StringData> tsFieldOverride,
                                    DocumentTimestampExtractor* timestampExtractor,
                                    SourceOperator::Options& options) {
    // deadLetterQueue
    options.deadLetterQueue = dlq;

    // timestampOutputFieldName
    if (tsFieldOverride) {
        options.timestampOutputFieldName = tsFieldOverride->toString();
    } else {
        options.timestampOutputFieldName = Parser::kDefaultTimestampOutputFieldName.toString();
    }
    uassert(ErrorCodes::InternalError,
            str::stream() << ChangeStreamSourceOptions::kTsFieldOverrideFieldName
                          << " cannot be empty",
            !options.timestampOutputFieldName.empty());

    // timestampExtractor
    options.timestampExtractor = timestampExtractor;
}

SourceParseResult makeSampleDataSource(const BSONObj& sourceSpec,
                                       const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                       OperatorFactory* operatorFactory,
                                       DeadLetterQueue* dlq,
                                       bool useWatermarks) {
    auto options = SampleDataSourceOptions::parse(
        IDLParserContext(Parser::kSourceStageName.toString()), sourceSpec);

    SourceParseResult result;
    result.timestampExtractor = createTimestampExtractor(expCtx, options.getTimeField());

    SampleDataSourceOperator::Options internalOptions;
    configureSourceOperatorOptions(
        dlq, options.getTsFieldOverride(), result.timestampExtractor.get(), internalOptions);

    if (useWatermarks) {
        int64_t allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
        internalOptions.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
            0 /* inputIdx */, nullptr /* combiner */, allowedLatenessMs);
    }

    result.sourceOperator = operatorFactory->toSourceOperator(std::move(internalOptions));
    return result;
}

SourceParseResult makeKafkaSource(const BSONObj& sourceSpec,
                                  const KafkaConnectionOptions& baseOptions,
                                  const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                  OperatorFactory* operatorFactory,
                                  DeadLetterQueue* dlq,
                                  bool useWatermarks) {
    auto options = KafkaSourceOptions::parse(IDLParserContext(Parser::kSourceStageName.toString()),
                                             sourceSpec);

    SourceParseResult result;
    result.timestampExtractor = createTimestampExtractor(expCtx, options.getTimeField());

    KafkaConsumerOperator::Options internalOptions;
    configureSourceOperatorOptions(
        dlq, options.getTsFieldOverride(), result.timestampExtractor.get(), internalOptions);

    internalOptions.bootstrapServers = std::string{baseOptions.getBootstrapServers()};
    internalOptions.topicName = std::string{options.getTopic()};
    internalOptions.deadLetterQueue = dlq;
    if (useWatermarks) {
        internalOptions.watermarkCombiner =
            std::make_unique<WatermarkCombiner>(options.getPartitionCount());
    }

    uassert(ErrorCodes::InvalidOptions, "Invalid partition count", options.getPartitionCount() > 0);

    int64_t allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
    for (int partition = 0; partition < options.getPartitionCount(); ++partition) {
        KafkaConsumerOperator::PartitionOptions partitionOptions;
        partitionOptions.partition = partition;
        if (useWatermarks) {
            partitionOptions.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
                partition, internalOptions.watermarkCombiner.get(), allowedLatenessMs);
        }
        internalOptions.partitionOptions.push_back(std::move(partitionOptions));
    }

    result.eventDeserializer = std::make_unique<JsonEventDeserializer>();
    internalOptions.deserializer = result.eventDeserializer.get();

    if (baseOptions.getIsTestKafka() && *baseOptions.getIsTestKafka()) {
        internalOptions.isTest = true;
    }

    result.sourceOperator = operatorFactory->toSourceOperator(std::move(internalOptions));
    return result;
}

SourceParseResult makeChangeStreamSource(const BSONObj& sourceSpec,
                                         const AtlasConnectionOptions& atlasOptions,
                                         const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                         OperatorFactory* operatorFactory,
                                         DeadLetterQueue* dlq,
                                         bool useWatermarks) {
    auto options = ChangeStreamSourceOptions::parse(
        IDLParserContext(Parser::kSourceStageName.toString()), sourceSpec);

    SourceParseResult result;
    result.timestampExtractor = createTimestampExtractor(expCtx, options.getTimeField());

    ChangeStreamSourceOperator::Options internalOptions;
    configureSourceOperatorOptions(
        dlq, options.getTsFieldOverride(), result.timestampExtractor.get(), internalOptions);

    if (useWatermarks) {
        int64_t allowedLatenessMs = parseAllowedLateness(options.getAllowedLateness());
        internalOptions.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
            0 /* inputIdx */, nullptr /* combiner */, allowedLatenessMs);
    }

    internalOptions.svcCtx = expCtx->opCtx->getServiceContext();
    internalOptions.uri = atlasOptions.getUri().toString();

    auto db = options.getDb();
    uassert(ErrorCodes::InvalidOptions,
            "Cannot specify a non-empty database name to $source when configuring a change stream",
            !db.empty());

    if (auto coll = options.getColl(); coll) {
        // TODO SERVER-77558: Update this code to account for tenantId.
        internalOptions.nss = NamespaceString(db.toString(), *coll, /* tenantid */ boost::none);
    } else {
        internalOptions.nss = NamespaceString(db.toString());
    }

    internalOptions.changeStreamOptions = constructChangeStreamOptions(options);
    result.sourceOperator = operatorFactory->toSourceOperator(std::move(internalOptions));
    return result;
}

SourceParseResult fromSourceSpec(const BSONObj& spec,
                                 const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                 OperatorFactory* operatorFactory,
                                 const stdx::unordered_map<std::string, Connection>& connectionObjs,
                                 DeadLetterQueue* dlq,
                                 bool useWatermarks) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid $source " << Parser::kSourceStageName << spec,
            spec.firstElementFieldName() == Parser::kSourceStageName &&
                spec.firstElement().isABSONObj());

    auto sourceSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sourceSpec.getField(kConnectionNameField);
    uassert(ErrorCodes::InvalidOptions,
            "$source must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    if (connectionName == kTestMemoryConnectionName) {
        return {std::make_unique<InMemorySourceOperator>(1), nullptr, nullptr};
    }

    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid connectionName in " << Parser::kSourceStageName << " "
                          << sourceSpec,
            connectionObjs.contains(connectionName));

    const auto& connection = connectionObjs.at(connectionName);
    switch (connection.getType()) {
        case ConnectionTypeEnum::Kafka: {
            auto options = KafkaConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                         connection.getOptions());
            return makeKafkaSource(
                sourceSpec, options, expCtx, operatorFactory, dlq, useWatermarks);
        };
        case ConnectionTypeEnum::SampleSolar: {
            return makeSampleDataSource(sourceSpec, expCtx, operatorFactory, dlq, useWatermarks);
        };
        case ConnectionTypeEnum::Atlas: {
            // We currently assume that an atlas connection implies a change stream $source.
            auto connOptions = AtlasConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                             connection.getOptions());
            return makeChangeStreamSource(
                sourceSpec, connOptions, expCtx, operatorFactory, dlq, useWatermarks);
        };
        default:
            uasserted(ErrorCodes::InvalidOptions,
                      "Only kafka, sample_solar, and atlas source connection type is supported");
    }
}
}  // namespace

Parser::Parser(Context* context, stdx::unordered_map<std::string, Connection> connections)
    : _context(context), _operatorFactory(context), _connectionObjs(std::move(connections)) {}

unique_ptr<OperatorDag> Parser::fromBson(const std::vector<BSONObj>& bsonPipeline) {
    uassert(ErrorCodes::InvalidOptions,
            "Pipeline must have at least one stage",
            bsonPipeline.size() > 0);
    OperatorDag::Options options;
    options.bsonPipeline = bsonPipeline;

    // We only use watermarks when the pipeline contains a window stage.
    bool useWatermarks = false;

    // Get the $source BSON
    auto current = bsonPipeline.begin();
    string firstStageName(current->firstElementFieldNameStringData());
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "First stage must be " << kSourceStageName
                          << ", found: " << firstStageName,
            isSourceStage(firstStageName));
    auto sourceSpec = *current;

    // Get the middle stages until we hit a sink stage
    std::vector<BSONObj> middleStages;
    current = next(current);
    while (current != bsonPipeline.end() &&
           !isSinkStage(current->firstElementFieldNameStringData())) {
        string stageName(current->firstElementFieldNameStringData());
        _operatorFactory.validateByName(stageName);
        if (isWindowStage(stageName)) {
            useWatermarks = true;
        }

        middleStages.emplace_back(*current);
        current = next(current);
    }

    // Create the source operator
    auto sourceParseResult = fromSourceSpec(sourceSpec,
                                            _context->expCtx,
                                            &_operatorFactory,
                                            _connectionObjs,
                                            _context->dlq.get(),
                                            useWatermarks);
    auto sourceOperator = std::move(sourceParseResult.sourceOperator);
    options.eventDeserializer = std::move(sourceParseResult.eventDeserializer);
    options.timestampExtractor = std::move(sourceParseResult.timestampExtractor);

    // Build the DAG
    // Start with the $source
    OperatorDag::OperatorContainer operators;
    operators.push_back(std::move(sourceOperator));

    // Then everything between the source and the $merge/$emit
    if (!middleStages.empty()) {
        auto pipeline = Pipeline::parse(middleStages, _context->expCtx);
        pipeline->optimizePipeline();
        for (const auto& stage : pipeline->getSources()) {
            auto op = _operatorFactory.toOperator(stage.get());
            // Make this operator the output of the prior operator.
            operators.back()->addOutput(op.get(), 0);
            operators.push_back(std::move(op));
        }
        options.pipeline = std::move(pipeline->getSources());
    }

    // After the loop above, current is either pointing to a sink
    // or the end.
    BSONObj sinkBson;
    if (current == bsonPipeline.end()) {
        // We're at the end of the bsonPipeline and we have not found a sink stage.
        // If the streamProcessor is ephemeral (created during a user .process() flow),
        // we allow no sink stage.
        uassert(ErrorCodes::InvalidOptions,
                "The last stage in the pipeline must be $merge or $emit.",
                _context->isEphemeral);

        // In the ephemeral case, we append a NoOpSink to handle the sample requests.
        // TODO(SERVER-76803): Use a more appropriate no-op sink, not LogSinkOperator.
        sinkBson = BSON(Parser::kEmitStageName
                        << BSON(kConnectionNameField << kNoOpSinkOperatorConnectionName));
    } else {
        sinkBson = *current;
        dassert(isSinkStage(sinkBson.firstElementFieldNameStringData()));
        uassert(ErrorCodes::InvalidOptions,
                "No stages are allowed after a $merge or $emit stage.",
                next(current) == bsonPipeline.end());
    }

    SinkParseResult sinkParseResult;
    auto sinkStageName = sinkBson.firstElementFieldNameStringData();
    if (isMergeStage(sinkStageName)) {
        sinkParseResult =
            fromMergeSpec(sinkBson, _context->expCtx, &_operatorFactory, _connectionObjs);
    } else {
        dassert(isEmitStage(sinkStageName));
        sinkParseResult =
            fromEmitSpec(sinkBson, _context->expCtx, &_operatorFactory, _connectionObjs);
    }

    if (sinkParseResult.documentSource) {
        options.pipeline.push_back(std::move(sinkParseResult.documentSource));
    }
    operators.back()->addOutput(sinkParseResult.sinkOperator.get(), 0);
    operators.push_back(std::move(sinkParseResult.sinkOperator));

    return make_unique<OperatorDag>(std::move(options), std::move(operators));
}

};  // namespace streams
