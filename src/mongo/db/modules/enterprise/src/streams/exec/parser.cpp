/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <memory>

#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/namespace_string_util.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/in_memory_source_sink_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/merge_stage_gen.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/source_stage_gen.h"
#include "streams/exec/test_constants.h"

#include "streams/exec/connection_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace std;
using namespace mongo;

namespace {

constexpr auto kConnectionNameField = "connectionName"_sd;
constexpr auto kIntoField = "into"_sd;
constexpr auto kKafkaConnectionType = "kafka"_sd;
constexpr auto kAtlasConnectionType = "atlas"_sd;

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
    auto mergeIntoAtlas =
        MergeIntoAtlas::parse(IDLParserContext("MergeIntoAtlas"), mergeOpSpec.getInto());
    DocumentSourceMergeSpec docSourceMergeSpec;
    docSourceMergeSpec.setTargetNss(NamespaceStringUtil::parseNamespaceFromRequest(
        mergeIntoAtlas.getDb(), mergeIntoAtlas.getColl()));
    docSourceMergeSpec.setOn(mergeOpSpec.getOn());
    docSourceMergeSpec.setWhenMatched(mergeOpSpec.getWhenMatched());
    docSourceMergeSpec.setWhenNotMatched(mergeOpSpec.getWhenNotMatched());
    return docSourceMergeSpec;
}

struct SinkParseResult {
    boost::intrusive_ptr<DocumentSource> documentSource;
    std::unique_ptr<Operator> sinkOperator;
};

SinkParseResult fromEmitSpec(const BSONObj& spec,
                             const boost::intrusive_ptr<ExpressionContext>& expCtx,
                             OperatorFactory* operatorFactory,
                             const stdx::unordered_map<std::string, Connection>& connectionObjs) {
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "Invalid sink: " << spec,
            spec.firstElementFieldName() == Parser::kEmitStageName &&
                spec.firstElement().isABSONObj());

    auto sinkSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sinkSpec.getField(kConnectionNameField);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            "$emit must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    SinkParseResult result;
    if (connectionName == kTestTypeLogToken) {
        result.sinkOperator = std::make_unique<LogSinkOperator>();
    } else if (connectionName == kTestTypeMemoryToken) {
        result.sinkOperator = std::make_unique<InMemorySourceSinkOperator>(1, 0);
    } else {
        uasserted(ErrorCode::kTemporaryUserErrorCode,
                  str::stream() << "Invalid " << Parser::kEmitStageName << sinkSpec);
    }
    return result;
}

SinkParseResult fromMergeSpec(const BSONObj& spec,
                              const boost::intrusive_ptr<ExpressionContext>& expCtx,
                              OperatorFactory* operatorFactory,
                              const stdx::unordered_map<std::string, Connection>& connectionObjs) {
    uassert(ErrorCode::kTemporaryUserErrorCode,
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
        uassert(ErrorCode::kTemporaryUserErrorCode,
                str::stream() << "Only atlas merge connection type is currently supported",
                connection.getType() == ConnectionTypeEnum::Atlas);
        auto options = AtlasConnectionOptions::parse(IDLParserContext("AtlasConnectionOptions"),
                                                     connection.getOptions());
        auto documentSourceMerge = DocumentSourceMerge::parse(
            expCtx, BSON("$merge" << buildDocumentSourceMergeSpec(mergeOpSpec).toBSON()));
        dassert(documentSourceMerge.size() == 1);

        SinkParseResult result;
        result.documentSource = std::move(documentSourceMerge.front());
        documentSourceMerge.pop_front();
        result.sinkOperator = operatorFactory->toOperator(result.documentSource.get());
        return result;
    } else {
        uasserted(ErrorCode::kTemporaryUserErrorCode,
                  str::stream() << "Invalid " << Parser::kMergeStageName << mergeObj);
    }
}

struct SourceParseResult {
    std::unique_ptr<SourceOperator> sourceOperator;
    std::unique_ptr<DocumentTimestampExtractor> timestampExtractor;
    std::unique_ptr<EventDeserializer> eventDeserializer;
};

SourceParseResult makeKafkaSource(const BSONObj& sourceSpec,
                                  const KafkaConnectionOptions& baseOptions,
                                  const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                  OperatorFactory* operatorFactory,
                                  DeadLetterQueue* dlq) {
    auto options = KafkaSourceOptions::parse(IDLParserContext(Parser::kSourceStageName.toString()),
                                             sourceSpec);

    KafkaConsumerOperator::Options internalOptions;
    internalOptions.bootstrapServers = std::string{baseOptions.getBootstrapServers()};
    internalOptions.topicName = std::string{options.getTopic()};
    internalOptions.deadLetterQueue = dlq;

    uassert(ErrorCode::kTemporaryUserErrorCode,
            "Invalid partition count",
            options.getPartitionCount() > 0);
    for (int partition = 0; partition < options.getPartitionCount(); ++partition) {
        KafkaConsumerOperator::PartitionOptions partitionOptions;
        partitionOptions.partition = partition;
        if (options.getAllowedLateness()) {
            partitionOptions.watermarkGeneratorAllowedLatenessMs = *options.getAllowedLateness();
        }
        internalOptions.partitionOptions.push_back(std::move(partitionOptions));
    }

    if (options.getTsFieldOverride()) {
        internalOptions.timestampOutputFieldName = options.getTsFieldOverride()->toString();
    } else {
        internalOptions.timestampOutputFieldName =
            Parser::kDefaultTimestampOutputFieldName.toString();
    }

    uassert(ErrorCode::kTemoraryInternalErrorCode,
            str::stream() << options.kTsFieldOverrideFieldName << " cannot be empty",
            !internalOptions.timestampOutputFieldName.empty());

    SourceParseResult result;
    if (options.getTimeField()) {
        auto exprObj = std::move(*options.getTimeField());
        auto expr = Expression::parseExpression(expCtx.get(), exprObj, expCtx->variablesParseState);
        result.timestampExtractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
        internalOptions.timestampExtractor = result.timestampExtractor.get();
    }

    result.eventDeserializer = std::make_unique<JsonEventDeserializer>();
    internalOptions.deserializer = result.eventDeserializer.get();

    if (baseOptions.getIsTestKafka() && *baseOptions.getIsTestKafka()) {
        internalOptions.isTest = true;
    }

    result.sourceOperator = operatorFactory->toSourceOperator(std::move(internalOptions));
    return result;
}

SourceParseResult fromSourceSpec(const BSONObj& spec,
                                 const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                 OperatorFactory* operatorFactory,
                                 const stdx::unordered_map<std::string, Connection>& connectionObjs,
                                 DeadLetterQueue* dlq) {
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "Invalid $source " << Parser::kSourceStageName << spec,
            spec.firstElementFieldName() == Parser::kSourceStageName &&
                spec.firstElement().isABSONObj());

    auto sourceSpec = spec.firstElement().Obj();
    // Read connectionName field.
    auto connectionField = sourceSpec.getField(kConnectionNameField);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            "$source must contain 'connectionName' field in it",
            connectionField.ok());
    std::string connectionName(connectionField.String());

    if (connectionObjs.contains(connectionName)) {
        const auto& connection = connectionObjs.at(connectionName);
        uassert(ErrorCode::kTemporaryUserErrorCode,
                str::stream() << "Only kafka source connection type is currently supported",
                connection.getType() == ConnectionTypeEnum::Kafka);
        auto options = KafkaConnectionOptions::parse(IDLParserContext("connectionParser"),
                                                     connection.getOptions());
        return makeKafkaSource(sourceSpec, options, expCtx, operatorFactory, dlq);
    } else if (connectionName == kTestTypeMemoryToken) {
        return {std::make_unique<InMemorySourceSinkOperator>(0, 1), nullptr, nullptr};
    } else {
        uasserted(ErrorCode::kTemporaryUserErrorCode,
                  str::stream() << "Invalid " << Parser::kSourceStageName << sourceSpec);
    }
}

}  // namespace

Parser::Parser(const std::vector<Connection>& connections) {
    for (auto& connection : connections) {
        _connectionObjs.emplace(std::make_pair(connection.getName(), connection));
    }
}

unique_ptr<OperatorDag> Parser::fromBson(const std::string& name,
                                         const std::vector<BSONObj>& bsonPipeline) {
    OperatorDag::Options options;
    options.bsonPipeline = bsonPipeline;

    options.context.clientName = name + UUID::gen().toString();
    options.context.client = getGlobalServiceContext()->makeClient(options.context.clientName);
    options.context.opCtx =
        getGlobalServiceContext()->makeOperationContext(options.context.client.get());
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    options.context.expCtx =
        make_intrusive<ExpressionContext>(options.context.opCtx.get(),
                                          std::unique_ptr<CollatorInterface>(nullptr),
                                          NamespaceString{});
    options.context.expCtx->allowDiskUse = true;
    // TODO(STREAMS-219)-PrivatePreview: Considering exposing this as a parameter.
    // Or, set a parameter to dis-allow spilling.
    // We're using the same default as in run_aggregate.cpp.
    // This tempDir is used for spill to disk in $sort, $group, etc. stages
    // in window inner pipelines.
    options.context.expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";

    uassert(ErrorCode::kTemporaryUserErrorCode,
            "Pipeline must have at least one stage",
            bsonPipeline.size() > 0);

    options.dlq = std::make_unique<NoOpDeadLetterQueue>();

    // Get the $source stage
    auto current = bsonPipeline.begin();
    string firstStageName(current->firstElementFieldNameStringData());
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "First stage must be " << kSourceStageName
                          << ", found: " << firstStageName,
            isSourceStage(firstStageName));
    auto sourceSpec = *current;
    auto sourceParseResult = fromSourceSpec(
        sourceSpec, options.context.expCtx, &_operatorFactory, _connectionObjs, options.dlq.get());
    auto sourceOperator = std::move(sourceParseResult.sourceOperator);
    options.eventDeserializer = std::move(sourceParseResult.eventDeserializer);
    options.timestampExtractor = std::move(sourceParseResult.timestampExtractor);

    // Get the middle stages until we hit a sink stage
    std::vector<BSONObj> middleStages;
    current = next(current);
    while (current != bsonPipeline.end() &&
           !isSinkStage(current->firstElementFieldNameStringData())) {
        string stageName(current->firstElementFieldNameStringData());
        _operatorFactory.validateByName(stageName);
        middleStages.emplace_back(*current);
        current = next(current);
    }

    // Get the sink stage
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "Last stage must be " << kEmitStageName << " or " << kMergeStageName,
            current != bsonPipeline.end() &&
                isSinkStage(current->firstElementFieldNameStringData()) &&
                next(current) == bsonPipeline.end());

    BSONObj sinkSpec = std::move(*current);
    SinkParseResult sinkParseResult;
    if (isMergeStage(sinkSpec.firstElementFieldNameStringData())) {
        sinkParseResult =
            fromMergeSpec(sinkSpec, options.context.expCtx, &_operatorFactory, _connectionObjs);
    } else {
        dassert(isEmitStage(sinkSpec.firstElementFieldNameStringData()));
        sinkParseResult =
            fromEmitSpec(sinkSpec, options.context.expCtx, &_operatorFactory, _connectionObjs);
    }

    // Build the DAG
    // Start with the $source
    OperatorDag::OperatorContainer operators;
    operators.push_back(std::move(sourceOperator));

    // Then everything between the source and the $merge/$emit
    if (!middleStages.empty()) {
        auto pipeline = Pipeline::parse(middleStages, options.context.expCtx);
        pipeline->optimizePipeline();
        for (const auto& stage : pipeline->getSources()) {
            auto op = _operatorFactory.toOperator(stage.get());
            // Make this operator the output of the prior operator.
            operators.back()->addOutput(op.get(), 0);
            operators.push_back(std::move(op));
        }
        options.pipeline = std::move(pipeline->getSources());
    }

    // Finally, the sink ($emit or $merge)
    if (sinkParseResult.documentSource) {
        options.pipeline.push_back(std::move(sinkParseResult.documentSource));
    }
    operators.back()->addOutput(sinkParseResult.sinkOperator.get(), 0);
    operators.push_back(std::move(sinkParseResult.sinkOperator));

    return make_unique<OperatorDag>(std::move(options), std::move(operators));
}

};  // namespace streams
