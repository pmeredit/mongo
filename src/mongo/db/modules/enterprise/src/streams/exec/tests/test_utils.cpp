/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/tests/test_utils.h"
#include "mongo/db/matcher/parsed_match_expression_for_test.h"
#include "mongo/db/service_context.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/parser.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"

using namespace mongo;

namespace streams {

std::unique_ptr<Context> getTestContext(mongo::ServiceContext* svcCtx,
                                        MetricManager* metricManager) {
    if (!svcCtx) {
        svcCtx = getGlobalServiceContext();
    }

    auto context = std::make_unique<Context>();
    context->metricManager = metricManager;
    context->streamName = "test";
    context->clientName = context->streamName + "-" + UUID::gen().toString();
    context->client = svcCtx->makeClient(context->clientName);
    context->opCtx = svcCtx->makeOperationContext(context->client.get());
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(
        context->opCtx.get(),
        std::unique_ptr<CollatorInterface>(nullptr),
        NamespaceString(DatabaseName::createDatabaseName_forTest(boost::none, "testDB")));
    context->expCtx->allowDiskUse = false;
    // TODO(STREAMS-219)-PrivatePreview: Considering exposing this as a parameter.
    // Or, set a parameter to dis-allow spilling.
    // We're using the same default as in run_aggregate.cpp.
    // This tempDir is used for spill to disk in $sort, $group, etc. stages
    // in window inner pipelines.
    context->expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";
    context->dlq = std::make_unique<InMemoryDeadLetterQueue>(context.get());
    return context;
}

BSONObj getTestLogSinkSpec() {
    return BSON(Parser::kEmitStageName << BSON("connectionName" << kTestLogConnectionName));
}

BSONObj getTestMemorySinkSpec() {
    return BSON(Parser::kEmitStageName << BSON("connectionName" << kTestMemoryConnectionName));
}

BSONObj getTestSourceSpec() {
    return BSON(Parser::kSourceStageName << BSON("connectionName" << kTestMemoryConnectionName));
}

std::vector<BSONObj> parseBsonVector(std::string json) {
    const auto inputBson = fromjson("{pipeline: " + json + "}");
    return parsePipelineFromBSON(inputBson["pipeline"]);
}

mongo::stdx::unordered_map<std::string, mongo::Connection> testKafkaConnectionRegistry() {
    KafkaConnectionOptions kafkaOptions("");
    kafkaOptions.setIsTestKafka(true);
    mongo::Connection connection("kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
    return {{"kafka1", connection}};
}

mongo::BSONObj testKafkaSourceSpec(int partitionCount) {
    auto sourceOptions = BSON("connectionName"
                              << "kafka1"
                              << "topic"
                              << "topic1"
                              << "timeField"
                              << fromjson(R"({ $dateFromString : { "dateString" : "$timestamp"} })")
                              << "partitionCount" << partitionCount << "allowedLateness"
                              << fromjson(R"({ size: 0, unit: "second"})"));
    return BSON("$source" << sourceOptions);
}

std::unique_ptr<CheckpointStorage> makeCheckpointStorage(ServiceContext* serviceContext,
                                                         const std::string& collection,
                                                         const std::string& database) {
    if (const char* envMongodbUri = std::getenv("CHECKPOINT_TEST_MONGODB_URI")) {
        return std::make_unique<MongoDBCheckpointStorage>(MongoDBCheckpointStorage::Options{
            .tenantId = UUID::gen().toString(),
            .streamProcessorId = UUID::gen().toString(),
            .svcCtx = serviceContext,
            .mongodbUri = std::string{envMongodbUri},
            .database = database,
            .collection = collection,
        });
    } else {
        return std::make_unique<InMemoryCheckpointStorage>();
    }
}

};  // namespace streams
