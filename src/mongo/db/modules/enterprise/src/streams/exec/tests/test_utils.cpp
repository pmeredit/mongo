/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/tests/test_utils.h"
#include "mongo/db/matcher/parsed_match_expression_for_test.h"
#include "mongo/db/service_context.h"
#include "streams/exec/constants.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/old_in_memory_checkpoint_storage.h"

using namespace mongo;

namespace streams {

std::tuple<std::unique_ptr<Context>, std::unique_ptr<Executor>> getTestContext(
    mongo::ServiceContext* svcCtx, std::string tenantId, std::string streamProcessorId) {
    if (!svcCtx) {
        svcCtx = getGlobalServiceContext();
    }

    auto context = std::make_unique<Context>();
    context->streamName = "test";
    context->clientName = context->streamName + "-" + UUID::gen().toString();
    context->client = svcCtx->getService()->makeClient(context->clientName);
    context->opCtx = svcCtx->makeOperationContext(context->client.get());
    context->tenantId = tenantId;
    context->streamProcessorId = streamProcessorId;
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(
        context->opCtx.get(),
        std::unique_ptr<CollatorInterface>(nullptr),
        NamespaceString(DatabaseName::createDatabaseName_forTest(boost::none, "test")));
    context->expCtx->allowDiskUse = false;
    context->dlq = std::make_unique<InMemoryDeadLetterQueue>(context.get());
    Executor::Options options;
    auto executor = std::make_unique<Executor>(context.get(), options);
    context->dlq->registerMetrics(executor->getMetricManager());

    return std::make_tuple(std::move(context), std::move(executor));
}

BSONObj getTestLogSinkSpec() {
    return BSON(kEmitStageName << BSON("connectionName" << kTestLogConnectionName));
}

BSONObj getTestMemorySinkSpec() {
    return BSON(kEmitStageName << BSON("connectionName" << kTestMemoryConnectionName));
}

BSONObj getTestSourceSpec() {
    return BSON(kSourceStageName << BSON("connectionName" << kTestMemoryConnectionName));
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


mongo::stdx::unordered_map<std::string, mongo::Connection> testInMemoryConnectionRegistry() {
    mongo::Connection connection(std::string(kTestMemoryConnectionName),
                                 mongo::ConnectionTypeEnum::InMemory,
                                 /* options */ mongo::BSONObj());
    return {{std::string(kTestMemoryConnectionName), connection}};
}

mongo::BSONObj testKafkaSourceSpec(int partitionCount) {
    auto sourceOptions = BSON("connectionName"
                              << "kafka1"
                              << "topic"
                              << "topic1"
                              << "timeField"
                              << fromjson(R"({ $dateFromString : { "dateString" : "$timestamp"} })")
                              << "testOnlyPartitionCount" << partitionCount);
    return BSON("$source" << sourceOptions);
}

std::unique_ptr<OldCheckpointStorage> makeCheckpointStorage(ServiceContext* serviceContext,
                                                            Context* context,
                                                            const std::string& collection,
                                                            const std::string& database) {
    if (const char* envMongodbUri = std::getenv("CHECKPOINT_TEST_MONGODB_URI")) {
        MongoCxxClientOptions mongoClientOptions;
        mongoClientOptions.svcCtx = serviceContext;
        mongoClientOptions.uri = std::string{envMongodbUri};
        mongoClientOptions.database = database;
        mongoClientOptions.collection = collection;
        MongoDBCheckpointStorage::Options internalOptions{
            .svcCtx = serviceContext, .mongoClientOptions = std::move(mongoClientOptions)};
        return std::make_unique<MongoDBCheckpointStorage>(context, std::move(internalOptions));
    } else {
        return std::make_unique<OldInMemoryCheckpointStorage>(context);
    }
}

BSONObj sanitizeDoc(const BSONObj& obj) {
    return obj.removeFields(StringDataSet{"_ts", "_stream_meta"});
}

std::shared_ptr<MongoDBProcessInterface> makeMongoDBProcessInterface(
    ServiceContext* serviceContext,
    const std::string& uri,
    const std::string& database,
    const std::string& collection) {
    MongoCxxClientOptions options;
    options.svcCtx = serviceContext;
    options.uri = uri;
    options.database = database;
    options.collection = collection;
    return std::make_shared<MongoDBProcessInterface>(std::move(options));
}

size_t getNumDlqDocsFromOperatorDag(const OperatorDag& dag) {
    size_t accum = 0;
    for (const auto& op : dag.operators()) {
        accum += op->getStats().numDlqDocs;
    }
    return accum;
}


std::shared_ptr<OperatorDag> makeDagFromBson(const std::vector<mongo::BSONObj>& bsonPipeline,
                                             std::unique_ptr<Context>& context,
                                             std::unique_ptr<Executor>& executor,
                                             OperatorDagTest& dagTest) {
    Planner planner(context.get(), /*options*/ {});
    auto dag = planner.plan(bsonPipeline);
    dagTest.registerMetrics(dag.get(), executor->getMetricManager());
    return dag;
}

std::vector<StreamMsgUnion> queueToVector(std::deque<StreamMsgUnion> queue) {
    std::vector<StreamMsgUnion> result;
    while (!queue.empty()) {
        result.push_back(std::move(queue.front()));
        queue.pop_front();
    }
    return result;
}

};  // namespace streams
