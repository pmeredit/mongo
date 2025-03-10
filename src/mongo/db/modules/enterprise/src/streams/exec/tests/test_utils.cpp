/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/tests/test_utils.h"

#include <memory>
#include <sstream>

#include "mongo/db/service_context.h"
#include "mongo/util/net/http_client_mock.h"
#include "streams/exec/constants.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/source_buffer_manager.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/exec/test_constants.h"
#include "streams/util/concurrent_memory_aggregator.h"
#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {

namespace {
constexpr double kStateSizeTolerancePercentage{0.3};
}

void assertStateSize(int64_t expected, int64_t actual) {
    double percentDiff = std::abs(double(actual - expected) / expected);
    ASSERT_LT(percentDiff, kStateSizeTolerancePercentage);
}

std::tuple<std::unique_ptr<Context>, std::unique_ptr<Executor>> getTestContext(
    mongo::ServiceContext* svcCtx,
    std::string tenantId,
    std::string streamProcessorId,
    ConcurrentMemoryAggregator* memoryAggregator) {
    static std::unique_ptr<ConcurrentMemoryAggregator> globalTestMemoryAggregator =
        std::make_unique<ConcurrentMemoryAggregator>(nullptr);

    if (!svcCtx) {
        svcCtx = getGlobalServiceContext();
    }

    auto context = std::make_unique<Context>();
    context->streamName = "test";
    context->clientName = context->streamName + "-" + UUID::gen().toString();

    memoryAggregator =
        memoryAggregator != nullptr ? memoryAggregator : globalTestMemoryAggregator.get();
    context->memoryAggregator =
        memoryAggregator->createChunkedMemoryAggregator(ChunkedMemoryAggregator::Options());

    context->sourceBufferManager = std::make_shared<NoOpSourceBufferManager>();

    context->client = svcCtx->getService()->makeClient(context->clientName);
    context->opCtx = svcCtx->makeOperationContext(context->client.get());
    context->tenantId = tenantId;
    context->streamProcessorId = streamProcessorId;

    // Streams has it's own $lookup semantics and syntax that aren't allowed in regular MQL. We set
    // 'allowGenericForeignDbLookup' to true so this syntax can be parsed in DocumentSourceLookup
    // without throwing errors.
    context->expCtx =
        ExpressionContextBuilder{}
            .opCtx(context->opCtx.get())
            .ns(NamespaceString(DatabaseName::createDatabaseName_forTest(boost::none, "test")))
            .allowDiskUse(false)
            .allowGenericForeignDbLookup(true)
            .build();
    context->dlq = std::make_unique<InMemoryDeadLetterQueue>(context.get());
    auto executor = std::make_unique<Executor>(
        context.get(), Executor::Options{.metricManager = std::make_unique<MetricManager>()});
    context->dlq->registerMetrics(executor->getMetricManager());
    context->streamMetaFieldName = "_stream_meta";
    context->featureFlags = StreamProcessorFeatureFlags{{}, Date_t::now()};
    context->concurrentCheckpointController = std::make_shared<ConcurrentCheckpointController>(1);
    context->region = "us-east-1";

    return std::make_tuple(std::move(context), std::move(executor));
}

BSONObj getTestLogSinkSpec() {
    return BSON(kEmitStageName << BSON("connectionName" << kTestLogConnectionName));
}

BSONObj getTestMemorySinkSpec() {
    return BSON(kEmitStageName << BSON("connectionName" << kTestMemoryConnectionName));
}

BSONObj getNoOpSinkSpec() {
    return BSON(kEmitStageName << BSON("connectionName" << kNoOpSinkOperatorConnectionName));
}

BSONObj getTestSourceSpec() {
    return BSON(kSourceStageName << BSON("connectionName" << kTestMemoryConnectionName));
}

std::vector<BSONObj> parseBsonVector(std::string json) {
    const auto inputBson = fromjson("{pipeline: " + json + "}");
    return parsePipelineFromBSON(inputBson["pipeline"]);
}

std::unique_ptr<ConnectionCollection> testKafkaConnections() {
    KafkaConnectionOptions kafkaOptions("");
    kafkaOptions.setIsTestKafka(true);
    mongo::Connection connection("kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
    return std::make_unique<ConnectionCollection>(std::vector<Connection>{connection});
}


std::vector<mongo::Connection> testInMemoryConnections() {
    mongo::Connection connection(std::string(kTestMemoryConnectionName),
                                 mongo::ConnectionTypeEnum::InMemory,
                                 /* options */ mongo::BSONObj());
    return {connection};
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

BSONObj sanitizeDoc(const BSONObj& obj) {
    return obj.removeFields(StringDataSet{"_ts", "_stream_meta"});
}

size_t getNumDlqDocsFromOperatorDag(const OperatorDag& dag) {
    size_t accum = 0;
    for (const auto& op : dag.operators()) {
        accum += op->getStats().numDlqDocs;
    }
    return accum;
}

size_t getNumDlqBytesFromOperatorDag(const OperatorDag& dag) {
    size_t accumBytes = 0;
    for (const auto& op : dag.operators()) {
        accumBytes += op->getStats().numDlqBytes;
    }

    return accumBytes;
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

std::string TestMetricsVisitor::getLabelsAsStrs(const MetricManager::LabelsVec& labels) {
    std::stringstream labelsStr;
    for (const auto& label : labels) {
        if (label.first == kProcessorIdLabelKey || label.first == kTenantIdLabelKey ||
            label.first == kProcessorNameLabelKey) {
            continue;
        }

        labelsStr << label.first << "=" << label.second << ",";
    }

    return labelsStr.str();
}

StubbableMockHttpClient::StubbableMockHttpClient(boost::optional<std::function<void()>> stubFn)
    : _mockClient{std::make_unique<MockHttpClient>()}, _stubFn{stubFn} {}

void StubbableMockHttpClient::expect(mongo::MockHttpClient::Request request,
                                     mongo::MockHttpClient::Response response) {
    _mockClient->expect(request, response);
}

HttpClient::HttpReply StubbableMockHttpClient::request(HttpMethod method,
                                                       mongo::StringData url,
                                                       mongo::ConstDataRange data) const {
    if (_stubFn.has_value()) {
        (*_stubFn)();
    }

    return _mockClient->request(method, url, data);
}

};  // namespace streams
