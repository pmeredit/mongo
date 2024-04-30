/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/json.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/thread.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/concurrent_memory_aggregator.h"
#include "mongo/util/processinfo.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/config_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tenant_feature_flags.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/management/stream_manager.h"
#include "streams/util/exception.h"

#include <chrono>
#include <filesystem>
#include <memory>

namespace streams {

using namespace mongo;

namespace {

constexpr const char kTestTenantId1[] = "tenant1";
constexpr const char kTestTenantId2[] = "tenant2";
static constexpr int64_t kMemoryUsageBatchSize = 32 * 1024 * 1024;  // 32 MB

};  // namespace

class StreamManagerTest : public AggregationContextFixture {
public:
    bool streamProcessorExists(StreamManager* streamManager, std::string name) {
        stdx::lock_guard<Latch> lk(streamManager->_mutex);
        return streamManager->_processors.contains(name);
    }

    bool isStreamProcessorConnected(StreamManager* streamManager, std::string name) {
        stdx::lock_guard<Latch> lk(streamManager->_mutex);
        auto it = streamManager->_processors.find(name);
        if (it == streamManager->_processors.end()) {
            return false;
        }
        return it->second->executor->isConnected();
    }

    void createStreamProcessor(
        StreamManager* streamManager,
        StartStreamProcessorCommand request,
        int64_t testOnlyDocsQueueMaxSizeBytes = std::numeric_limits<int64_t>::max()) {
        stdx::lock_guard<Latch> lk(streamManager->_mutex);
        auto info = streamManager->createStreamProcessorInfo(lk, request);
        auto executorOptions = info->executor->_options;
        executorOptions.testOnlyDocsQueueMaxSizeBytes = testOnlyDocsQueueMaxSizeBytes;
        info->executor =
            std::make_unique<Executor>(info->context.get(), std::move(executorOptions));
        auto [it, _] = streamManager->_processors.emplace(
            std::make_pair(request.getName().toString(), std::move(info)));

        // Register metrics and start all operators.
        for (auto& op : it->second->operatorDag->operators()) {
            op->registerMetrics(streamManager->_metricManager.get());
        }
        it->second->operatorDag->start();
        it->second->executor->ensureConnected(Date_t::now() + mongo::Seconds(15));
    }

    void stopStreamProcessor(StreamManager* streamManager, std::string tenantId, StringData name) {
        StopStreamProcessorCommand stopRequest;
        stopRequest.setTenantId(tenantId);
        stopRequest.setName(name);
        stopRequest.setProcessorId(StringData(name));
        stopRequest.setTimeout(mongo::Seconds(60));
        streamManager->stopStreamProcessor(stopRequest);
    }

    void stopStreamProcessor(StreamManager* streamManager,
                             std::string tenantId,
                             std::string name,
                             StopReason stopReason) {
        StopStreamProcessorCommand stopCommand;
        stopCommand.setTenantId(tenantId);
        stopCommand.setName(name);
        stopCommand.setProcessorId(StringData(name));
        stopCommand.setTimeout(mongo::Seconds(60));
        streamManager->stopStreamProcessor(stopCommand, stopReason);
    }

    StreamManager::StreamProcessorInfo* getStreamProcessorInfo(StreamManager* streamManager,
                                                               const std::string& name) {
        return streamManager->_processors.at(name).get();
    }

    void insert(StreamManager* streamManager,
                const std::string streamName,
                const std::vector<BSONObj>& documents) {
        auto spInfo = getStreamProcessorInfo(streamManager, streamName);
        spInfo->executor->testOnlyInsertDocuments(std::move(documents));
    }

    void runOnce(StreamManager* streamManager, const std::string streamName) {
        auto spInfo = getStreamProcessorInfo(streamManager, streamName);
        spInfo->executor->runOnce();
    }

    const auto& getTestOnlyDocs(StreamManager* streamManager, const std::string& streamName) {
        auto spInfo = getStreamProcessorInfo(streamManager, streamName);
        return spInfo->executor->_testOnlyDocsQueue;
    }

    ConcurrentMemoryAggregator* getMemoryAggregator(StreamManager* streamManager) {
        return streamManager->_memoryAggregator.get();
    }

    void checkSPMemoryUsage(const StreamManager* streamManager,
                            const std::string& spID,
                            int64_t expectedMemoryUsage) const {
        auto it = streamManager->_processors.find(spID);
        ASSERT_NOT_EQUALS(it, streamManager->_processors.end());
        ASSERT_EQUALS(expectedMemoryUsage,
                      it->second->context->memoryAggregator->getCurrentMemoryUsageBytes());
    }

    void ensureSPsOOMKilled(const std::vector<ListStreamProcessorsReplyItem>& sps) const {
        for (const auto& sp : sps) {
            const auto& err = sp.getError();
            ASSERT_TRUE(err);
            ASSERT_EQUALS(ErrorCodes::Error::ExceededMemoryLimit, err->getCode());
            ASSERT_EQUALS("stream processing instance out of memory", err->getReason());
            ASSERT_EQUALS(false, err->getRetryable());
        }
    }

    void checkExceededMemoryLimitSignal(const StreamManager* streamManager, bool expected) const {
        ASSERT_EQUALS(expected, streamManager->_memoryUsageMonitor->hasExceededMemoryLimit());
    }

    bool poll(std::function<bool()> func, Seconds timeout = Seconds{5000}) {
        auto deadline = Date_t::now() + timeout;
        while (Date_t::now() < deadline) {
            if (func()) {
                return true;
            }
        }
        return false;
    }

    const auto& getNumStreamProcessorsByStatus(const StreamManager* streamManager) const {
        return streamManager->_numStreamProcessorsByStatusGauges;
    }

    void updateContextFeatureFlags(StreamManager::StreamProcessorInfo* processorInfo,
                                   std::shared_ptr<TenantFeatureFlags> featureFlags) {
        processorInfo->executor->_tenantFeatureFlags = featureFlags;
        processorInfo->executor->_featureFlagsUpdated = true;
        processorInfo->executor->updateContextFeatureFlags();
    }

    std::chrono::milliseconds getCheckpointInterval(
        StreamManager::StreamProcessorInfo* processorInfo) {
        return processorInfo->executor->_context->checkpointInterval;
    }
};

TEST_F(StreamManagerTest, Start) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("name1"));
    request.setCorrelationId(StringData("userRequest1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request);
    ASSERT(streamProcessorExists(streamManager.get(), "name1"));
    stopStreamProcessor(
        streamManager.get(), kTestTenantId1, "name1", StopReason::ExternalStopRequest);
    ASSERT(!streamProcessorExists(streamManager.get(), "name1"));
}

TEST_F(StreamManagerTest, ConcurrentStartStop_StopAfterConnection) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});

    // Start 10 stream processors asynchronously on 10 threads.
    std::vector<stdx::thread> startThreads;
    for (int i = 0; i < 10; ++i) {
        startThreads.push_back(stdx::thread([i, &streamManager, this]() {
            StartStreamProcessorCommand request;
            request.setTenantId(StringData(kTestTenantId1));
            auto processorName = fmt::format("name{}", i);
            request.setName(processorName);
            request.setProcessorId(StringData(processorName));
            request.setCorrelationId(StringData("userRequest"));
            request.setPipeline(
                {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
            request.setConnections({mongo::Connection(
                "__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
            request.setOptions(mongo::StartOptions{});
            streamManager->startStreamProcessor(request);
        }));
    }

    // Stop the 10 stream processors asynchronously.
    std::vector<stdx::thread> stopThreads;
    for (size_t i = 0; i < 10; ++i) {
        stopThreads.push_back(stdx::thread([i, &streamManager, this]() {
            // Wait until the stream processor is connected before stopping it.
            auto processorName = fmt::format("name{}", i);
            while (!isStreamProcessorConnected(streamManager.get(), processorName)) {
                sleepFor(Milliseconds(100));
            }
            stopStreamProcessor(streamManager.get(),
                                kTestTenantId1,
                                processorName,
                                StopReason::ExternalStopRequest);
        }));
    }

    for (size_t i = 0; i < startThreads.size(); ++i) {
        startThreads[i].join();
        stopThreads[i].join();
    }

    ListStreamProcessorsCommand listRequest;
    listRequest.setCorrelationId(StringData("userRequest1"));
    auto listReply = streamManager->listStreamProcessors(listRequest);
    ASSERT_TRUE(listReply.getStreamProcessors().empty());
}

TEST_F(StreamManagerTest, ConcurrentStartStop_StopDuringConnection) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});

    // Start a stream processor asynchronously and make it sleep for 10s while establishing
    // connections.
    setGlobalFailPoint("streamProcessorStartSleepSeconds",
                       BSON("mode"
                            << "alwaysOn"
                            << "data" << BSON("sleepSeconds" << 10)));

    stdx::thread startThread = stdx::thread([&]() {
        StartStreamProcessorCommand request;
        request.setTenantId(StringData(kTestTenantId1));
        auto processorName = "name";
        request.setName(processorName);
        request.setProcessorId(StringData(processorName));
        request.setCorrelationId(StringData("userRequest"));
        request.setPipeline(
            {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
        request.setConnections({mongo::Connection(
            "__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
        request.setOptions(mongo::StartOptions{});
        // Test that startStreamProcessor() fails due to stream processor being removed while it is
        // still starting.
        ASSERT_THROWS(streamManager->startStreamProcessor(request), DBException);
    });

    // Stop the stream processor asynchronously before it has established connections.
    stdx::thread stopThread = stdx::thread([&]() {
        // Wait until the stream processor is connected before stopping it.
        auto processorName = "name";
        while (!streamProcessorExists(streamManager.get(), processorName)) {
            sleepFor(Milliseconds(100));
        }
        ASSERT_FALSE(isStreamProcessorConnected(streamManager.get(), processorName));
        stopStreamProcessor(
            streamManager.get(), kTestTenantId1, processorName, StopReason::ExternalStopRequest);
    });

    startThread.join();
    stopThread.join();

    ListStreamProcessorsCommand listRequest;
    listRequest.setCorrelationId(StringData("userRequest1"));
    auto listReply = streamManager->listStreamProcessors(listRequest);
    ASSERT_TRUE(listReply.getStreamProcessors().empty());
}

TEST_F(StreamManagerTest, StartTimesOut) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});

    // Start a stream processor asynchronously and make it sleep for 100s while establishing
    // connections.
    setGlobalFailPoint("streamProcessorStartSleepSeconds",
                       BSON("mode"
                            << "alwaysOn"
                            << "data" << BSON("sleepSeconds" << 15)));

    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    auto processorName = "name";
    request.setName(processorName);
    request.setProcessorId(StringData(processorName));
    request.setCorrelationId(StringData("userRequest"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});
    request.setTimeout(mongo::Seconds{10});
    // Test that startStreamProcessor() fails with timeout error.
    ASSERT_THROWS_WHAT(
        streamManager->startStreamProcessor(request), DBException, "Timeout while connecting"_sd);

    stopStreamProcessor(
        streamManager.get(), kTestTenantId1, processorName, StopReason::ExternalStopRequest);
    ASSERT_FALSE(streamProcessorExists(streamManager.get(), processorName));

    ListStreamProcessorsCommand listRequest;
    listRequest.setCorrelationId(StringData("userRequest1"));
    auto listReply = streamManager->listStreamProcessors(listRequest);
    ASSERT_TRUE(listReply.getStreamProcessors().empty());

    // Deactivate the fail point.
    setGlobalFailPoint("streamProcessorStartSleepSeconds",
                       BSON("mode"
                            << "off"));
}

TEST_F(StreamManagerTest, GetStats) {
    const std::string streamName = "name1";
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData(streamName));
    request.setProcessorId(StringData(streamName));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("id" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request);

    insert(streamManager.get(),
           streamName,
           {
               BSON("id" << 1),
               BSON("id" << 2),
           });

    GetStatsCommand getStatsRequest;
    getStatsRequest.setTenantId(StringData(kTestTenantId1));
    getStatsRequest.setName(StringData(streamName));
    getStatsRequest.setScale(1);
    getStatsRequest.setVerbose(true);
    getStatsRequest.setCorrelationId(StringData("getStatsUserRequest1"));

    // Poll stats until the doc has made it to the output sink.
    auto statsReply = streamManager->getStats(getStatsRequest);
    while (statsReply.getOutputMessageCount() < 1) {
        statsReply = streamManager->getStats(getStatsRequest);
    }

    ASSERT_EQUALS(streamName, statsReply.getName());
    ASSERT_EQUALS(streamName, statsReply.getProcessorId());
    ASSERT_EQUALS(StreamStatusEnum::Running, statsReply.getStatus());
    ASSERT_EQUALS(1, statsReply.getScaleFactor());
    ASSERT_EQUALS(2, statsReply.getInputMessageCount());
    ASSERT_EQUALS(506, statsReply.getInputMessageSize());
    ASSERT_EQUALS(1, statsReply.getOutputMessageCount());
    ASSERT_EQUALS(253, statsReply.getOutputMessageSize());

    // Since this stream processor is not using a kafka source, it should not return `partitions`.
    ASSERT_FALSE(statsReply.getKafkaPartitions());

    // Ensure that the operator stats are returned in verbose mode.
    // Three operators - Source, match, and sink operator.
    auto operatorStatsWrap = statsReply.getOperatorStats();
    ASSERT_TRUE(operatorStatsWrap);

    auto operatorStats = operatorStatsWrap.get();
    ASSERT_EQUALS(3, operatorStats.size());

    ASSERT_EQUALS("InMemorySourceOperator", operatorStats[0].getName());
    ASSERT_EQUALS(statsReply.getInputMessageCount(), operatorStats[0].getInputMessageCount());
    ASSERT_EQUALS(statsReply.getInputMessageSize(), operatorStats[0].getInputMessageSize());
    ASSERT_EQUALS(2, operatorStats[0].getOutputMessageCount());

    ASSERT_EQUALS("MatchOperator", operatorStats[1].getName());
    ASSERT_EQUALS(operatorStats[0].getOutputMessageCount(),
                  operatorStats[1].getInputMessageCount());
    ASSERT_EQUALS(1, operatorStats[1].getOutputMessageCount());

    ASSERT_EQUALS("LogSinkOperator", operatorStats[2].getName());
    ASSERT_EQUALS(statsReply.getOutputMessageCount(), operatorStats[2].getInputMessageCount());
    ASSERT_EQUALS(statsReply.getOutputMessageSize(), operatorStats[2].getInputMessageSize());

    stopStreamProcessor(streamManager.get(), kTestTenantId1, streamName);
}

TEST_F(StreamManagerTest, GetStats_Kafka) {
    const std::string streamName = "sp1";
    const int32_t partitionCount = 12;

    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData(streamName));
    request.setProcessorId(StringData(streamName));
    request.setPipeline({BSON("$source" << BSON("connectionName"
                                                << "kafka"
                                                << "topic"
                                                << "input"
                                                << "testOnlyPartitionCount" << partitionCount)),
                         getTestLogSinkSpec()});
    request.setConnections({mongo::Connection("kafka",
                                              mongo::ConnectionTypeEnum::Kafka,
                                              BSON("bootstrapServers"
                                                   << "localhost:9092"
                                                   << "isTestKafka" << true))});
    request.setOptions(mongo::StartOptions{});

    // Create rather than start since we'll be calling `runOnce()` manually within this function.
    createStreamProcessor(streamManager.get(), request, std::numeric_limits<int64_t>::max());

    GetStatsCommand getStatsRequest;
    getStatsRequest.setTenantId(StringData(kTestTenantId1));
    getStatsRequest.setName(StringData(streamName));
    getStatsRequest.setVerbose(true);
    getStatsRequest.setCorrelationId(StringData("getStatsUserRequest1"));

    insert(streamManager.get(),
           streamName,
           {
               BSON("id" << 1),
           });

    // Wait until the input document has been processed.
    while (getTestOnlyDocs(streamManager.get(), streamName).getStats().queueDepth == 0) {
    }

    // Run the first time to consume the document, and then run the second time to update
    // the kafka consumer partition state snapshot.
    runOnce(streamManager.get(), streamName);
    runOnce(streamManager.get(), streamName);

    // Poll stats until the doc has made it to the output sink.
    auto statsReply = streamManager->getStats(getStatsRequest);

    ASSERT_EQUALS(streamName, statsReply.getName());
    ASSERT_EQUALS(StreamStatusEnum::Running, statsReply.getStatus());

    // Since this stream processor has a kafka source, it should include the `partitions` field
    // in the `getStats` reply.
    ASSERT_TRUE(statsReply.getKafkaPartitions());
    ASSERT_EQUALS(partitionCount, statsReply.getKafkaPartitions()->size());

    for (int32_t partition = 0; partition < partitionCount; ++partition) {
        const auto& state = statsReply.getKafkaPartitions()->at(partition);
        ASSERT_EQUALS(partition, state.getPartition());

        // Only one input document was sent, which should have been sent to partition 0.
        if (partition == 0) {
            ASSERT_EQUALS(1, state.getCurrentOffset());
        } else {
            ASSERT_EQUALS(0, state.getCurrentOffset());
        }

        ASSERT_EQUALS(0, state.getCheckpointOffset());
    }
}

TEST_F(StreamManagerTest, GetMetrics) {
    const std::string streamProcessorName = "sp1";
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData(streamProcessorName));
    request.setProcessorId(StringData(streamProcessorName));
    request.setPipeline({getTestSourceSpec(), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request);

    streamManager->getMetrics();
    const auto& numStreamProcessorsByStatus = getNumStreamProcessorsByStatus(streamManager.get());
    for (size_t i = 0; i < numStreamProcessorsByStatus.size(); ++i) {
        double actualValue = numStreamProcessorsByStatus[i]->value();
        double actualSnapshotValue = numStreamProcessorsByStatus[i]->snapshotValue();
        double expectedValue = 0;

        if (StreamStatusEnum(i) == StreamStatusEnum::Running) {
            expectedValue = 1;
        }

        ASSERT_APPROX_EQUAL(expectedValue, actualValue, 0.0000001);
        ASSERT_APPROX_EQUAL(expectedValue, actualSnapshotValue, 0.0000001);
    }

    stopStreamProcessor(streamManager.get(), kTestTenantId1, streamProcessorName);
}

TEST_F(StreamManagerTest, List) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request1;
    request1.setTenantId(StringData(kTestTenantId1));
    request1.setName(StringData("name1"));
    request1.setProcessorId(StringData("name1"));
    request1.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request1.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request1.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request1);

    StartStreamProcessorCommand request2;
    request2.setTenantId(StringData(kTestTenantId1));
    request2.setName(StringData("name2"));
    request2.setProcessorId(StringData("name2"));
    request2.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request2.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request2.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request2);

    ListStreamProcessorsCommand listRequest;
    listRequest.setCorrelationId(StringData("userRequest1"));
    auto listReply = streamManager->listStreamProcessors(listRequest);
    ASSERT_EQUALS(2, listReply.getStreamProcessors().size());

    auto& sps = listReply.getStreamProcessors();
    std::sort(sps.begin(), sps.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return lhs.getName().compare(rhs.getName()) < 0;
    });

    const auto& sp0 = sps[0];
    ASSERT_EQUALS(StringData(kTestTenantId1), sp0.getTenantId());
    ASSERT_EQUALS(StringData("name1"), sp0.getProcessorId());
    ASSERT_EQUALS(StringData("name1"), sp0.getName());

    const auto& sp1 = sps[1];
    ASSERT_EQUALS(StringData(kTestTenantId1), sp1.getTenantId());
    ASSERT_EQUALS(StringData("name2"), sp1.getProcessorId());
    ASSERT_EQUALS(StringData("name2"), sp1.getName());

    stopStreamProcessor(
        streamManager.get(), kTestTenantId1, "name1", StopReason::ExternalStopRequest);
    stopStreamProcessor(
        streamManager.get(), kTestTenantId1, "name2", StopReason::ExternalStopRequest);
    listReply = streamManager->listStreamProcessors(listRequest);
    ASSERT_EQUALS(0, listReply.getStreamProcessors().size());
}

TEST_F(StreamManagerTest, ErrorHandling) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("name1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request);
    ASSERT(streamProcessorExists(streamManager.get(), "name1"));

    // Inject an exception into the executor.
    auto processorInfo = getStreamProcessorInfo(streamManager.get(), "name1");
    processorInfo->executor->testOnlyInjectException(
        std::make_exception_ptr(std::runtime_error("hello exception")));

    // Verify that the exception causes the streamProcessor to enter an error status.
    auto success = poll([&]() {
        ListStreamProcessorsCommand listRequest;
        listRequest.setCorrelationId(StringData("userRequest2"));
        auto reply = streamManager->listStreamProcessors(listRequest).getStreamProcessors();
        auto it = std::find_if(
            reply.begin(), reply.end(), [&](auto sp) { return sp.getName() == request.getName(); });
        ASSERT_NOT_EQUALS(it, reply.end());
        bool isError = it->getStatus() == StreamStatusEnum::Error;
        if (isError) {
            ASSERT(it->getError());
            ASSERT_EQUALS(ErrorCodes::InternalError, it->getError()->getCode());
            ASSERT_EQUALS("Caught std::exception of type std::runtime_error: hello exception",
                          it->getError()->getReason());
            ASSERT_EQUALS(true, it->getError()->getRetryable());
        }
        return isError;
    });
    ASSERT(success);

    // Call stopStreamProcessor to remove the erroring streamProcessor from memory.
    ASSERT(streamProcessorExists(streamManager.get(), request.getName().toString()));
    stopStreamProcessor(streamManager.get(), kTestTenantId1, request.getName().toString());
    ASSERT(!streamProcessorExists(streamManager.get(), request.getName().toString()));
}

// Verifies that checkpointing is disabled for sources other than Kafka and Changestream.
TEST_F(StreamManagerTest, DisableCheckpoint) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});

    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("name1"));
    request.setPipeline({getTestSourceSpec(), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request);
    ASSERT(streamProcessorExists(streamManager.get(), "name1"));
    auto processorInfo = getStreamProcessorInfo(streamManager.get(), "name1");
    // Verify checkpointing is disabled.
    ASSERT(!processorInfo->context->checkpointStorage.get());
    stopStreamProcessor(streamManager.get(), kTestTenantId1, request.getName().toString());
    ASSERT(!streamProcessorExists(streamManager.get(), request.getName().toString()));

    StartStreamProcessorCommand request2;
    request2.setTenantId(StringData(kTestTenantId1));
    request2.setName(StringData("name2"));
    request2.setProcessorId(StringData("name2"));
    request2.setPipeline({BSON(kSourceStageName << BSON("connectionName"
                                                        << "sample_data_solar")),
                          getTestLogSinkSpec()});
    request2.setConnections({mongo::Connection(
        "sample_data_solar", mongo::ConnectionTypeEnum::SampleSolar, mongo::BSONObj())});
    request2.setOptions(mongo::StartOptions{});
    streamManager->startStreamProcessor(request2);
    ASSERT(streamProcessorExists(streamManager.get(), "name2"));
    auto processorInfo2 =
        getStreamProcessorInfo(streamManager.get(), request2.getName().toString());
    // Verify checkpointing is disabled.
    ASSERT(!processorInfo2->context->checkpointStorage.get());
    stopStreamProcessor(streamManager.get(), kTestTenantId1, request2.getName().toString());
    ASSERT(!streamProcessorExists(streamManager.get(), request2.getName().toString()));
}

TEST_F(StreamManagerTest, TestOnlyInsert) {
    const std::string streamName = "name1";

    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData(kTestTenantId1));
    request.setName(StringData(streamName));
    request.setProcessorId(StringData(streamName));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("id" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request.setOptions(mongo::StartOptions{});

    // Create a stream processor, but don't start the executor loop.

    size_t objSize = BSON("id" << 1).objsize();
    createStreamProcessor(streamManager.get(), request, objSize);

    auto insertThread = stdx::thread([&]() {
        insert(streamManager.get(), streamName, {BSON("id" << 1)});

        // This next insert should block.
        insert(streamManager.get(), streamName, {BSON("id" << 2)});
    });

    while (getTestOnlyDocs(streamManager.get(), streamName).getStats().queueDepth == 0) {
    }

    GetStatsCommand getStatsRequest;
    getStatsRequest.setTenantId(StringData(kTestTenantId1));
    getStatsRequest.setName(StringData(streamName));
    getStatsRequest.setScale(1);
    getStatsRequest.setVerbose(true);

    // Ensure that the blocked `testOnlyInsert` doesn't hold the executor lock by calling
    // `getStats`, which acquires the executor lock.
    for (int i = 0; i < 10; ++i) {
        stdx::this_thread::sleep_for(stdx::chrono::seconds(1));
        (void)streamManager->getStats(getStatsRequest);
        ASSERT_EQUALS(objSize,
                      getTestOnlyDocs(streamManager.get(), streamName).getStats().queueDepth);
    }

    runOnce(streamManager.get(), streamName);
    insertThread.join();
}

TEST_F(StreamManagerTest, CheckpointInterval) {
    auto innerTest = [this](std::string pipelineBson, int64_t expectedIntervalMs) {
        auto writeDir =
            fmt::format("/tmp/stream_manager_test/checkpointwritedir/{}", UUID::gen().toString());
        std::filesystem::create_directories(writeDir);
        auto streamManager =
            std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
        StartStreamProcessorCommand request;
        request.setTenantId(StringData(kTestTenantId1));
        request.setName(StringData("name1"));
        request.setProcessorId(StringData("name1"));
        StartOptions startOptions;
        CheckpointOptions checkpointOptions;
        LocalDiskStorageOptions localDiskOptions{writeDir};
        checkpointOptions.setLocalDisk(localDiskOptions);
        startOptions.setCheckpointOptions(checkpointOptions);
        request.setOptions(startOptions);
        request.setConnections({mongo::Connection("testKafka",
                                                  mongo::ConnectionTypeEnum::Kafka,
                                                  BSON("bootstrapServers"
                                                       << "localhost:9092"
                                                       << "isTestKafka" << true))});
        const auto inputBson = fromjson("{pipeline: " + pipelineBson + "}");
        request.setPipeline(parsePipelineFromBSON(inputBson["pipeline"]));
        streamManager->startStreamProcessor(request);
        ASSERT(streamProcessorExists(streamManager.get(), request.getName().toString()));

        auto processorInfo = getStreamProcessorInfo(streamManager.get(), "name1");
        ASSERT(processorInfo->checkpointCoordinator);
        ASSERT_EQ(stdx::chrono::milliseconds{expectedIntervalMs},
                  processorInfo->checkpointCoordinator->getCheckpointInterval());

        mongo::BSONObj featureFlags =
            mongo::fromjson("{ checkpointDuration: { streamProcessors: {name1: 50000}}}");
        std::shared_ptr<TenantFeatureFlags> tFeatureFlags = std::make_shared<TenantFeatureFlags>();
        tFeatureFlags->updateFeatureFlags(featureFlags);
        updateContextFeatureFlags(processorInfo, tFeatureFlags);
        ASSERT_EQ(stdx::chrono::milliseconds{50000}, getCheckpointInterval(processorInfo));

        featureFlags =
            mongo::fromjson("{ checkpointDuration: { streamProcessors: {name1: \"60000\"}}}");
        tFeatureFlags = std::make_shared<TenantFeatureFlags>();
        tFeatureFlags->updateFeatureFlags(featureFlags);
        updateContextFeatureFlags(processorInfo, tFeatureFlags);
        ASSERT_EQ(stdx::chrono::milliseconds{50000}, getCheckpointInterval(processorInfo));

        stopStreamProcessor(streamManager.get(), kTestTenantId1, request.getName().toString());
        ASSERT(!streamProcessorExists(streamManager.get(), request.getName().toString()));
        std::filesystem::remove_all(writeDir);
    };

    innerTest(R"(
        [
            {
                $source: {
                    connectionName: "testKafka",
                    topic: "t1",
                    testOnlyPartitionCount: 1
                }
            },
            {
                $tumblingWindow: {
                    interval: {size: 1, unit: "second"},
                    pipeline: [{$group: { _id: null}}]
                }
            },
            {
                $emit: {
                    connectionName: "__testLog"
                }
            }
        ]
    )",
              60 * 1000 * 60);

    innerTest(R"(
        [
            {
                $source: {
                    connectionName: "testKafka",
                    topic: "t1",
                    testOnlyPartitionCount: 1
                }
            },
            {
                $tumblingWindow: {
                    interval: {size: 1, unit: "second"},
                    pipeline: [{$group: { _id: null}}]
                }
            },
            {
                $emit: {
                    connectionName: "__testLog"
                }
            }
        ]
    )",
              60 * 1000 * 60);

    innerTest(R"(
        [
            {
                $source: {
                    connectionName: "testKafka",
                    topic: "t1",
                    testOnlyPartitionCount: 1
                }
            },
            {
                $emit: {
                    connectionName: "__testLog"
                }
            }
        ]
    )",
              5 * 1000 * 60);
}

TEST_F(StreamManagerTest, MemoryTracking) {
    std::string pipelineRaw = R"([
        {
            $source: {
                connectionName: "__testMemory",
                timeField: { $dateFromString: { dateString: "$ts" } }
            }
        },
        {
            $tumblingWindow: {
                interval: { size: 1, unit: "second" },
                allowedLateness: { size: 5, unit: "second" },
                pipeline: [
                    {
                        $group: {
                            _id: "$id",
                            value: { $sum: "$value" }
                        }
                    }
                ]
            }
        },
        {
            $emit: {
                connectionName: "__noopSink"
            }
        }
    ])";
    auto pipelineBson = fromjson("{pipeline: " + pipelineRaw + "}");
    ASSERT_EQUALS(pipelineBson["pipeline"].type(), BSONType::Array);
    auto pipeline = pipelineBson["pipeline"];

    std::string sp1 = "sp1";
    std::string sp2 = "sp2";
    std::string sp3 = "sp3";

    StreamManager::Options options{.memoryLimitBytes = 2 * kMemoryUsageBatchSize};
    auto streamManager = std::make_unique<StreamManager>(getServiceContext(), std::move(options));
    StartStreamProcessorCommand request1;
    request1.setTenantId(StringData(kTestTenantId1));
    request1.setName(StringData(sp1));
    request1.setProcessorId(StringData(sp1));
    request1.setPipeline(parsePipelineFromBSON(pipeline));
    request1.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request1.setOptions(mongo::StartOptions{});
    createStreamProcessor(streamManager.get(), request1);

    StartStreamProcessorCommand request2;
    request2.setTenantId(StringData(kTestTenantId1));
    request2.setName(StringData(sp2));
    request2.setProcessorId(StringData(sp2));
    request2.setPipeline(parsePipelineFromBSON(pipeline));
    request2.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request2.setOptions(mongo::StartOptions{});
    createStreamProcessor(streamManager.get(), request2);

    StartStreamProcessorCommand request3;
    request3.setTenantId(StringData(kTestTenantId1));
    request3.setName(StringData(sp3));
    request3.setProcessorId(StringData(sp3));
    request3.setPipeline(parsePipelineFromBSON(pipeline));
    request3.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    request3.setOptions(mongo::StartOptions{});
    createStreamProcessor(streamManager.get(), request3);

    auto* memoryAggregator = getMemoryAggregator(streamManager.get());

    // Insert documents into sp1
    insert(streamManager.get(),
           sp1,
           {
               BSON("id" << 1 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:00.000Z"),
               BSON("id" << 2 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:00.000Z"),
           });

    runOnce(streamManager.get(), sp1);
    checkSPMemoryUsage(streamManager.get(), sp1, /* expectedMemoryUsage */ 288);
    checkSPMemoryUsage(streamManager.get(), sp2, /* expectedMemoryUsage */ 0);
    ASSERT_EQUALS(kMemoryUsageBatchSize, memoryAggregator->getCurrentMemoryUsageBytes());

    // Insert documents into sp2
    insert(streamManager.get(),
           sp2,
           {
               BSON("id" << 1 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:00.000Z"),
           });

    runOnce(streamManager.get(), sp2);
    checkSPMemoryUsage(streamManager.get(), sp1, /* expectedMemoryUsage */ 288);
    checkSPMemoryUsage(streamManager.get(), sp2, /* expectedMemoryUsage */ 144);
    ASSERT_EQUALS(2 * kMemoryUsageBatchSize, memoryAggregator->getCurrentMemoryUsageBytes());

    // Add a new key to get $group state for sp1
    insert(streamManager.get(),
           sp1,
           {
               BSON("id" << 3 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:00.000Z"),
           });

    runOnce(streamManager.get(), sp1);
    checkSPMemoryUsage(streamManager.get(), sp1, /* expectedMemoryUsage */ 432);
    checkSPMemoryUsage(streamManager.get(), sp2, /* expectedMemoryUsage */ 144);
    ASSERT_EQUALS(2 * kMemoryUsageBatchSize, memoryAggregator->getCurrentMemoryUsageBytes());

    // Add a new key to get $group state for sp2
    insert(streamManager.get(),
           sp2,
           {
               BSON("id" << 3 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:00.000Z"),
           });

    runOnce(streamManager.get(), sp2);
    checkSPMemoryUsage(streamManager.get(), sp1, /* expectedMemoryUsage */ 432);
    checkSPMemoryUsage(streamManager.get(), sp2, /* expectedMemoryUsage */ 288);
    ASSERT_EQUALS(2 * kMemoryUsageBatchSize, memoryAggregator->getCurrentMemoryUsageBytes());

    // Open a new window for sp1
    insert(streamManager.get(),
           sp1,
           {
               BSON("id" << 1 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:01.000Z"),
           });

    runOnce(streamManager.get(), sp1);
    checkSPMemoryUsage(streamManager.get(), sp1, /* expectedMemoryUsage */ 576);
    checkSPMemoryUsage(streamManager.get(), sp2, /* expectedMemoryUsage */ 288);
    ASSERT_EQUALS(2 * kMemoryUsageBatchSize, memoryAggregator->getCurrentMemoryUsageBytes());

    // Close the first window while opening a third window for sp1
    insert(streamManager.get(),
           sp1,
           {
               BSON("id" << 1 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:07.000Z"),
               BSON("id" << 2 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:07.000Z"),
           });
    runOnce(streamManager.get(), sp1);

    // The first window that closed had 3 $group keys, so the memory should go down by
    // a total of 48 bytes, and the previous insert opened a new window with two new keys,
    // so that added a total of 32 bytes -- (64 - 48 + 32) = 48
    checkSPMemoryUsage(streamManager.get(), sp1, /* expectedMemoryUsage */ 432);
    checkSPMemoryUsage(streamManager.get(), sp2, /* expectedMemoryUsage */ 288);
    ASSERT_EQUALS(2 * kMemoryUsageBatchSize, memoryAggregator->getCurrentMemoryUsageBytes());

    // Insert documents into sp3 for the first time which should cause the approximate memory
    // usage to exceed the limit, which should kill all the stream processors.
    insert(streamManager.get(),
           sp3,
           {
               BSON("id" << 1 << "value" << 1 << "ts"
                         << "2023-01-01T00:00:00.000Z"),
           });
    ASSERT_THROWS_WHAT(
        runOnce(streamManager.get(), sp3), SPException, "stream processing instance out of memory");
    ASSERT_THROWS_WHAT(
        runOnce(streamManager.get(), sp2), SPException, "stream processing instance out of memory");
    ASSERT_THROWS_WHAT(
        runOnce(streamManager.get(), sp1), SPException, "stream processing instance out of memory");
}

TEST_F(StreamManagerTest, SingleTenancy) {
    std::string pipelineRaw = R"([
        { $source: { connectionName: "__testMemory" } },
        { $emit: { connectionName: "__noopSink" } }
    ])";
    auto pipelineBson = fromjson("{pipeline: " + pipelineRaw + "}");
    ASSERT_EQUALS(pipelineBson["pipeline"].type(), BSONType::Array);
    auto pipeline = pipelineBson["pipeline"];

    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options());

    std::string sp1{"sp1"};
    std::string sp2{"sp2"};
    std::string sp3{"sp3"};

    auto makeRequest = [&](std::string tenantId, std::string spID) {
        auto id = UUID::gen().toString();
        StartStreamProcessorCommand request;
        request.setTenantId(StringData(tenantId));
        request.setName(StringData(spID));
        request.setProcessorId(StringData(spID));
        request.setPipeline(parsePipelineFromBSON(pipeline));
        request.setConnections({mongo::Connection(
            "__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
        request.setOptions(mongo::StartOptions{});
        return request;
    };

    streamManager->startStreamProcessor(makeRequest(kTestTenantId1, sp1));
    streamManager->startStreamProcessor(makeRequest(kTestTenantId1, sp2));

    // Creating another stream processor with a different tenant ID should throw an
    // invariant exception.
    ASSERT_THROWS_CODE(streamManager->startStreamProcessor(makeRequest(kTestTenantId2, sp3)),
                       AssertionException,
                       ErrorCodes::InternalError);

    stopStreamProcessor(streamManager.get(), kTestTenantId1, sp2);
    stopStreamProcessor(streamManager.get(), kTestTenantId1, sp1);
}

TEST_F(StreamManagerTest, MultiTenancy) {
    std::string pipelineRaw = R"([
        { $source: { connectionName: "__testMemory" } },
        { $emit: { connectionName: "__noopSink" } }
    ])";
    auto pipelineBson = fromjson("{pipeline: " + pipelineRaw + "}");
    ASSERT_EQUALS(pipelineBson["pipeline"].type(), BSONType::Array);
    auto pipeline = pipelineBson["pipeline"];

    mongo::streams::gStreamsAllowMultiTenancy = true;
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});

    std::string sp1{"sp1"};
    std::string sp2{"sp2"};

    auto makeRequest = [&](std::string tenantId, std::string spID) {
        auto id = UUID::gen().toString();
        StartStreamProcessorCommand request;
        request.setTenantId(StringData(tenantId));
        request.setName(StringData(spID));
        request.setProcessorId(StringData(spID));
        request.setPipeline(parsePipelineFromBSON(pipeline));
        request.setConnections({mongo::Connection(
            "__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
        request.setOptions(mongo::StartOptions{});
        return request;
    };

    // Should allow scheduling stream processors from two different tenants when multi-tenancy
    // is enabled.
    streamManager->startStreamProcessor(makeRequest(kTestTenantId1, sp1));
    streamManager->startStreamProcessor(makeRequest(kTestTenantId2, sp2));
    stopStreamProcessor(streamManager.get(), kTestTenantId1, sp1);
    stopStreamProcessor(streamManager.get(), kTestTenantId2, sp2);
    mongo::streams::gStreamsAllowMultiTenancy = false;
}

}  // namespace streams
