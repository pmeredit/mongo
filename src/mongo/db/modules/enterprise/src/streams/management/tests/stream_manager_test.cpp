/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/operation_context.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/management/stream_manager.h"

namespace streams {

using namespace mongo;

class StreamManagerTest : public AggregationContextFixture {
public:
    bool exists(StreamManager* streamManager, std::string name) {
        return streamManager->_processors.contains(name);
    }

    void createStreamProcessor(StreamManager* streamManager,
                               const std::string& streamName,
                               StartStreamProcessorCommand request,
                               int64_t testOnlyDocsQueueMaxSizeBytes) {
        auto info = streamManager->createStreamProcessorInfoLocked(request);
        auto executorOptions = info->executor->_options;
        executorOptions.testOnlyDocsQueueMaxSizeBytes = testOnlyDocsQueueMaxSizeBytes;
        info->executor =
            std::make_unique<Executor>(info->context.get(), std::move(executorOptions));
        streamManager->_processors.emplace(std::make_pair(streamName, std::move(info)));
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

    bool poll(std::function<bool()> func, Seconds timeout = Seconds{5000}) {
        auto deadline = Date_t::now() + timeout;
        while (Date_t::now() < deadline) {
            if (func()) {
                return true;
            }
        }
        return false;
    }
};

TEST_F(StreamManagerTest, Start) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData("tenant1"));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("processor1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    streamManager->startStreamProcessor(request);
    ASSERT(exists(streamManager.get(), "name1"));
    streamManager->stopStreamProcessor("name1");
    ASSERT(!exists(streamManager.get(), "name1"));
}

TEST_F(StreamManagerTest, GetStats) {
    const std::string streamName = "name1";

    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData("tenant1"));
    request.setName(StringData(streamName));
    request.setProcessorId(StringData("processor1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("id" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    streamManager->startStreamProcessor(request);

    insert(streamManager.get(),
           streamName,
           {
               BSON("id" << 1),
               BSON("id" << 2),
           });

    // Poll stats until the doc has made it to the output sink.
    auto statsReply = streamManager->getStats(streamName, /*scale*/ 1, /* verbose */ true);
    while (statsReply.getOutputMessageCount() < 1) {
        statsReply = streamManager->getStats(streamName, /*scale*/ 1, /* verbose */ true);
    }
    ASSERT_EQUALS(streamName, statsReply.getName());
    ASSERT_EQUALS(StreamStatusEnum::Running, statsReply.getStatus());
    ASSERT_EQUALS(1, statsReply.getScaleFactor());
    ASSERT_EQUALS(2, statsReply.getInputMessageCount());
    ASSERT_EQUALS(506, statsReply.getInputMessageSize());
    ASSERT_EQUALS(1, statsReply.getOutputMessageCount());
    ASSERT_EQUALS(253, statsReply.getOutputMessageSize());

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

    streamManager->stopStreamProcessor(streamName);
}

TEST_F(StreamManagerTest, List) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request1;
    request1.setTenantId(StringData("tenant1"));
    request1.setName(StringData("name1"));
    request1.setProcessorId(StringData("processor1"));
    request1.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request1.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    streamManager->startStreamProcessor(request1);

    StartStreamProcessorCommand request2;
    request2.setTenantId(StringData("tenant1"));
    request2.setName(StringData("name2"));
    request2.setProcessorId(StringData("processor2"));
    request2.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request2.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    streamManager->startStreamProcessor(request2);
    auto listReply = streamManager->listStreamProcessors();
    ASSERT_EQUALS(2, listReply.getStreamProcessors().size());

    auto& sps = listReply.getStreamProcessors();
    std::sort(sps.begin(), sps.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return lhs.getName().compare(rhs.getName()) < 0;
    });

    const auto& sp0 = sps[0];
    ASSERT_EQUALS(StringData("tenant1"), sp0.getTenantId());
    ASSERT_EQUALS(StringData("processor1"), sp0.getProcessorId());
    ASSERT_EQUALS(StringData("name1"), sp0.getName());

    const auto& sp1 = sps[1];
    ASSERT_EQUALS(StringData("tenant1"), sp1.getTenantId());
    ASSERT_EQUALS(StringData("processor2"), sp1.getProcessorId());
    ASSERT_EQUALS(StringData("name2"), sp1.getName());

    streamManager->stopStreamProcessor("name1");
    streamManager->stopStreamProcessor("name2");
    listReply = streamManager->listStreamProcessors();
    ASSERT_EQUALS(0, listReply.getStreamProcessors().size());
}

TEST_F(StreamManagerTest, ErrorHandling) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData("tenant1"));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("processor1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    streamManager->startStreamProcessor(request);
    ASSERT(exists(streamManager.get(), "name1"));

    // Inject an exception into the executor.
    auto processorInfo = getStreamProcessorInfo(streamManager.get(), "name1");
    processorInfo->executor->testOnlyInjectException(
        std::make_exception_ptr(std::runtime_error("hello exception")));

    // Verify that the exception causes the streamProcessor to enter an error status.
    auto success = poll([&]() {
        auto reply = streamManager->listStreamProcessors().getStreamProcessors();
        auto it = std::find_if(
            reply.begin(), reply.end(), [&](auto sp) { return sp.getName() == request.getName(); });
        ASSERT_NOT_EQUALS(it, reply.end());
        bool isError = it->getStatus() == StreamStatusEnum::Error;
        if (isError) {
            ASSERT(it->getError());
            ASSERT_EQUALS(75385, it->getError()->getCode());
            ASSERT_EQUALS("An internal error occured.", it->getError()->getReason());
        }
        return isError;
    });
    ASSERT(success);

    // Call stopStreamProcessor to remove the erroring streamProcessor from memory.
    ASSERT(exists(streamManager.get(), request.getName().toString()));
    streamManager->stopStreamProcessor(request.getName().toString());
    ASSERT(!exists(streamManager.get(), request.getName().toString()));
}

// Verifies that checkpointing is disabled for sources other than Kafka and Changestream.
TEST_F(StreamManagerTest, DisableCheckpoint) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});

    StartStreamProcessorCommand request;
    request.setTenantId(StringData("tenant1"));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("processor1"));
    request.setPipeline({getTestSourceSpec(), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});
    streamManager->startStreamProcessor(request);
    ASSERT(exists(streamManager.get(), "name1"));
    auto processorInfo = getStreamProcessorInfo(streamManager.get(), "name1");
    // Verify checkpointing is disabled.
    ASSERT(!processorInfo->context->checkpointStorage.get());
    streamManager->stopStreamProcessor(request.getName().toString());
    ASSERT(!exists(streamManager.get(), request.getName().toString()));

    StartStreamProcessorCommand request2;
    request2.setTenantId(StringData("tenant1"));
    request2.setName(StringData("name2"));
    request2.setProcessorId(StringData("processor2"));
    request2.setPipeline({BSON(kSourceStageName << BSON("connectionName"
                                                        << "sample_data_solar")),
                          getTestLogSinkSpec()});
    request2.setConnections({mongo::Connection(
        "sample_data_solar", mongo::ConnectionTypeEnum::SampleSolar, mongo::BSONObj())});
    streamManager->startStreamProcessor(request2);
    ASSERT(exists(streamManager.get(), "name2"));
    auto processorInfo2 =
        getStreamProcessorInfo(streamManager.get(), request2.getName().toString());
    // Verify checkpointing is disabled.
    ASSERT(!processorInfo2->context->checkpointStorage.get());
    streamManager->stopStreamProcessor(request2.getName().toString());
    ASSERT(!exists(streamManager.get(), request2.getName().toString()));
}

TEST_F(StreamManagerTest, TestOnlyInsert) {
    const std::string streamName = "name1";

    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData("tenant1"));
    request.setName(StringData(streamName));
    request.setProcessorId(StringData("processor1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("id" << 1)), getTestLogSinkSpec()});
    request.setConnections(
        {mongo::Connection("__testMemory", mongo::ConnectionTypeEnum::InMemory, mongo::BSONObj())});

    // Create a stream processor, but don't start the executor loop.

    size_t objSize = BSON("id" << 1).objsize();
    createStreamProcessor(streamManager.get(), streamName, request, objSize);

    auto insertThread = stdx::thread([&]() {
        insert(streamManager.get(), streamName, {BSON("id" << 1)});

        // This next insert should block.
        insert(streamManager.get(), streamName, {BSON("id" << 2)});
    });

    while (getTestOnlyDocs(streamManager.get(), streamName).getStats().queueDepth == 0) {
    }

    // Ensure that the blocked `testOnlyInsert` doesn't hold the executor lock by calling
    // `getStats`, which acquires the executor lock.
    for (int i = 0; i < 10; ++i) {
        stdx::this_thread::sleep_for(stdx::chrono::seconds(1));
        (void)streamManager->getStats(streamName, /*scale*/ 1, /*verbose*/ true);
        ASSERT_EQUALS(objSize,
                      getTestOnlyDocs(streamManager.get(), streamName).getStats().queueDepth);
    }

    runOnce(streamManager.get(), streamName);
    insertThread.join();
}

}  // namespace streams
