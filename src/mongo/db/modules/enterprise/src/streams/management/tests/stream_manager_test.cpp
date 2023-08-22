/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/operation_context.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/constants.h"
#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
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

    StreamManager::StreamProcessorInfo* getStreamProcessorInfo(StreamManager* streamManager,
                                                               const std::string& name) {
        return streamManager->_processors.at(name).get();
    }

    void pruneStreamProcessors(StreamManager* streamManager) {
        streamManager->pruneStreamProcessors();
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
    request.setConnections({});
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
    request.setConnections({});
    streamManager->startStreamProcessor(request);

    insert(streamManager.get(),
           streamName,
           {
               BSON("id" << 1),
               BSON("id" << 2),
           });

    // Run executor loop twice so that the stat snapshots acquired on the second
    // run are the final stats from the first run.
    runOnce(streamManager.get(), streamName);
    runOnce(streamManager.get(), streamName);

    auto statsReply = streamManager->getStats(streamName, /*scale*/ 1, /* verbose */ true);
    ASSERT_EQUALS(streamName, statsReply.getName());
    ASSERT_EQUALS(StreamStatusEnum::Running, statsReply.getStatus());
    ASSERT_EQUALS(1, statsReply.getScaleFactor());
    ASSERT_EQUALS(2, statsReply.getInputDocs());
    ASSERT_EQUALS(250, statsReply.getInputBytes());
    ASSERT_EQUALS(1, statsReply.getOutputDocs());
    ASSERT_EQUALS(253, statsReply.getOutputBytes());

    // Ensure that the operator stats are returned in verbose mode.
    // Three operators - Source, match, and sink operator.
    auto operatorStatsWrap = statsReply.getOperatorStats();
    ASSERT_TRUE(operatorStatsWrap);

    auto operatorStats = operatorStatsWrap.get();
    ASSERT_EQUALS(3, operatorStats.size());

    ASSERT_EQUALS("InMemorySourceOperator", operatorStats[0].getName());
    ASSERT_EQUALS(statsReply.getInputDocs(), operatorStats[0].getInputDocs());
    ASSERT_EQUALS(statsReply.getInputBytes(), operatorStats[0].getInputBytes());
    ASSERT_EQUALS(2, operatorStats[0].getOutputDocs());

    ASSERT_EQUALS("MatchOperator", operatorStats[1].getName());
    ASSERT_EQUALS(operatorStats[0].getOutputDocs(), operatorStats[1].getInputDocs());
    ASSERT_EQUALS(1, operatorStats[1].getOutputDocs());

    ASSERT_EQUALS("LogSinkOperator", operatorStats[2].getName());
    ASSERT_EQUALS(statsReply.getOutputDocs(), operatorStats[2].getInputDocs());
    ASSERT_EQUALS(statsReply.getOutputBytes(), operatorStats[2].getInputBytes());

    streamManager->stopStreamProcessor(streamName);
}

TEST_F(StreamManagerTest, List) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    StartStreamProcessorCommand request;
    request.setTenantId(StringData("tenant1"));
    request.setName(StringData("name1"));
    request.setProcessorId(StringData("processor1"));
    request.setPipeline(
        {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()});
    request.setConnections({});
    streamManager->startStreamProcessor(request);
    request.setName(StringData("name2"));
    request.setProcessorId(StringData("processor2"));
    streamManager->startStreamProcessor(request);
    auto listReply = streamManager->listStreamProcessors();
    ASSERT_EQUALS(2, listReply.getStreamProcessors().size());

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
    request.setConnections({});
    streamManager->startStreamProcessor(request);
    ASSERT(exists(streamManager.get(), "name1"));

    // Inject an exception into the executor.
    auto processorInfo = getStreamProcessorInfo(streamManager.get(), "name1");
    processorInfo->executor->testOnlyInjectException(
        std::make_exception_ptr(std::runtime_error("hello exception")));

    // Verify that the exception causes the StreamManager to stop the stream processor.
    pruneStreamProcessors(streamManager.get());
    while (exists(streamManager.get(), "name1")) {
        stdx::this_thread::sleep_for(stdx::chrono::seconds(1));
        pruneStreamProcessors(streamManager.get());
    }
}

}  // namespace streams
