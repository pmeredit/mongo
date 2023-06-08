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
    auto statsReply = streamManager->getStats("name1", /*scale*/ 1);
    ASSERT_EQUALS("name1", statsReply.getName());
    ASSERT_EQUALS(StreamStatusEnum::Running, statsReply.getStatus());
    streamManager->stopStreamProcessor("name1");
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
