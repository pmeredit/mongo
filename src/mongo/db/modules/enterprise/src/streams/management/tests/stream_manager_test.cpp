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
};

TEST_F(StreamManagerTest, Start) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    std::string name("name1");
    streamManager->startStreamProcessor(
        name, {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()}, {});
    ASSERT(exists(streamManager.get(), name));
    streamManager->stopStreamProcessor(name);
    ASSERT(!exists(streamManager.get(), name));
}

TEST_F(StreamManagerTest, GetStats) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    streamManager->startStreamProcessor(
        "name1", {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()}, {});
    auto statsReply = streamManager->getStats("name1", /*scale*/ 1);
    ASSERT_EQUALS("name1", statsReply.getName());
    ASSERT_EQUALS(StreamStatusEnum::Running, statsReply.getStatus());
    streamManager->stopStreamProcessor("name1");
}

TEST_F(StreamManagerTest, List) {
    auto streamManager =
        std::make_unique<StreamManager>(getServiceContext(), StreamManager::Options{});
    streamManager->startStreamProcessor(
        "name1", {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()}, {});
    streamManager->startStreamProcessor(
        "name2", {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()}, {});
    auto listReply = streamManager->listStreamProcessors();
    ASSERT_EQUALS(2, listReply.getStreamProcessors().size());

    streamManager->stopStreamProcessor("name1");
    streamManager->stopStreamProcessor("name2");
    listReply = streamManager->listStreamProcessors();
    ASSERT_EQUALS(0, listReply.getStreamProcessors().size());
}

}  // namespace streams
