/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/operation_context.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/connection_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/management/stream_manager.h"

namespace streams {

using namespace mongo;

class StreamManagerTest : public AggregationContextFixture {
protected:
    void validateStreamProcessorInfo(StreamManager& streamManager,
                                     std::string name,
                                     int expectedNumOperators) {
        auto info = std::move(streamManager.getStreamProcessorInfo(name));
        ASSERT_EQ(expectedNumOperators, info.operatorDag->operators().size());
        info.executor->stop();
    }
};

TEST_F(StreamManagerTest, SmokeTest1) {
    StreamManager& streamManager = StreamManager::get();
    std::string name("name1");
    streamManager.startStreamProcessor(
        name, {getTestSourceSpec(), BSON("$match" << BSON("a" << 1)), getTestLogSinkSpec()}, {});
    validateStreamProcessorInfo(streamManager, name, 3);
}

}  // namespace streams
