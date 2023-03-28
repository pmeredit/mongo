/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/operation_context.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"

#include "streams/exec/constants.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/management/stream_manager.h"

namespace streams {
namespace {

using namespace mongo;

using StreamManagerTest = AggregationContextFixture;

TEST_F(StreamManagerTest, SmokeTest1) {
    StreamManager& streamManager = StreamManager::get();

    std::string name("name1");
    streamManager.startStreamProcessor(name,
                                       {TestUtils::getTestSourceSpec(),
                                        BSON("$match" << BSON("a" << 1)),
                                        TestUtils::getTestLogSinkSpec()},
                                       {});

    auto dag = std::move(streamManager.getStreamProcessorInfo(name));
    ASSERT_EQ(3, dag->operators().size());
}

}  // namespace
}  // namespace streams
