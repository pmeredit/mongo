/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"

#include "./stream_manager.h"

namespace mongo {
namespace {

TEST(StreamManagerTest, SmokeTest1) {
    StreamManager& streamManager = StreamManager::get();
    std::vector<BSONObj> pipeline{BSON("$foo"
                                       << "bar")};
    streamManager.startStreamProcessor(pipeline);
}

}  // namespace
}  // namespace mongo
