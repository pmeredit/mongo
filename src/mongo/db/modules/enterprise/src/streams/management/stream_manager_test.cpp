/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"

#include "./stream_manager.h"

namespace streams {
namespace {

using namespace mongo;

TEST(StreamManagerTest, SmokeTest1) {
    StreamManager& streamManager = StreamManager::get();
    std::vector<BSONObj> pipeline{BSON("$foo"
                                       << "bar")};
    streamManager.startStreamProcessor(pipeline);
}

}  // namespace
}  // namespace streams
