/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "streams/management/stream_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace streams {

using namespace mongo;

StreamManager& StreamManager::get() {
    static auto streamManager = new StreamManager();
    return *streamManager;
}

void StreamManager::startStreamProcessor(const std::vector<BSONObj>& pipeline) {
    LOGV2_DEBUG(999999, 1, "startStreamProcessor");
}

}  // namespace streams
