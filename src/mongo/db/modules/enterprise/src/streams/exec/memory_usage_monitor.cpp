/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/memory_usage_monitor.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/logv2/log.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

KillAllMemoryUsageMonitor::KillAllMemoryUsageMonitor(int64_t maxAllowedMemoryUsageBytes)
    : _maxAllowedMemoryUsageBytes(maxAllowedMemoryUsageBytes) {}

void KillAllMemoryUsageMonitor::onMemoryUsageIncreased(
    int64_t memoryUsageBytes,
    int64_t memoryAggregatorId,
    const ConcurrentMemoryAggregator* memoryAggregator) {
    if (memoryUsageBytes > _maxAllowedMemoryUsageBytes) {
        _exceededMemoryLimit.store(true);
    }

    if (_exceededMemoryLimit.load()) {
        LOGV2_WARNING(
            8400500,
            "MemoryUsageMonitor killing stream processor because memory limit has been exceeded");
        throw SPException(Status(ErrorCodes::Error::ExceededMemoryLimit, "Worker out of memory"));
    }
}

void KillAllMemoryUsageMonitor::reset() {
    if (_exceededMemoryLimit.load()) {
        LOGV2_INFO(8400501, "MemoryUsageMonitor resetting 'exceeded memory limit' signal");
    }

    _exceededMemoryLimit.store(false);
}

};  // namespace streams
