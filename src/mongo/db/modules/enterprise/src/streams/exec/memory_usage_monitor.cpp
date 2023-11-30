/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/memory_usage_monitor.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "streams/util/exception.h"

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
        throw SPException(Status(ErrorCodes::Error::ExceededMemoryLimit,
                                 "stream processing instance out of memory"));
    }
}

};  // namespace streams
