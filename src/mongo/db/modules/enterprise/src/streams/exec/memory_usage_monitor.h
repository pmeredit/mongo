/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/platform/atomic_word.h"
#include "mongo/util/concurrent_memory_aggregator.h"

namespace streams {

// Responsible for enforcing the memory limit. This is fed into the `ConcurrentMemoryAggregator`,
// which will invoke `onMemoryUsageIncreased` whenever there is a significant change in memory. The
// heuristic for this class is to kill all stream processors on this process once the memory limit
// has been exceeded.
class KillAllMemoryUsageMonitor : public mongo::ConcurrentMemoryAggregator::UsageMonitor {
public:
    KillAllMemoryUsageMonitor(int64_t maxAllowedMemoryUsageBytes);

    void onMemoryUsageIncreased(int64_t memoryUsageBytes,
                                int64_t memoryAggregatorId,
                                const mongo::ConcurrentMemoryAggregator* memoryAggregator) override;

    // Resets the `_exceededMemoryLimit` signal, allowing for new stream processors to be scheduled
    // on this process.
    void reset();

    // Whether or not the memory limit has been exceeded across all stream processors running on
    // this process.
    bool hasExceededMemoryLimit() const {
        return _exceededMemoryLimit.load();
    }

private:
    // Max amount of memory that is allowed to be used across all stream processors. Once
    // this limit is exceeded, all the stream processors on this process will be killed.
    int64_t _maxAllowedMemoryUsageBytes{0};

    // Whether or not the memory limit has been exceeded. When the memory limit has been exceeded,
    // all stream processors should be killed.
    mongo::AtomicWord<bool> _exceededMemoryLimit;
};  // class KillAllMemoryUsageMonitor

};  // namespace streams
