/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#pragma once

#include <functional>
#include <memory>

#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/util/chunked_memory_aggregator.h"

namespace mongo {

/**
 * Memory aggregator where memory updates can be propagated to concurrently. This creates child
 * `ChunkedMemoryAggregator` nodes which propagate updates to this `ConcurrentMemoryAggregator`
 * node in chunks, which will always be an over count and never an under count.
 */
class ConcurrentMemoryAggregator {
public:
    /**
     * Optional user provided callback that will be invoked when memory usage increases.
     */
    class UsageMonitor {
    public:
        virtual ~UsageMonitor() {}

        /**
         * Invoked when the memory usage increases.
         *
         * `memoryUsageBytes` - Current overall memory usage reported from the concurrent memory
         * aggregator instance. `sourceId` - The chunked memory aggregator ID that triggered this
         * increase in memory. `memoryAggregator` - The concurrent memory aggregator instance.
         */
        virtual void onMemoryUsageIncreased(int64_t memoryUsageBytes,
                                            int64_t sourceId,
                                            const ConcurrentMemoryAggregator* memoryAggregator) = 0;
    };  // class UsageMonitor

    ConcurrentMemoryAggregator(std::shared_ptr<UsageMonitor> usageMonitor = nullptr);

    ~ConcurrentMemoryAggregator();

    /**
     * Creates a new chunked memory aggregator. A chunked memory aggregator can be used to allocate
     * `MemoryUsageHandle` instances which are used to report memory usage. Updates to the
     * `ChunkedMemoryAggregator` are thread-safe.
     */
    std::shared_ptr<ChunkedMemoryAggregator> createChunkedMemoryAggregator(
        ChunkedMemoryAggregator::Options options);

    /**
     * Returns the current memory usage across all chunked memory aggregator. This is meant to be an
     * approximation and not an exact number because the chunked memory aggregators only send
     * updates if there is a significant memory change.
     */
    int64_t getCurrentMemoryUsageBytes() const;

    /**
     * Invokes the callback for each active `ChunkedMemoryAggregator` under this
     * `ConcurrentMemoryAggregator`. This acquires the `ConcurrentMemoryAggregator` `_mutex` for the
     * entire duration.
     */
    void visitAll(
        std::function<void(std::shared_ptr<const ChunkedMemoryAggregator>)> callback) const;

private:
    friend class ChunkedMemoryAggregator;
    friend class ChunkedMemoryAggregatorDeleter;
    friend class ConcurrentMemoryAggregatorTest;

    /**
     * Atomically updates the overall memory usage. If the memory usage went up, a notification
     * will be sent to the usage monitor that was supplied, letting it know what the current
     * memory usage is and which chunked memory aggregator just triggered this update.
     */
    void add(ChunkedMemoryAggregator* sourceAggregator, int64_t delta);

    /**
     * Invokes the `_usageMonitor` callback, if present.
     */
    void poll(ChunkedMemoryAggregator* sourceAggregator) const;

    /**
     * Removes the input chunked memory aggregator. This will be called automatically when the
     * `ChunkedMemoryAggregator` is destroyed through `ChunkedMemoryAggregatorDeleter`.
     */
    void remove(const ChunkedMemoryAggregator* aggregator);

    // Custom user supplied callback that gets notified when the memory usage increases.
    std::shared_ptr<UsageMonitor> _usageMonitor;

    // Overall memory usage based on deltas reported by the individual chunked memory aggregators.
    AtomicWord<int64_t> _curMemoryUsageBytes;

    // Mutex that protects access to `_chunkedMemoryAggregators`.
    mutable stdx::mutex _mutex;

    // Map (key is the ID) of currently active chunked memory aggregators. This gets automatically
    // cleaned up when `ChunkedMemoryAggregator`s go out of scope.
    stdx::unordered_map<int64_t, std::weak_ptr<ChunkedMemoryAggregator>> _chunkedMemoryAggregators;

    // Auto incrementing ID generator that is used to assign unique IDs to `ChunkedMemoryAggregator`
    // instances that are created from this `ConcurrentMemoryAggregator`
    int64_t _chunkedMemoryAggregatorIDCounter{0};
};  // class ConcurrentMemoryAggregator

};  // namespace mongo
