/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/util/chunked_memory_aggregator.h"

#include "mongo/util/assert_util.h"
#include "streams/util/concurrent_memory_aggregator.h"

namespace mongo {

ChunkedMemoryAggregator::ChunkedMemoryAggregator(ChunkedMemoryAggregator::PrivateTag,
                                                 ChunkedMemoryAggregator::Options options,
                                                 int64_t id,
                                                 ConcurrentMemoryAggregator* parent)
    : _options(std::move(options)), _id(id), _parent(parent) {}

ChunkedMemoryAggregator::~ChunkedMemoryAggregator() {
    // Ensure that all `MemoryUsageHandle` instances have gone out of scope
    // and freed their memory before the `ChunkedMemoryAggregator` goes out of scope.
    dassert(_curMemoryUsageBytes.load() == 0);
}

MemoryUsageHandle ChunkedMemoryAggregator::createUsageHandle(int64_t initialBytes) {
    return MemoryUsageHandle(initialBytes, this);
}

int64_t ChunkedMemoryAggregator::getCurrentMemoryUsageBytes() const {
    return _curMemoryUsageBytes.load();
}

int64_t ChunkedMemoryAggregator::getId() const {
    return _id;
}

void ChunkedMemoryAggregator::poll() {
    _parent->poll(this);
}

void ChunkedMemoryAggregator::add(int64_t delta) {
    int64_t oldValue = _curMemoryUsageBytes.fetchAndAdd(delta);
    int64_t newValue = oldValue + delta;
    int64_t update = computeUpstreamUpdate(oldValue, newValue);
    dassert(newValue >= 0);

    // Only send an update if we've accumulated a significant amount of memory
    // that's worth notifying about.
    if (update != 0) {
        _parent->add(this, update);
    }
}

int64_t ChunkedMemoryAggregator::computeUpstreamUpdate(int64_t oldValue, int64_t newValue) const {
    // Computes the delta update that should be propagated to the upstream memory aggregator. This
    // returns zero if no update should be sent.
    //
    // This is computed by rounding the old memory usage and the new memory usage values up to the
    // next `memoryUsageUpdateBatchSize` multiple. If the old and new memory usage values round up
    // to different boundaries, then that difference will be sent as the update to the upstream
    // memory aggregator, which will always be a multiple of `memoryUsageUpdateBatchSize` since this
    // only sends updates in chunks/batches.
    int64_t newValueBoundary = (newValue + (_options.memoryUsageUpdateBatchSize - 1)) /
        _options.memoryUsageUpdateBatchSize;
    int64_t oldValueBoundary = (oldValue + (_options.memoryUsageUpdateBatchSize - 1)) /
        _options.memoryUsageUpdateBatchSize;
    return (newValueBoundary - oldValueBoundary) * _options.memoryUsageUpdateBatchSize;
}

};  // namespace mongo
