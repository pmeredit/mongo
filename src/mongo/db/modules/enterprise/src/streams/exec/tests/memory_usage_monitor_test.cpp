/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/memory_usage_monitor.h"

#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "mongo/util/concurrent_memory_aggregator.h"
#include "streams/util/exception.h"

using namespace mongo;

namespace streams {
namespace {

static constexpr int64_t kMemoryUsageUpdateBatchSize = 32 * 1024 * 1024;  // 32MB

static const ChunkedMemoryAggregator::Options kChunkedMemoryAggregatorOptions{
    .memoryUsageUpdateBatchSize = kMemoryUsageUpdateBatchSize};

TEST(MemoryUsageMonitor, BelowMaxMemory) {
    auto monitor = std::make_unique<KillAllMemoryUsageMonitor>(kMemoryUsageUpdateBatchSize + 1);
    auto manager = std::make_unique<ConcurrentMemoryAggregator>(std::move(monitor));
    auto local = manager->createChunkedMemoryAggregator(kChunkedMemoryAggregatorOptions);
    auto handle = local->createUsageHandle();

    ASSERT_DOES_NOT_THROW(handle.set(kMemoryUsageUpdateBatchSize));
    ASSERT_EQUALS(kMemoryUsageUpdateBatchSize, manager->getCurrentMemoryUsageBytes());
}

TEST(MemoryUsageMonitor, AboveMaxMemory) {
    auto monitor = std::make_unique<KillAllMemoryUsageMonitor>(kMemoryUsageUpdateBatchSize / 2);
    auto manager = std::make_unique<ConcurrentMemoryAggregator>(std::move(monitor));
    auto local1 = manager->createChunkedMemoryAggregator(kChunkedMemoryAggregatorOptions);
    auto local2 = manager->createChunkedMemoryAggregator(kChunkedMemoryAggregatorOptions);
    auto handle1 = local1->createUsageHandle();

    ASSERT_THROWS(handle1.set(kMemoryUsageUpdateBatchSize), SPException);
    ASSERT_EQUALS(kMemoryUsageUpdateBatchSize, manager->getCurrentMemoryUsageBytes());
}

};  // namespace
};  // namespace streams
