/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/util/concurrent_memory_aggregator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/common_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/unflushed_state_container.h"
#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {
namespace {

void assertStatsEqual(std::vector<OperatorStats> expected,
                      std::vector<mongo::CheckpointOperatorInfo> actual) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        auto actualStats = toOperatorStats(actual[i].getStats());
        auto expectedStats = expected[i];
        ASSERT_EQ(expectedStats.numInputBytes, actualStats.numInputBytes);
        ASSERT_EQ(expectedStats.numInputDocs, actualStats.numInputDocs);
        ASSERT_EQ(expectedStats.numOutputBytes, actualStats.numOutputBytes);
        ASSERT_EQ(expectedStats.numOutputDocs, actualStats.numOutputDocs);
        ASSERT_EQ(expectedStats.numDlqDocs, actualStats.numDlqDocs);
        ASSERT_EQ(expectedStats.numDlqBytes, actualStats.numDlqBytes);
    }
}

struct Metrics {
    double durationSinceLastCommittedMs{0};
    double numOngoing{0};
};

Metrics getMetrics(Executor* executor, std::string processorId) {
    TestMetricsVisitor metrics;
    executor->getMetricManager()->visitAllMetrics(&metrics);
    const auto& callbackGauges = metrics.callbackGauges().find(processorId);
    double durationSinceLastCommitted = -1;
    if (callbackGauges != metrics.callbackGauges().end()) {
        auto it = callbackGauges->second.find(std::string{"duration_since_last_checkpoint_ms"});
        if (it != callbackGauges->second.end()) {
            durationSinceLastCommitted = it->second->value();
        }
    }
    const auto& gauges = metrics.gauges().find(processorId)->second;
    auto numOngoing = gauges.find(std::string{"checkpoint_num_ongoing"});
    return Metrics{.durationSinceLastCommittedMs = durationSinceLastCommitted,
                   .numOngoing = numOngoing != gauges.end() ? numOngoing->second->value() : -1};
}

void testBasicIdAndCommitLogic(InMemoryCheckpointStorage* storage,
                               Executor* executor,
                               std::string processorId,
                               Context* context) {
    auto startingMetrics = getMetrics(executor, processorId);
    // Validate there is no latest checkpointId.
    ASSERT(!storage->getLatestCommittedCheckpointId());
    // Validate non-existent checkpoint are ignored in onCheckpointFlushed.
    storage->onCheckpointFlushed(CheckpointId{0});
    ASSERT(storage->getFlushedCheckpoints().empty());
    // Create an ID, but don't commit it.
    auto id = storage->startCheckpoint();
    ASSERT_EQ(startingMetrics.numOngoing + 1, getMetrics(executor, processorId).numOngoing);
    // Validate there is still no latest committed checkpoint.
    ASSERT(!storage->getLatestCommittedCheckpointId());
    // Commit and validate readLatest returns it.
    std::vector<OperatorStats> dummyStats{OperatorStats{"", 2, id / 2, 4, 5, 1, 10},
                                          OperatorStats{"", 2, id / 2, 4, 5, 1, 11}};
    storage->addStats(id, 0, dummyStats[0]);
    storage->addStats(id, 1, dummyStats[1]);
    storage->commitCheckpoint(id);
    ASSERT_EQ(startingMetrics.numOngoing, getMetrics(executor, processorId).numOngoing);
    auto duration1 = getMetrics(executor, processorId).durationSinceLastCommittedMs;
    sleepFor(Milliseconds(10));
    auto duration2 = getMetrics(executor, processorId).durationSinceLastCommittedMs;
    ASSERT_GT(duration2, duration1);
    ASSERT_EQ(id, storage->getLatestCommittedCheckpointId());
    context->restoredCheckpointInfo = storage->startCheckpointRestore(id);
    auto opInfo = *context->restoredCheckpointInfo->operatorInfo;
    assertStatsEqual(dummyStats, opInfo);
    storage->checkpointRestored(id);
    // Create 100 empty checkpoints, commit them, validate the most recent is returned.
    std::vector<CheckpointId> ids{id};
    auto lastId = id;
    for (int i = 0; i < 100; ++i) {
        auto id = storage->startCheckpoint();

        std::vector<OperatorStats> restoreCheckpointStats;
        restoreCheckpointStats.reserve(context->restoredCheckpointInfo->operatorInfo->size());
        for (auto& opInfo : *context->restoredCheckpointInfo->operatorInfo) {
            restoreCheckpointStats.push_back(toOperatorStats(opInfo.getStats()));
        }
        std::vector<OperatorStats> dummyStats{OperatorStats{"", 10, 100, 4, 400, 10, 1000},
                                              OperatorStats{"", 10, 100, 4, 400, 10, 1001}};

        ASSERT_EQ(lastId, *storage->getLatestCommittedCheckpointId());
        storage->addStats(id, 0, dummyStats[0]);
        storage->addStats(id, 1, dummyStats[1]);
        storage->commitCheckpoint(id);
        ASSERT_EQ(id, *storage->getLatestCommittedCheckpointId());
        context->restoredCheckpointInfo = storage->startCheckpointRestore(id);
        auto stats = *context->restoredCheckpointInfo->operatorInfo;
        storage->checkpointRestored(id);

        auto expectedStats = combineAdditiveStats(dummyStats, restoreCheckpointStats);
        assertStatsEqual(expectedStats, stats);
        ids.push_back(id);
        lastId = id;
    }

    // Validate the committed checkpoints can be flushed.
    for (CheckpointId id : ids) {
        storage->onCheckpointFlushed(CheckpointId{id});
    }
    auto flushed = storage->getFlushedCheckpoints();
    ASSERT_EQ(ids.size(), flushed.size());
    for (size_t idx = 0; idx < ids.size(); ++idx) {
        ASSERT_EQ(ids[idx], flushed[idx].getId());
    }
}

class CheckpointStorageTest : public AggregationContextFixture {
protected:
    auto makeContext(std::string tenantId, std::string streamProcessorId) {
        MetricManager::LabelsVec labels;
        labels.push_back(std::make_pair(kTenantIdLabelKey, tenantId));
        labels.push_back(std::make_pair(kProcessorIdLabelKey, streamProcessorId));
        auto context = std::make_unique<Context>();
        context->streamProcessorId = streamProcessorId;
        context->tenantId = tenantId;
        context->memoryAggregator =
            _memoryAggregator->createChunkedMemoryAggregator(ChunkedMemoryAggregator::Options());
        return context;
    }

    auto makeStorage(Context* context, Executor& executor) {
        std::string collectionName(UUID::gen().toString());
        std::string dbName("test");
        auto c = std::make_unique<InMemoryCheckpointStorage>(context);
        c->registerMetrics(executor.getMetricManager());
        return c;
    }

    std::unique_ptr<ConcurrentMemoryAggregator> _memoryAggregator =
        std::make_unique<ConcurrentMemoryAggregator>(nullptr);
    ServiceContext* _serviceContext{getServiceContext()};
};

TEST_F(CheckpointStorageTest, BasicIdAndCommitLogic) {
    std::string tenantId = UUID::gen().toString();
    std::string streamProcessorId = UUID::gen().toString();
    auto context = makeContext(tenantId, streamProcessorId);
    std::unique_ptr<Executor> executor = std::make_unique<Executor>(
        context.get(), Executor::Options{.metricManager = std::make_unique<MetricManager>()});
    auto storage = makeStorage(context.get(), *executor);
    testBasicIdAndCommitLogic(storage.get(), executor.get(), streamProcessorId, context.get());
}

TEST_F(CheckpointStorageTest, BasicOperatorState) {
    std::string tenantId = UUID::gen().toString();
    std::string streamProcessorId = UUID::gen().toString();
    auto innerTest = [&](uint32_t numOperators, uint32_t chunksPerOperator) {
        auto context = makeContext(tenantId, streamProcessorId);
        std::unique_ptr<Executor> executor = std::make_unique<Executor>(
            context.get(), Executor::Options{.metricManager = std::make_unique<MetricManager>()});
        auto storage = makeStorage(context.get(), *executor);
        auto id = storage->startCheckpoint();
        stdx::unordered_map<OperatorId, std::vector<BSONObj>> expectedState;
        std::vector<OperatorStats> stats;
        for (OperatorId operatorId = 0; size_t(operatorId) < numOperators; ++operatorId) {
            auto writer = storage->createStateWriter(id, operatorId);
            expectedState.emplace(operatorId, std::vector<BSONObj>{});
            for (uint32_t chunk = 0; chunk < chunksPerOperator; ++chunk) {
                expectedState[operatorId].push_back(
                    BSON("a" << UUID::gen().toString() << "b" << (int64_t)chunk << "_id" << 0));
                storage->appendRecord(writer.get(), Document(expectedState[operatorId].back()));
            }
            stats.push_back(OperatorStats{
                "",
                // Use an assorted number of input/output docs based on the operatorId.
                operatorId * 3,
                operatorId * 3 * 10,
                operatorId + 1,
                (operatorId + 1) * 10,
                operatorId + 2,
                (operatorId + 2) * 10});
            storage->addStats(id, operatorId, stats[operatorId]);
        }
        storage->commitCheckpoint(id);
        context->restoredCheckpointInfo = storage->startCheckpointRestore(id);
        for (uint32_t operatorId = 0; operatorId < numOperators; ++operatorId) {
            auto reader = storage->createStateReader(id, operatorId);
            for (int32_t chunkNumber = 0; size_t(chunkNumber) < chunksPerOperator; ++chunkNumber) {
                auto state = storage->getNextRecord(reader.get());
                ASSERT(state);
                ASSERT_BSONOBJ_EQ(expectedState[operatorId][chunkNumber], state->toBson());
            }
        }
        auto checkpointStats = *context->restoredCheckpointInfo->operatorInfo;
        assertStatsEqual(stats, checkpointStats);
        storage->checkpointRestored(id);
    };

    innerTest(10, 1);
    innerTest(2, 10);
    innerTest(10, 10);
}

TEST_F(CheckpointStorageTest, BasicMultipleProcessors) {
    int countThreads = 20;
    std::vector<stdx::thread> threads;
    auto tenantId = UUID::gen().toString();
    for (int i = 0; i < countThreads; ++i) {
        threads.emplace_back([this, i, tenantId]() {
            std::string streamProcessorId(i, 'a');
            auto context = makeContext(tenantId, streamProcessorId);
            std::unique_ptr<Executor> executor = std::make_unique<Executor>(
                context.get(),
                Executor::Options{.metricManager = std::make_unique<MetricManager>()});
            auto storage = makeStorage(context.get(), *executor);
            testBasicIdAndCommitLogic(
                storage.get(), executor.get(), streamProcessorId, context.get());
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(CheckpointStorageTest, UnflushedStateContainerTest) {
    UnflushedStateContainer container;
    auto state1 = std::make_pair(1, BSON("a" << 1));
    container.add(state1.first, state1.second);
    // checkpoint1's state should be returned.
    auto result = container.pop(state1.first);
    ASSERT_BSONOBJ_EQ(state1.second, result);
    // checkpoint1's state should no longer be in the container.
    ASSERT(!container.contains(state1.first));
    ASSERT_THROWS_CODE(container.pop(state1.first), DBException, ErrorCodes::InternalError);

    auto state2 = std::make_pair(2, BSON("a" << 2));
    container.add(state2.first, state2.second);
    // checkpoint2's state should be returned.
    ASSERT(container.contains(state2.first));
    result = container.pop(state2.first);
    ASSERT_BSONOBJ_EQ(state2.second, result);
    // checkpoint2's state should no longer be in the container.
    ASSERT_THROWS_CODE(container.pop(state2.first), DBException, ErrorCodes::InternalError);

    container.add(state1.first, state1.second);
    ASSERT(container.contains(state1.first));
    container.add(state2.first, state2.second);
    ASSERT(container.contains(state2.first));
    // This should error, state2.first is not the oldest unflushed checkpointId.
    ASSERT_THROWS_CODE(container.pop(state2.first), DBException, ErrorCodes::InternalError);
}

}  // namespace
}  // namespace streams
