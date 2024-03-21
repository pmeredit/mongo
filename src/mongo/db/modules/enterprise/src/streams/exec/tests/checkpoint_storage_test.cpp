/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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
#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {
namespace {

// A visitor class that can be used with MetricManager::visitAllMetrics().
class TestMetricsVisitor {
public:
    const auto& gauges() {
        return _gauges;
    }

    const auto& intGauges() {
        return _intGauges;
    }

    const auto& callbackGauges() {
        return _callbackGauges;
    }

    void visit(Counter* counter,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {}

    void visit(Gauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _gauges[getProcessorIdLabel(labels)][name] = gauge;
    }

    void visit(IntGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _intGauges[getProcessorIdLabel(labels)][name] = gauge;
    }

    void visit(CallbackGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _callbackGauges[getProcessorIdLabel(labels)][name] = gauge;
    }

    void visit(Histogram* histogram,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {}

private:
    std::string getProcessorIdLabel(const MetricManager::LabelsVec& labels) {
        auto result = std::find_if(labels.begin(), labels.end(), [](const auto& l) {
            return l.first == kProcessorIdLabelKey;
        });
        invariant(result != labels.end());
        return result->second;
    }

    using ProcessorId = std::string;
    using MetricName = std::string;
    stdx::unordered_map<ProcessorId, stdx::unordered_map<MetricName, Gauge*>> _gauges;
    stdx::unordered_map<ProcessorId, stdx::unordered_map<MetricName, IntGauge*>> _intGauges;
    stdx::unordered_map<ProcessorId, stdx::unordered_map<MetricName, CallbackGauge*>>
        _callbackGauges;
};

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

std::string getProcessorIdLabel(const std::vector<mongo::MetricLabel>& labels) {
    auto result = std::find_if(labels.begin(), labels.end(), [](const auto& l) {
        return l.getKey() == kProcessorIdLabelKey;
    });
    invariant(result != labels.end());
    return result->getValue().toString();
}

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
                               std::string processorId) {
    auto startingMetrics = getMetrics(executor, processorId);
    // Validate there is no latest checkpointId.
    ASSERT(!storage->getLatestCommittedCheckpointId());
    // Create an ID, but don't commit it.
    auto id = storage->startCheckpoint();
    ASSERT_EQ(startingMetrics.numOngoing + 1, getMetrics(executor, processorId).numOngoing);
    // Validate there is still no latest committed checkpoint.
    ASSERT(!storage->getLatestCommittedCheckpointId());
    // Commit and validate readLatest returns it.
    std::vector<OperatorStats> dummyStats{OperatorStats{"", 2, id / 2, 4, 5, 1, 10}};
    storage->addStats(id, 0, dummyStats[0]);
    storage->commitCheckpoint(id);
    ASSERT_EQ(startingMetrics.numOngoing, getMetrics(executor, processorId).numOngoing);
    auto duration1 = getMetrics(executor, processorId).durationSinceLastCommittedMs;
    sleepFor(Milliseconds(10));
    auto duration2 = getMetrics(executor, processorId).durationSinceLastCommittedMs;
    ASSERT_GT(duration2, duration1);
    ASSERT_EQ(id, storage->getLatestCommittedCheckpointId());
    storage->startCheckpointRestore(id);
    auto opInfo = storage->getRestoreCheckpointOperatorInfo();
    assertStatsEqual(dummyStats, opInfo);
    storage->checkpointRestored(id);
    // Create 100 empty checkpoints, commit them, validate the most recent is returned.
    std::vector<CheckpointId> ids;
    auto lastId = id;
    for (int i = 0; i < 100; ++i) {
        auto id = storage->startCheckpoint();
        std::vector<OperatorStats> dummyStats{OperatorStats{"", 10, 100, 4, 400, 10, 1000}};
        ASSERT_EQ(lastId, *storage->getLatestCommittedCheckpointId());
        storage->addStats(id, 0, dummyStats[0]);
        storage->commitCheckpoint(id);
        ASSERT_EQ(id, *storage->getLatestCommittedCheckpointId());
        storage->startCheckpointRestore(id);
        auto stats = storage->getRestoreCheckpointOperatorInfo();
        storage->checkpointRestored(id);
        assertStatsEqual(dummyStats, stats);
        ids.push_back(id);
        lastId = id;
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
    std::unique_ptr<MetricManager> _metricManager = std::make_unique<MetricManager>();
    ServiceContext* _serviceContext{getServiceContext()};
    std::unique_ptr<Context> _context{std::get<0>(getTestContext(_serviceContext))};
    std::unique_ptr<Executor> _executor =
        std::make_unique<Executor>(_context.get(), Executor::Options{});
    bool _useRealMongo{false};
    std::string _mongodbUri;
    const std::string _database{"test"};
};

TEST_F(CheckpointStorageTest, BasicIdAndCommitLogic) {
    std::string tenantId = UUID::gen().toString();
    std::string streamProcessorId = UUID::gen().toString();
    auto context = makeContext(tenantId, streamProcessorId);
    std::unique_ptr<Executor> executor =
        std::make_unique<Executor>(_context.get(), Executor::Options{});
    auto storage = makeStorage(context.get(), *executor);
    testBasicIdAndCommitLogic(storage.get(), executor.get(), streamProcessorId);
}

TEST_F(CheckpointStorageTest, BasicOperatorState) {
    std::string tenantId = UUID::gen().toString();
    std::string streamProcessorId = UUID::gen().toString();
    auto innerTest = [&](uint32_t numOperators, uint32_t chunksPerOperator) {
        auto context = makeContext(tenantId, streamProcessorId);
        std::unique_ptr<Executor> executor =
            std::make_unique<Executor>(_context.get(), Executor::Options{});
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
        storage->startCheckpointRestore(id);
        for (uint32_t operatorId = 0; operatorId < numOperators; ++operatorId) {
            auto reader = storage->createStateReader(id, operatorId);
            for (int32_t chunkNumber = 0; size_t(chunkNumber) < chunksPerOperator; ++chunkNumber) {
                auto state = storage->getNextRecord(reader.get());
                ASSERT(state);
                ASSERT_BSONOBJ_EQ(expectedState[operatorId][chunkNumber], state->toBson());
            }
        }
        auto checkpointStats = storage->getRestoreCheckpointOperatorInfo();
        assertStatsEqual(stats, checkpointStats);
        storage->checkpointRestored(id);
    };

    innerTest(10, 1);
    innerTest(1, 10);
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
            std::unique_ptr<Executor> executor =
                std::make_unique<Executor>(_context.get(), Executor::Options{});
            auto storage = makeStorage(context.get(), *executor);
            testBasicIdAndCommitLogic(storage.get(), executor.get(), streamProcessorId);
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

}  // namespace
}  // namespace streams
