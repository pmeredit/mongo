/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>
#include <boost/none.hpp>
#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <rdkafkacpp.h>
#include <string>
#include <vector>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/periodic_runner_factory.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/old_checkpoint_storage.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/old_in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/management/stream_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using mongo::stdx::chrono::milliseconds;

/**
 * CheckpointTestWorkload helps run test workloads to test
 * checkpointing.
 */
class CheckpointTestWorkload {
public:
    struct PartitionInput {
        std::vector<BSONObj> docs;
        int docsPerChunk{1};
    };
    using Input = stdx::unordered_map<int32_t, PartitionInput>;

    struct Properties {
        std::unique_ptr<Context> context;
        std::unique_ptr<Executor> executor;
        KafkaConsumerOperator* source;
        InMemorySinkOperator* sink;
        std::unique_ptr<OperatorDag> dag;
        std::unique_ptr<CheckpointCoordinator> checkpointCoordinator;
        Input input;
        std::vector<BSONObj> userBson;
        std::unique_ptr<MetricManager> metricManager;
    };

    CheckpointTestWorkload(std::string pipeline,
                           Input input,
                           ServiceContext* svcCtx,
                           bool useNewStorage = false) {
        auto bsonVector = parseBsonVector(pipeline);
        bsonVector.insert(bsonVector.begin(), testKafkaSourceSpec(input.size()));
        bsonVector.push_back(getTestMemorySinkSpec());
        _props.userBson = bsonVector;

        _props.metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Executor> executor;
        std::tie(_props.context, executor) =
            getTestContext(nullptr, UUID::gen().toString(), UUID::gen().toString());
        if (useNewStorage) {
            _props.context->checkpointStorage =
                std::make_unique<InMemoryCheckpointStorage>(_props.context.get());
            _props.context->checkpointStorage->registerMetrics(executor->getMetricManager());
        } else {
            _props.context->oldCheckpointStorage =
                makeCheckpointStorage(svcCtx, _props.context.get());
            _props.context->oldCheckpointStorage->registerMetrics(executor->getMetricManager());
        }
        _props.context->dlq = std::make_unique<NoOpDeadLetterQueue>(_props.context.get());
        _props.context->connections = testKafkaConnectionRegistry();
        Planner planner(_props.context.get(), {});
        _props.dag = planner.plan(bsonVector);

        CheckpointCoordinator::Options coordinatorOptions{
            .processorId = _props.context->streamProcessorId,
            .oldStorage = _props.context->oldCheckpointStorage.get(),
            .storage = _props.context->checkpointStorage.get()};
        _props.checkpointCoordinator =
            std::make_unique<CheckpointCoordinator>(std::move(coordinatorOptions));

        _props.input = std::move(input);

        init();
    }

    CheckpointTestWorkload(std::string pipeline,
                           std::vector<BSONObj> input,
                           ServiceContext* svcCtx,
                           bool useNextStorage = false)
        : CheckpointTestWorkload(
              pipeline, Input{{0 /* partitionId */, {std::move(input)}}}, svcCtx, useNextStorage) {}

    Properties& props() {
        return _props;
    }

    CheckpointControlMsg checkpointAndRun() {
        // Create a checkpoint and send it through the operator dag.
        auto checkpointControlMsg =
            _props.checkpointCoordinator->getCheckpointControlMsgIfReady(/*force*/ true);
        _props.source->onControlMsg(0 /* inputIdx */,
                                    StreamControlMsg{.checkpointMsg = *checkpointControlMsg});
        _props.executor->runOnce();
        return *checkpointControlMsg;
    }

    void runAll() {
        while (_props.executor->runOnce() == Executor::RunStatus::kActive) {
        }
    }

    std::vector<StreamMsgUnion> getResults() {
        std::vector<StreamMsgUnion> results;
        auto messages = _props.sink->getMessages();


        while (!messages.empty()) {
            results.push_back(messages.front());
            messages.pop_front();
        }
        return results;
    }

    std::vector<StreamDocument> getDataResults() {
        std::vector<StreamDocument> dataResults;
        for (auto& result : getResults()) {
            if (result.dataMsg) {
                for (auto& doc : result.dataMsg->docs) {
                    dataResults.push_back(std::move(doc));
                }
            }
        }
        return dataResults;
    }

    void restore(CheckpointId checkpointId) {
        _props.context->restoreCheckpointId = checkpointId;
        _props.context->connections = testKafkaConnectionRegistry();
        Planner planner(_props.context.get(), {});
        _props.dag = planner.plan(_props.userBson);
        init();
    }

    boost::optional<CheckpointId> getLatestCommittedCheckpointId() {
        if (_props.context->oldCheckpointStorage) {
            return _props.context->oldCheckpointStorage->readLatestCheckpointId();
        } else {
            auto inMemoryStorage =
                dynamic_cast<InMemoryCheckpointStorage*>(_props.context->checkpointStorage.get());
            return inMemoryStorage->getLatestCommittedCheckpointId();
        }
    }

    bool isCheckpointCommitted(CheckpointId id) {
        if (_props.context->oldCheckpointStorage) {
            return bool(_props.context->oldCheckpointStorage->readCheckpointInfo(id));
        } else {
            auto storageV1 =
                dynamic_cast<InMemoryCheckpointStorage*>(_props.context->checkpointStorage.get());
            return storageV1->_checkpoints[id].committed;
        }
    }

    boost::optional<BSONObj> getSingleState(CheckpointId checkpointId, OperatorId operatorId) {
        if (_props.context->oldCheckpointStorage) {
            auto result = _props.context->oldCheckpointStorage->readState(
                checkpointId, operatorId, 0 /* chunkNumber */);
            ASSERT_FALSE(_props.context->oldCheckpointStorage->readState(
                checkpointId, operatorId, 1 /* chunkNumber */));
            return result;
        } else {
            auto reader =
                _props.context->checkpointStorage->createStateReader(checkpointId, operatorId);
            auto result = _props.context->checkpointStorage->getNextRecord(reader.get());
            ASSERT_FALSE(_props.context->checkpointStorage->getNextRecord(reader.get()));
            if (result) {
                return result->toBson();
            }
            return boost::none;
        }
    }

private:
    void init() {
        invariant(_props.dag && _props.context && _props.metricManager &&
                  (_props.context->oldCheckpointStorage || _props.context->checkpointStorage));
        _props.source = dynamic_cast<KafkaConsumerOperator*>(_props.dag->operators().front().get());
        _props.sink = dynamic_cast<InMemorySinkOperator*>(_props.dag->operators().back().get());
        _props.executor = std::make_unique<Executor>(
            _props.context.get(),
            Executor::Options{.operatorDag = _props.dag.get(),
                              .checkpointCoordinator = _props.checkpointCoordinator.get()});

        for (auto& oper : _props.dag->operators()) {
            oper->registerMetrics(_props.executor->getMetricManager());
        }
        // Start the DAG so the consumers are created.
        _props.dag->start();
        ASSERT_EQ(_props.input.size(), _props.source->_consumers.size());

        // Setup the consumers with the input.
        for (auto& kv : _props.input) {
            const int partition = kv.first;
            auto& input = kv.second;
            auto consumerIt =
                std::find_if(_props.source->_consumers.begin(),
                             _props.source->_consumers.end(),
                             [=](auto& consumer) { return consumer.partition == partition; });
            ASSERT_NOT_EQUALS(consumerIt, _props.source->_consumers.end());
            auto consumer = dynamic_cast<FakeKafkaPartitionConsumer*>(consumerIt->consumer.get());
            std::vector<KafkaSourceDocument> sourceDocs;
            for (auto& doc : input.docs) {
                sourceDocs.push_back(KafkaSourceDocument{.doc = doc, .sizeBytes = doc.objsize()});
            }
            consumer->_docsPerChunk = input.docsPerChunk;
            consumer->_overrideOffsets = true;
            consumer->addDocuments(std::move(sourceDocs));
        }
    }

    Properties _props;
};

class CheckpointTest : public AggregationContextFixture {
public:
    CheckpointTest() {
        // Set up the periodic runner to allow background job execution for tests that require it.
        auto runner = makePeriodicRunner(_serviceContext);
        _serviceContext->setPeriodicRunner(std::move(runner));
    }

    BSONObj toOriginalBson(Document doc) {
        return doc.toBson().removeField("_stream_meta").removeField("_ts");
    }

    // Workaround for parametrized tests.
    void Test_AlwaysCheckpoint_EmptyPipeline(bool useNewStorage);
    void Test_AlwaysCheckpoint_EmptyPipeline_MultiPartition(bool useNewStorage);
    void Test_CoordinatorWallclockTime(bool useNewStorage);

protected:
    ServiceContext* _serviceContext{getServiceContext()};
};

void CheckpointTest::Test_AlwaysCheckpoint_EmptyPipeline(bool useNewStorage) {
    const int partitionCount = 1;

    std::vector<BSONObj> input = {
        fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839"})"),
        fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839"})"),
        fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:04:20.062839"})"),
        fromjson(R"({"id": 15, "timestamp": "2023-04-10T17:05:20.062839"})"),
        fromjson(R"({"id": 16, "timestamp": "2023-04-10T17:06:20.062839"})"),
    };
    CheckpointTestWorkload workload("[]", input, _serviceContext, useNewStorage);

    // Run the workload, sending a checkpoint before every document.
    std::vector<CheckpointId> checkpointIds;
    std::vector<BSONObj> fullResults;
    CheckpointId lastId{-1};
    for (size_t idx = 0; idx < input.size(); ++idx) {
        // Checkpoint then send a single document through the DAG.
        workload.checkpointAndRun();

        // Verify the document in the sink.
        auto results = workload.getDataResults();
        ASSERT_EQ(1, results.size());
        auto bsonResult = toOriginalBson(results[0].doc);
        ASSERT_BSONOBJ_EQ(input[idx], bsonResult);
        fullResults.push_back(bsonResult);

        // Verify the valid committed checkpointId in checkpoint storage.
        auto checkpointId = workload.getLatestCommittedCheckpointId();
        ASSERT(checkpointId);
        ASSERT_NE(lastId, *checkpointId);
        lastId = *checkpointId;

        // Verify the $source operator state in the checkpoint.
        ASSERT_EQ(0, workload.props().source->getOperatorId());
        auto state =
            workload.getSingleState(*checkpointId, workload.props().source->getOperatorId());
        ASSERT(state);
        auto sourceState = KafkaSourceCheckpointState::parse(IDLParserContext("test"), *state);
        ASSERT_EQ(partitionCount, sourceState.getPartitions().size());
        auto partitionState = sourceState.getPartitions()[0];
        ASSERT_EQ(0, partitionState.getPartition());
        // On each idx, we checkpoint before we send input[idx].
        // So, for idx==0, the checkpoint correponds to before sending any data.
        // At idx==1, the checkpoint corresponds to having already sent data[0].
        // At idx==2, the checkpoint corresponds to having already sent data[1].
        ASSERT_EQ(idx, partitionState.getOffset());

        // Verify no watermark data in the state.
        ASSERT_EQ(boost::none, partitionState.getWatermark());

        // Verify no other operators have state in the checkpoint.
        auto it = workload.props().dag->operators().begin() + 1;
        while (it != workload.props().dag->operators().end()) {
            auto operatorId = (*it)->getOperatorId();
            state = workload.getSingleState(*checkpointId, operatorId);
            ASSERT_FALSE(state);
            it = next(it);
        }

        // Save the checkpoint ID for later.
        checkpointIds.push_back(*checkpointId);
    }

    // We should have one checkpoint ID that was written before each input.
    ASSERT_EQ(checkpointIds.size(), input.size());

    for (size_t idx = 0; idx < input.size(); ++idx) {
        // Get the checkpointId written on the idx loop.
        auto checkpointId = checkpointIds[idx];
        // Restore from that checkpoint.
        workload.restore(checkpointId);
        // Run until the $source is exhausted.
        workload.runAll();
        auto results = workload.getDataResults();
        // Verify the number of documents output after restoring from checkpointId.
        // When idx==0, the checkpoint occurred before any document in input was sent.
        // When idx==1, the checkpoint occurred after the first document in input was sent.
        // Thus when restoring from idx==1, we expected the output to be size: input.size() - 1,
        // or input.size() - idx.
        auto expectedNumOutput = input.size() - idx;
        ASSERT_EQ(expectedNumOutput, results.size());
        for (size_t resultIdx = 0; resultIdx < results.size(); ++resultIdx) {
            auto expected = input[idx + resultIdx];
            auto actual = toOriginalBson(results[resultIdx].doc);
            ASSERT_BSONOBJ_EQ(expected, actual);
        }
    }
}

TEST_F(CheckpointTest, AlwaysCheckpoint_EmptyPipeline) {
    Test_AlwaysCheckpoint_EmptyPipeline(false /* useNewStorage */);
    Test_AlwaysCheckpoint_EmptyPipeline(true /* useNewStorage */);
}

void CheckpointTest::Test_AlwaysCheckpoint_EmptyPipeline_MultiPartition(bool useNewStorage) {
    CheckpointTestWorkload::Input input = {
        {0,
         {.docs =
              {
                  fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839"})"),
                  fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839"})"),
                  fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:04:20.062839"})"),
                  fromjson(R"({"id": 15, "timestamp": "2023-04-10T17:05:20.062839"})"),
                  fromjson(R"({"id": 16, "timestamp": "2023-04-10T17:06:20.062839"})"),
              },
          .docsPerChunk = 1}},
        {1,
         {.docs =
              {
                  fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839"})"),
                  fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839"})"),
                  fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:04:20.062839"})"),
                  fromjson(R"({"id": 15, "timestamp": "2023-04-10T17:05:20.062839"})"),
                  fromjson(R"({"id": 16, "timestamp": "2023-04-10T17:06:20.062839"})"),
              },
          .docsPerChunk = 3}},
        {2,
         {.docs =
              {
                  fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839"})"),
                  fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839"})"),
                  fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:04:20.062839"})"),
                  fromjson(R"({"id": 15, "timestamp": "2023-04-10T17:05:20.062839"})"),
                  fromjson(R"({"id": 16, "timestamp": "2023-04-10T17:06:20.062839"})"),
              },
          .docsPerChunk = 1}}};
    CheckpointTestWorkload workload("[]", input, _serviceContext, useNewStorage);

    // inputIdx tracks, per partition, the current index we're at in the vector
    // of input docs.
    stdx::unordered_map<int, size_t> inputIdx;
    for (auto& kv : input) {
        auto partition = kv.first;
        inputIdx[partition] = 0;
    }

    // lambda to retrieve the current input index for the partition.
    auto currentIdx = [&](int partition) {
        auto idx = inputIdx[partition];
        if (idx < input[partition].docs.size()) {
            return idx;
        } else {
            return input[partition].docs.size();
        }
    };

    // true if any input remains.
    auto inputRemains = [&]() {
        for (auto& [partition, idx] : inputIdx) {
            auto count = input[partition].docs.size();
            if (idx < count) {
                return true;
            }
        }
        return false;
    };

    // expected number of documents to output in the next
    // loop execution.
    auto expectedNumDocsThisRun = [&]() {
        size_t result = 0;
        int64_t numBatches = 0;
        int64_t maxBatches = input.size();
        int64_t currPartition = 0;
        int64_t totalRemainingDocs = 0;

        for (const auto& [partition, options] : input) {
            totalRemainingDocs += options.docs.size() - currentIdx(partition);
        }

        // We may send more than one batch per partition if another partition has
        // no batches to send. On every run iteration, we try to send atmost N
        // batches where N is the number of partitions.
        while (numBatches < maxBatches && totalRemainingDocs > 0) {
            const auto& options = input[currPartition];
            auto remainingDocs = options.docs.size() - currentIdx(currPartition);
            auto chunkSize = size_t(options.docsPerChunk);
            auto expectedNumDocs = remainingDocs < chunkSize ? remainingDocs : chunkSize;

            if (expectedNumDocs > 0) {
                ++numBatches;
                inputIdx[currPartition] = inputIdx[currPartition] + expectedNumDocs;
            }

            currPartition = (currPartition + 1) % input.size();
            totalRemainingDocs -= expectedNumDocs;
            result += expectedNumDocs;
        }

        return result;
    };

    // tracks the expected number of docs to output after restoring from
    // each CheckpointId.
    stdx::unordered_map<CheckpointId, int> expectedCountOutputAfterRestore;
    auto docsRemaining = [&](const auto& inputIdxSnapshot) {
        size_t result = 0;
        for (auto& [partition, options] : input) {
            auto it = inputIdxSnapshot.find(partition);
            ASSERT_TRUE(it != inputIdxSnapshot.end());

            result += options.docs.size() - it->second;
        }
        return result;
    };

    // Run the workload, sending a checkpoint before every document.
    std::vector<CheckpointId> checkpointIds;
    std::vector<BSONObj> fullResults;
    while (inputRemains()) {
        auto inputIdxSnapshot = inputIdx;

        // Send a document from each partition through the DAG.
        // Since checkpoint is configured to always run,
        // before sending the document, the Executor
        // will send a checkpoint message.
        workload.checkpointAndRun();

        // Verify the documents in the sink.
        auto results = workload.getDataResults();
        ASSERT_EQ(expectedNumDocsThisRun(), results.size());

        // For each document, inspect the stream meta to verify it.
        for (auto& doc : results) {
            int64_t offset = *doc.streamMeta.getSourceOffset();
            int32_t partition = *doc.streamMeta.getSourcePartition();
            ASSERT_BSONOBJ_EQ(toOriginalBson(doc.doc), input[partition].docs[offset]);
        }

        // Verify the valid committed checkpointId in checkpoint storage.
        auto checkpointId = workload.getLatestCommittedCheckpointId();
        ASSERT(checkpointId);

        // Verify the $source operator state in the checkpoint.
        ASSERT_EQ(0, workload.props().source->getOperatorId());
        auto state =
            workload.getSingleState(*checkpointId, workload.props().source->getOperatorId());
        ASSERT(state);
        auto sourceState = KafkaSourceCheckpointState::parse(IDLParserContext("test"), *state);
        ASSERT_EQ(input.size(), sourceState.getPartitions().size());

        // Verify the partition state, including log offsets.
        for (size_t partition = 0; partition < sourceState.getPartitions().size(); ++partition) {
            auto& partitionState = sourceState.getPartitions()[partition];
            ASSERT_EQ(partition, partitionState.getPartition());
            ASSERT_EQ(boost::none, partitionState.getWatermark());
            ASSERT_EQ(inputIdxSnapshot[partition], partitionState.getOffset());
        }

        // Verify no other operators have state in the checkpoint.
        auto it = workload.props().dag->operators().begin() + 1;
        while (it != workload.props().dag->operators().end()) {
            auto operatorId = (*it)->getOperatorId();
            ASSERT_FALSE(workload.getSingleState(*checkpointId, operatorId));
            it = next(it);
        }

        // Save the checkpoint ID for later.
        checkpointIds.push_back(*checkpointId);
        expectedCountOutputAfterRestore[*checkpointId] = docsRemaining(inputIdxSnapshot);
    }

    for (CheckpointId checkpointId : checkpointIds) {
        // Restore from that checkpoint.
        workload.restore(checkpointId);
        // Run until the $source is exhausted.
        workload.runAll();
        auto results = workload.getDataResults();
        // Verify the number of documents output after restoring from checkpointId.
        auto expectedNumOutput = expectedCountOutputAfterRestore[checkpointId];
        ASSERT_EQ(expectedNumOutput, results.size());
        for (size_t resultIdx = 0; resultIdx < results.size(); ++resultIdx) {
            auto& doc = results[resultIdx];
            int64_t offset = *doc.streamMeta.getSourceOffset();
            int32_t partition = *doc.streamMeta.getSourcePartition();
            ASSERT_BSONOBJ_EQ(toOriginalBson(doc.doc), input[partition].docs[offset]);
        }
    }
}

TEST_F(CheckpointTest, AlwaysCheckpoint_EmptyPipeline_MultiPartition) {
    Test_AlwaysCheckpoint_EmptyPipeline_MultiPartition(false /* useNewStorage */);
    Test_AlwaysCheckpoint_EmptyPipeline_MultiPartition(true /* useNewStorage */);
}

void CheckpointTest::Test_CoordinatorWallclockTime(bool useNewStorage) {
    struct Spec {
        milliseconds checkpointInterval{0};
        milliseconds runtime{0};
        int expectedCheckpointCount{0};
        Seconds tolerance{1};
    };

    auto innerTest = [&](Spec spec) {
        CheckpointTestWorkload workload(
            "[]", std::vector<BSONObj>{}, _serviceContext, useNewStorage);
        auto metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Context> context;
        std::unique_ptr<Executor> executor;
        std::tie(context, executor) = getTestContext(_serviceContext);
        auto storage = std::make_unique<OldInMemoryCheckpointStorage>(context.get());
        storage->registerMetrics(executor->getMetricManager());
        auto coordinator = std::make_unique<CheckpointCoordinator>(
            CheckpointCoordinator::Options{"", storage.get(), false, spec.checkpointInterval});
        auto start = stdx::chrono::steady_clock::now();
        std::vector<CheckpointId> checkpoints;
        while (stdx::chrono::steady_clock::now() - start < spec.runtime) {
            auto checkpointMsg = coordinator->getCheckpointControlMsgIfReady();
            if (checkpointMsg) {
                checkpoints.push_back(checkpointMsg->id);
            }
        }
        ASSERT_LTE(std::abs<int>(checkpoints.size() - spec.expectedCheckpointCount),
                   spec.tolerance.count());
    };

    innerTest(Spec{.checkpointInterval = milliseconds{500},
                   .runtime = milliseconds{3000},
                   .expectedCheckpointCount = 6});
    innerTest(Spec{.checkpointInterval = milliseconds{1000},
                   .runtime = milliseconds{5000},
                   .expectedCheckpointCount = 5});
}

TEST_F(CheckpointTest, CoordinatorWallclockTime) {
    Test_CoordinatorWallclockTime(false /* useNewStorage */);
    Test_CoordinatorWallclockTime(true /* useNewStorage */);
}

TEST_F(CheckpointTest, CheckpointStats) {
    std::vector<BSONObj> input = {
        fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839"})"),
        fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839"})"),
        fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:04:20.062839"})"),
        fromjson(R"({"id": 15, "timestamp": "2023-04-10T17:05:20.062839"})"),
        fromjson(R"({"id": 16, "timestamp": "2023-04-10T17:06:20.062839"})"),
    };
    CheckpointTestWorkload workload(R"([
        {
            $match: {
                id: {
                    $gt: 12
                }
            }
        },
        {
            $project: {
                id: 1
            }
        }
    ])",
                                    input,
                                    _serviceContext);

    // Run the workload, sending a checkpoint before every document.
    std::vector<BSONObj> fullResults;
    CheckpointId lastId{-1};
    std::vector<CheckpointId> checkpointIds;
    for (size_t idx = 0; idx < input.size() + 1; ++idx) {
        std::map<OperatorId, OperatorStats> expectedStats;
        for (auto& op : workload.props().dag->operators()) {
            expectedStats[op->getOperatorId()] = op->getStats();
        }
        // Checkpoint then send a single document through the DAG.
        workload.checkpointAndRun();
        // Verify the valid committed checkpointId in checkpoint storage.
        auto checkpointId = workload.getLatestCommittedCheckpointId();
        ASSERT(checkpointId);
        ASSERT_NE(lastId, *checkpointId);
        lastId = *checkpointId;
        checkpointIds.push_back(*checkpointId);
        // Verify the stats in the checkpoint for each operator.
        auto opInfo = workload.props()
                          .context->oldCheckpointStorage->readCheckpointInfo(*checkpointId)
                          ->getOperatorInfo();

        ASSERT_EQ(expectedStats.size(), opInfo.size());
        for (auto& op : opInfo) {
            auto operatorId = op.getOperatorId();
            auto& expected = expectedStats[operatorId];
            auto& stat = op.getStats();
            ASSERT_EQ(expected.numInputBytes, stat.getInputBytes());
            ASSERT_EQ(expected.numInputDocs, stat.getInputDocs());
            ASSERT_EQ(expected.numOutputBytes, stat.getOutputBytes());
            ASSERT_EQ(expected.numOutputDocs, stat.getOutputDocs());
        }
    }
    // Verify the expected number of checkpoints.
    ASSERT_EQ(input.size() + 1, checkpointIds.size());
    // Verify the stats in the last checkpoint after all the input.
    auto stats = workload.props()
                     .context->oldCheckpointStorage
                     ->readCheckpointInfo(*workload.getLatestCommittedCheckpointId())
                     ->getOperatorInfo();
    ASSERT_EQ(4, stats.size());
    // Verify the $source stats.
    ASSERT_EQ(0, stats[0].getOperatorId());
    ASSERT_EQ(5, stats[0].getStats().getInputDocs());
    ASSERT_EQ(5, stats[0].getStats().getOutputDocs());
    ASSERT_GT(stats[0].getStats().getInputBytes(), 0);
    // Verify the first $match stats.
    ASSERT_EQ(1, stats[1].getOperatorId());
    ASSERT_EQ(5, stats[1].getStats().getInputDocs());
    ASSERT_EQ(4, stats[1].getStats().getOutputDocs());
    // Verify the $project stats.
    ASSERT_EQ(2, stats[2].getOperatorId());
    ASSERT_EQ(4, stats[2].getStats().getInputDocs());
    ASSERT_EQ(4, stats[2].getStats().getOutputDocs());
    // Verify the sink stats.
    ASSERT_EQ(3, stats[3].getOperatorId());
    ASSERT_EQ(4, stats[3].getStats().getInputDocs());
    ASSERT_EQ(4, stats[3].getStats().getOutputDocs());
}

TEST_F(CheckpointTest, CheckpointStatsWithWindows) {
    std::vector<BSONObj> input = {
        fromjson(R"({"val": 12, "timestamp": "2023-04-10T17:02:20.000000"})"),
        fromjson(R"({"val": 13, "timestamp": "2023-04-10T17:02:40.000000"})"),
        fromjson(R"({"val": 14, "timestamp": "2023-04-10T17:03:00.001000"})")};
    CheckpointTestWorkload workload(R"([
        {
            $tumblingWindow: {
                interval: { size: 1, unit: "minute" },
                allowedLateness: { size: 0, unit: "second" },
                pipeline: [
                    {
                        $group: {_id: null, id: { $sum: "$val" }}
                    },
                    {
                        $sort: {a: 1}
                    },
                    {
                        $limit: 5
                    }
                ]
            }
        }
    ])",
                                    input,
                                    _serviceContext);

    // Run the workload, sending a checkpoint before every document, and one after the
    // last document.
    std::vector<BSONObj> fullResults;
    std::vector<CheckpointId> checkpointIds;
    for (size_t idx = 0; idx < input.size() + 1; ++idx) {
        // Checkpoint then send a single document through the DAG.
        auto checkpointMsg = workload.checkpointAndRun();
        checkpointIds.push_back(checkpointMsg.id);
    }
    // Verify the expected number of checkpoints.
    ASSERT_EQ(input.size() + 1, checkpointIds.size());
    // Verify the stats in the first checkpoint before any input.
    auto opInfo = workload.props()
                      .context->oldCheckpointStorage->readCheckpointInfo(checkpointIds[0])
                      ->getOperatorInfo();
    ASSERT_EQ(3, opInfo.size());
    // Verify the operator IDs.
    // The $source.
    ASSERT_EQ(0, opInfo[0].getOperatorId());
    // The $tumblingWindow.
    ASSERT_EQ(1, opInfo[1].getOperatorId());
    // The sink's operatorId is 4,
    // because the window's $group inner operator occupies 2, and the sort+limit occupies 3, and the
    // CollectOperator occupies 4.
    ASSERT_EQ(5, opInfo[2].getOperatorId());
    // Verify the stats in checkpoint0.
    for (auto& info : opInfo) {
        auto& stats = info.getStats();
        ASSERT_EQ(0, stats.getInputDocs());
        ASSERT_EQ(0, stats.getOutputDocs());
        ASSERT_EQ(0, stats.getInputBytes());
        ASSERT_EQ(0, stats.getOutputBytes());
        ASSERT_EQ(0, stats.getDlqDocs());
        ASSERT_EQ(0, stats.getStateSize());
    }
    // Verify the stats in checkpoint1, $source.
    opInfo = workload.props()
                 .context->oldCheckpointStorage->readCheckpointInfo(checkpointIds[1])
                 ->getOperatorInfo();
    auto& stats = opInfo[0].getStats();
    ASSERT_EQ(1, stats.getInputDocs());
    ASSERT_EQ(1, stats.getOutputDocs());
    ASSERT_GT(stats.getInputBytes(), 0);
    ASSERT_EQ(0, stats.getDlqDocs());
    ASSERT_EQ(0, stats.getStateSize());
    // Verify the stats in checkpoint2, $source.
    opInfo = workload.props()
                 .context->oldCheckpointStorage->readCheckpointInfo(checkpointIds[2])
                 ->getOperatorInfo();
    stats = opInfo[0].getStats();
    ASSERT_EQ(2, stats.getInputDocs());
    ASSERT_EQ(2, stats.getOutputDocs());
    ASSERT_GT(stats.getInputBytes(), 0);
    ASSERT_EQ(0, stats.getDlqDocs());
    ASSERT_EQ(0, stats.getStateSize());

    // Verify the stats in checkpoint1 and checkpoint2 for the $tumblingWindow.
    // the $tumblingWindow receives checkpoint1 when a window is open.
    // the $tumblingWindow sends checkpoint1+checkpoint2 later when that window has closed.
    // at that time the $tumblingWindow has: output 1 document, received 3 documents,
    // and has one open window.
    for (auto idx : std::vector<size_t>{1, 2}) {
        opInfo = workload.props()
                     .context->oldCheckpointStorage->readCheckpointInfo(checkpointIds[idx])
                     ->getOperatorInfo();
        stats = opInfo[1].getStats();
        ASSERT_EQ(3, stats.getInputDocs());
        ASSERT_EQ(1, stats.getOutputDocs());
        // Bytes are not tracked for $tumblingWindow.
        ASSERT_EQ(0, stats.getInputBytes());
        ASSERT_EQ(0, stats.getOutputBytes());
        ASSERT_EQ(0, stats.getDlqDocs());
        // There should be some state size because there is one open window.
        ASSERT_GT(stats.getStateSize(), 0);
    }

    // Checkpoint3 should not be committed because there is still an open window before it.
    ASSERT(!workload.isCheckpointCommitted(checkpointIds[3]));
}

}  // namespace streams
