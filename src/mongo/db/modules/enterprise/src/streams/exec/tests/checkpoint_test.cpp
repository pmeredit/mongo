/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include <algorithm>
#include <boost/none.hpp>
#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <rdkafkacpp.h>
#include <vector>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/periodic_runner_factory.h"
#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/concurrent_checkpoint_monitor.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
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
        std::filesystem::path checkpointWriteDir;
    };

    CheckpointTestWorkload(std::string pipeline, Input input, ServiceContext* svcCtx) {
        auto bsonVector = parseBsonVector(pipeline);
        bsonVector.insert(bsonVector.begin(), testKafkaSourceSpec(input.size()));
        bsonVector.push_back(getTestMemorySinkSpec());
        _props.userBson = bsonVector;

        _props.metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Executor> executor;
        std::tie(_props.context, executor) =
            getTestContext(nullptr, UUID::gen().toString(), UUID::gen().toString());

        _props.context->tenantId = UUID::gen().toString();
        _props.context->streamProcessorId = UUID::gen().toString();

        _props.checkpointWriteDir = std::filesystem::path{"/tmp/streams_checkpoint_test/write"} /
            _props.context->tenantId / _props.context->streamProcessorId;

        _props.context->checkpointStorage = std::make_unique<LocalDiskCheckpointStorage>(
            LocalDiskCheckpointStorage::Options{.writeRootDir = _props.checkpointWriteDir,
                                                .restoreRootDir = std::filesystem::path{},
                                                .hostName = "test",
                                                .userPipeline = _props.userBson},
            _props.context.get());

        _props.context->checkpointStorage->registerMetrics(executor->getMetricManager());
        _props.context->dlq = std::make_unique<NoOpDeadLetterQueue>(_props.context.get());
        _props.context->connections = testKafkaConnectionRegistry();
        Planner planner(_props.context.get(), {});
        _props.dag = planner.plan(bsonVector);
        _props.context->executionPlan = _props.dag->optimizedPipeline();

        CheckpointCoordinator::Options coordinatorOptions{
            .processorId = _props.context->streamProcessorId,
            .storage = _props.context->checkpointStorage.get(),
            .checkpointController = _props.context->concurrentCheckpointController};
        _props.checkpointCoordinator =
            std::make_unique<CheckpointCoordinator>(std::move(coordinatorOptions));

        _props.input = std::move(input);

        init();
    }

    CheckpointTestWorkload(std::string pipeline, std::vector<BSONObj> input, ServiceContext* svcCtx)
        : CheckpointTestWorkload(
              pipeline, Input{{0 /* partitionId */, {std::move(input)}}}, svcCtx) {}

    Properties& props() {
        return _props;
    }

    CheckpointControlMsg checkpointAndRun() {
        // Create a checkpoint and send it through the operator dag.
        auto checkpointControlMsg = _props.checkpointCoordinator->getCheckpointControlMsgIfReady(
            CheckpointCoordinator::CheckpointRequest{.writeCheckpointCommand =
                                                         WriteCheckpointCommand::kForce});
        _props.executor->sendCheckpointControlMsg(*checkpointControlMsg);
        _props.executor->runOnce();
        return *checkpointControlMsg;
    }

    // Write a checkpoint, restore the operators from it, then call runOnce.
    auto checkpointRestoreAndRun() {
        // Create a checkpoint and send it through the operator dag.
        auto checkpointControlMsg = _props.checkpointCoordinator->getCheckpointControlMsgIfReady(
            CheckpointCoordinator::CheckpointRequest{.writeCheckpointCommand =
                                                         WriteCheckpointCommand::kForce});
        _props.source->onControlMsg(0 /* inputIdx */,
                                    StreamControlMsg{.checkpointMsg = *checkpointControlMsg});

        // Restore the operators from the checkpoint. These re-created operators will
        // be used in the runOnce call below.
        startCheckpointRestore(checkpointControlMsg->id);

        Planner planner{props().context.get(), Planner::Options{.shouldOptimize = false}};
        auto checkpointId = getLatestCommittedCheckpointId();
        auto dag = planner.plan(props().userBson);
        props().dag = std::move(dag);
        init();

        // Verify the expected context properties are set after checkpoint restore.
        ASSERT(props().context->restoredCheckpointInfo->operatorInfo);
        ASSERT(props().context->restoredCheckpointInfo->pipelineVersion >= 0);
        ASSERT(!props().context->restoredCheckpointInfo->userPipeline.empty());
        ASSERT(props().context->restoredCheckpointInfo->summaryStats);
        props().context->checkpointStorage->checkpointRestored(*checkpointId);

        auto checkpointOpInfo = *props().context->restoredCheckpointInfo->operatorInfo;

        _props.executor->runOnce();
        return std::make_tuple(*checkpointControlMsg, std::move(checkpointOpInfo));
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
        startCheckpointRestore(checkpointId);

        _props.context->restoreCheckpointId = checkpointId;
        _props.context->connections = testKafkaConnectionRegistry();

        Planner::Options plannerOptions{.shouldOptimize = false};
        Planner planner(_props.context.get(), plannerOptions);
        _props.dag = planner.plan(_props.context->executionPlan);

        init();

        props().context->checkpointStorage->checkpointRestored(checkpointId);
    }

    boost::optional<CheckpointId> getLatestCommittedCheckpointId() {
        std::vector<CheckpointId> checkpoints;
        for (const auto& dirEntry :
             std::filesystem::recursive_directory_iterator(_props.checkpointWriteDir)) {
            if (dirEntry.path().filename() == "MANIFEST") {
                auto checkpointPath = dirEntry.path().parent_path();
                auto checkpointIdStr = checkpointPath.filename().string();
                checkpoints.push_back(CheckpointId{std::stoll(checkpointIdStr)});
            }
        }

        std::sort(checkpoints.begin(), checkpoints.end(), std::greater<>());
        if (checkpoints.empty()) {
            return boost::none;
        }
        return checkpoints[0];
    }

    bool isCheckpointCommitted(CheckpointId id) {
        return std::filesystem::exists(std::filesystem::path(_props.checkpointWriteDir) /
                                       std::to_string(id) / "MANIFEST");
    }

    void startCheckpointRestore(CheckpointId checkpointId) {
        props().context->executionPlan = {};
        auto localDisk =
            dynamic_cast<LocalDiskCheckpointStorage*>(props().context->checkpointStorage.get());
        localDisk->_opts.restoreRootDir = props().checkpointWriteDir / std::to_string(checkpointId);
        localDisk->populateManifestInfo(localDisk->_opts.restoreRootDir / "MANIFEST");
        props().context->restoredCheckpointInfo = localDisk->startCheckpointRestore(checkpointId);
        props().context->restoreCheckpointId = checkpointId;
        props().context->executionPlan = props().context->restoredCheckpointInfo->executionPlan;
    }

    boost::optional<BSONObj> getSingleState(CheckpointId checkpointId, OperatorId operatorId) {
        auto reader =
            _props.context->checkpointStorage->createStateReader(checkpointId, operatorId);
        auto result = _props.context->checkpointStorage->getNextRecord(reader.get());
        ASSERT_FALSE(_props.context->checkpointStorage->getNextRecord(reader.get()));
        if (result) {
            return result->toBson();
        }
        return boost::none;
    }

    void init() {
        invariant(_props.dag && _props.context && _props.metricManager &&
                  (_props.context->checkpointStorage));
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
        int32_t totalDocsPerChunk{0};
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
                sourceDocs.push_back(
                    KafkaSourceDocument{.doc = doc, .messageSizeBytes = doc.objsize()});
            }
            totalDocsPerChunk += input.docsPerChunk;
            consumer->_docsPerChunk = input.docsPerChunk;
            consumer->_overrideOffsets = true;
            consumer->addDocuments(std::move(sourceDocs));
        }
        _props.source->_testOnlyDataMsgMaxDocSize = totalDocsPerChunk;
    }

private:
    Properties _props;
};

class CheckpointTest : public AggregationContextFixture {
public:
    CheckpointTest() {
        // Set up the periodic runner to allow background job execution for tests that require
        // it.
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
    void Test_CoordinatorGetCheckpointControlMsgIfReady();

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
    CheckpointTestWorkload workload("[]", input, _serviceContext);

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

        workload.startCheckpointRestore(*checkpointId);

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
            ASSERT_THROWS_CODE(
                workload.getSingleState(*checkpointId, operatorId), DBException, 7863423);
            it = next(it);
        }

        workload.props().context->checkpointStorage->checkpointRestored(*checkpointId);

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
    CheckpointTestWorkload workload("[]", input, _serviceContext);

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
        int32_t result = 0;
        int64_t currPartition = 0;
        int64_t totalRemainingDocs = 0;
        int32_t testOnlyDataMsgMaxDocSize = 5;

        for (const auto& [partition, options] : input) {
            totalRemainingDocs += options.docs.size() - currentIdx(partition);
        }

        while (result < testOnlyDataMsgMaxDocSize && totalRemainingDocs > 0) {
            const auto& options = input[currPartition];
            auto remainingDocs = options.docs.size() - currentIdx(currPartition);
            auto chunkSize = size_t(options.docsPerChunk);
            auto expectedNumDocs = remainingDocs < chunkSize ? remainingDocs : chunkSize;

            if (expectedNumDocs > 0) {
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
            int64_t offset = *doc.streamMeta.getSource()->getOffset();
            int32_t partition = *doc.streamMeta.getSource()->getPartition();
            ASSERT_BSONOBJ_EQ(toOriginalBson(doc.doc), input[partition].docs[offset]);
        }

        // Verify the valid committed checkpointId in checkpoint storage.
        auto checkpointId = workload.getLatestCommittedCheckpointId();
        ASSERT(checkpointId);

        workload.startCheckpointRestore(*checkpointId);

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
            ASSERT_THROWS_CODE(
                workload.getSingleState(*checkpointId, operatorId), DBException, 7863423);
            it = next(it);
        }

        // Save the checkpoint ID for later.
        checkpointIds.push_back(*checkpointId);
        expectedCountOutputAfterRestore[*checkpointId] = docsRemaining(inputIdxSnapshot);

        workload.props().context->checkpointStorage->checkpointRestored(*checkpointId);
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
            int64_t offset = *doc.streamMeta.getSource()->getOffset();
            int32_t partition = *doc.streamMeta.getSource()->getPartition();
            ASSERT_BSONOBJ_EQ(toOriginalBson(doc.doc), input[partition].docs[offset]);
        }
    }
}

TEST_F(CheckpointTest, AlwaysCheckpoint_EmptyPipeline_MultiPartition) {
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
        CheckpointTestWorkload workload("[]", std::vector<BSONObj>{}, _serviceContext);
        auto metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Context> context;
        std::unique_ptr<Executor> executor;
        std::tie(context, executor) = getTestContext(_serviceContext);
        auto storage = std::make_unique<InMemoryCheckpointStorage>(context.get());
        storage->registerMetrics(executor->getMetricManager());
        auto coordinator = std::make_unique<CheckpointCoordinator>(CheckpointCoordinator::Options{
            .processorId = "",
            .writeFirstCheckpoint = false,
            .checkpointIntervalMs = spec.checkpointInterval,
            .storage = storage.get(),
            .checkpointController = context->concurrentCheckpointController});
        auto start = stdx::chrono::steady_clock::now();
        std::vector<CheckpointId> checkpoints;
        while (stdx::chrono::steady_clock::now() - start < spec.runtime) {
            auto checkpointMsg = coordinator->getCheckpointControlMsgIfReady(
                CheckpointCoordinator::CheckpointRequest{.uncheckpointedState = true});
            if (checkpointMsg) {
                checkpoints.push_back(checkpointMsg->id);
                context->concurrentCheckpointController->onCheckpointComplete();
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

typedef void (*CoordinatorTestCase)(
    std::shared_ptr<CheckpointCoordinator> coordinator,
    std::shared_ptr<ConcurrentCheckpointController> CheckpointController);

void CheckpointTest::Test_CoordinatorGetCheckpointControlMsgIfReady() {
    struct Spec {
        milliseconds checkpointInterval{0};
        bool writeFirstCheckpoint{false};
        bool enableDataFlow{true};
        std::function<void(std::shared_ptr<CheckpointCoordinator> coordinator,
                           std::shared_ptr<ConcurrentCheckpointController> CheckpointController)>
            tc;
    };

    auto runTestCase = [&](Spec spec) {
        CheckpointTestWorkload workload("[]", std::vector<BSONObj>{}, _serviceContext);
        auto metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Context> context;
        std::unique_ptr<Executor> executor;
        std::tie(context, executor) = getTestContext(_serviceContext);
        auto storage = std::make_unique<InMemoryCheckpointStorage>(context.get());
        storage->registerMetrics(executor->getMetricManager());
        auto coordinator = std::make_shared<CheckpointCoordinator>(CheckpointCoordinator::Options{
            .processorId = "",
            .enableDataFlow = spec.enableDataFlow,
            .writeFirstCheckpoint = spec.writeFirstCheckpoint,
            .checkpointIntervalMs = spec.checkpointInterval,
            .storage = storage.get(),
            .checkpointController = context->concurrentCheckpointController});

        spec.tc(coordinator, context->concurrentCheckpointController);
    };

    auto firstCheckpointTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            // ensure that checkpoint is blocked for workload
            ASSERT_TRUE(CheckpointController->startNewCheckpointIfRoom(false));

            bool checkpointCompleted = false;
            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.uncheckpointedState = true})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_TRUE(checkpointCompleted);

            CheckpointController->onCheckpointComplete();
        };

    runTestCase(Spec{.writeFirstCheckpoint = true, .tc = firstCheckpointTc});

    auto iterativeCheckpointNotTakenTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            ASSERT_TRUE(CheckpointController->startNewCheckpointIfRoom(false));

            bool checkpointCompleted = false;
            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.uncheckpointedState = true})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_FALSE(checkpointCompleted);

            CheckpointController->onCheckpointComplete();
        };

    runTestCase(Spec{.tc = iterativeCheckpointNotTakenTc});

    auto iterativeCheckpointTakenTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.uncheckpointedState = true})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_TRUE(checkpointCompleted);
        };

    runTestCase(Spec{.tc = iterativeCheckpointTakenTc});

    auto iterativeCheckpointTakenWhenRoomBecomesAvailableTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            ASSERT_TRUE(CheckpointController->startNewCheckpointIfRoom(false));
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.uncheckpointedState = true})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_FALSE(checkpointCompleted);

            CheckpointController->onCheckpointComplete();

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.uncheckpointedState = true})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_TRUE(checkpointCompleted);
        };

    runTestCase(Spec{.tc = iterativeCheckpointTakenWhenRoomBecomesAvailableTc});

    auto checkpointFromShutdownTakenTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.shutdown = true})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_TRUE(checkpointCompleted);
        };

    runTestCase(Spec{.tc = checkpointFromShutdownTakenTc});

    auto checkpointFromForceCommandTakenTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{.writeCheckpointCommand =
                                                                 WriteCheckpointCommand::kForce})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_TRUE(checkpointCompleted);
        };

    runTestCase(Spec{.tc = checkpointFromForceCommandTakenTc});

    auto checkpointFromNormalCommandTakenTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{
                        .uncheckpointedState = true,
                        .writeCheckpointCommand = WriteCheckpointCommand::kNormal})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_TRUE(checkpointCompleted);
        };

    runTestCase(Spec{.tc = checkpointFromNormalCommandTakenTc});

    auto checkpointSkippedFromNothingChangedTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_FALSE(checkpointCompleted);
        };

    runTestCase(Spec{.tc = checkpointSkippedFromNothingChangedTc});

    auto checkpointSkippedFromInsufficentTimeElapseTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_FALSE(checkpointCompleted);
        };

    runTestCase(Spec{.checkpointInterval = milliseconds{10000},
                     .tc = checkpointSkippedFromInsufficentTimeElapseTc});

    auto checkpointSkippedFromDisabledDataFlowTc =
        [&](std::shared_ptr<CheckpointCoordinator> coordinator,
            std::shared_ptr<ConcurrentCheckpointController> CheckpointController) {
            bool checkpointCompleted = false;

            if (coordinator->getCheckpointControlMsgIfReady(
                    CheckpointCoordinator::CheckpointRequest{})) {
                CheckpointController->onCheckpointComplete();
                checkpointCompleted = true;
            }

            ASSERT_FALSE(checkpointCompleted);
        };

    runTestCase(Spec{.enableDataFlow = false, .tc = checkpointSkippedFromDisabledDataFlowTc});
}


TEST_F(CheckpointTest, CoordinatorGetCheckpointControlMsgIfReady) {
    Test_CoordinatorGetCheckpointControlMsgIfReady();
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

    // Initialize the expectedStats.
    std::map<OperatorId, OperatorStats> expectedStats;
    for (auto& op : workload.props().dag->operators()) {
        expectedStats[op->getOperatorId()] = op->getStats();
    }

    for (size_t idx = 0; idx < input.size() + 1; ++idx) {
        // Checkpoint, restore from it, then send a single document through the DAG.
        auto [checkpointMsg, checkpointOpInfo] = workload.checkpointRestoreAndRun();

        // Verify the valid committed checkpointId in checkpoint storage.
        auto checkpointId = checkpointMsg.id;
        ASSERT_NE(lastId, checkpointId);
        lastId = checkpointId;
        checkpointIds.push_back(checkpointId);

        // Get the operator stats after this checkpointAndRun.
        std::map<OperatorId, OperatorStats> statsFromThisRun;
        for (auto& op : workload.props().dag->operators()) {
            statsFromThisRun[op->getOperatorId()] = op->getStats().getAdditiveStats();
        }

        // Verify the stats in the checkpoint for each operator.
        ASSERT_EQ(expectedStats.size(), checkpointOpInfo.size());
        for (auto& op : checkpointOpInfo) {
            auto operatorId = op.getOperatorId();
            auto& expected = expectedStats[operatorId];
            auto& stat = op.getStats();
            ASSERT_EQ(expected.numInputBytes, stat.getInputBytes());
            ASSERT_EQ(expected.numInputDocs, stat.getInputDocs());
            ASSERT_EQ(expected.numOutputBytes, stat.getOutputBytes());
            ASSERT_EQ(expected.numOutputDocs, stat.getOutputDocs());
        }

        // Increment the next iteration's expectedStats by this iteration's stats.
        for (const auto& [opId, stats] : statsFromThisRun) {
            expectedStats[opId] += stats.getAdditiveStats();
        }
    }
    // Verify the expected number of checkpoints.
    ASSERT_EQ(input.size() + 1, checkpointIds.size());
    // Verify the stats in the last checkpoint after all the input.
    workload.startCheckpointRestore(*workload.getLatestCommittedCheckpointId());
    auto stats = *workload.props().context->restoredCheckpointInfo->operatorInfo;
    workload.props().context->checkpointStorage->checkpointRestored(
        *workload.getLatestCommittedCheckpointId());

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

    workload.startCheckpointRestore(checkpointIds[0]);
    auto opInfo = *workload.props().context->restoredCheckpointInfo->operatorInfo;
    workload.props().context->checkpointStorage->checkpointRestored(checkpointIds[0]);
    ASSERT_EQ(4, opInfo.size());
    // Verify the operator IDs.
    // The $source.
    ASSERT_EQ(0, opInfo[0].getOperatorId());
    // The $group.
    ASSERT_EQ(1, opInfo[1].getOperatorId());
    // The $sort+$limit.
    ASSERT_EQ(2, opInfo[2].getOperatorId());
    // The sink.
    ASSERT_EQ(3, opInfo[3].getOperatorId());
    // Verify the stats in checkpoint0.
    for (auto& info : opInfo) {
        auto& stats = info.getStats();
        ASSERT_EQ(0, stats.getInputDocs());
        ASSERT_EQ(0, stats.getOutputDocs());
        ASSERT_EQ(0, stats.getInputBytes());
        ASSERT_EQ(0, stats.getOutputBytes());
        ASSERT_EQ(0, stats.getDlqDocs());
    }
    // Verify the stats in checkpoint1, $source.
    workload.startCheckpointRestore(checkpointIds[1]);
    opInfo = *workload.props().context->restoredCheckpointInfo->operatorInfo;
    workload.props().context->checkpointStorage->checkpointRestored(checkpointIds[1]);

    auto stats = opInfo[0].getStats();
    ASSERT_EQ(1, stats.getInputDocs());
    ASSERT_EQ(1, stats.getOutputDocs());
    ASSERT_GT(stats.getInputBytes(), 0);
    ASSERT_EQ(0, stats.getDlqDocs());
    // Verify the stats in checkpoint2, $source.
    workload.startCheckpointRestore(checkpointIds[2]);
    opInfo = *workload.props().context->restoredCheckpointInfo->operatorInfo;
    workload.props().context->checkpointStorage->checkpointRestored(checkpointIds[2]);
    stats = opInfo[0].getStats();
    ASSERT_EQ(2, stats.getInputDocs());
    ASSERT_EQ(2, stats.getOutputDocs());
    ASSERT_GT(stats.getInputBytes(), 0);
    ASSERT_EQ(0, stats.getDlqDocs());

    // Verify the stats in checkpoint1 and checkpoint2 for the $group, for checkpoints 1 and 2.
    // During each of these checkpoints there is one open window.
    for (auto idx : std::vector<size_t>{1, 2}) {
        workload.startCheckpointRestore(checkpointIds[idx]);
        opInfo = *workload.props().context->restoredCheckpointInfo->operatorInfo;
        workload.props().context->checkpointStorage->checkpointRestored(checkpointIds[idx]);

        stats = opInfo[1].getStats();
        ASSERT_EQ(idx, stats.getInputDocs());
        ASSERT_EQ(0, stats.getOutputDocs());
        // Bytes are not tracked for $tumblingWindow.
        ASSERT_EQ(0, stats.getInputBytes());
        ASSERT_EQ(0, stats.getOutputBytes());
        // No DLQ docs are expected.
        ASSERT_EQ(0, stats.getDlqDocs());
    }

    // Verify the stats in checkpoint3.
    workload.startCheckpointRestore(checkpointIds[3]);
    opInfo = *workload.props().context->restoredCheckpointInfo->operatorInfo;
    workload.props().context->checkpointStorage->checkpointRestored(checkpointIds[3]);
    stats = opInfo[1].getStats();
    ASSERT_EQ(3, stats.getInputDocs());
    ASSERT_EQ(1, stats.getOutputDocs());

    // All checkpoints should be committed.
    ASSERT(workload.isCheckpointCommitted(checkpointIds[3]));
}

}  // namespace streams
