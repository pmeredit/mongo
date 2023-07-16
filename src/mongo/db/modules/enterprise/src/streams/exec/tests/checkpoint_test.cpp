/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>
#include <boost/none.hpp>
#include <chrono>
#include <exception>
#include <memory>
#include <rdkafkacpp.h>
#include <string>
#include <vector>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_restore.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/management/stream_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

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
        std::unique_ptr<Executor> executor;
        KafkaConsumerOperator* source;
        InMemorySinkOperator* sink;
        std::unique_ptr<OperatorDag> dag;
        std::unique_ptr<Context> context;
        CheckpointStorage* storage;
        Input input;
        std::vector<BSONObj> userBson;
        std::unique_ptr<MetricManager> metricManager;
    };

    CheckpointTestWorkload(std::string pipeline, Input input, ServiceContext* svcCtx) {
        auto bsonVector = parseBsonVector(pipeline);
        bsonVector.insert(bsonVector.begin(), testKafkaSourceSpec(input.size()));
        bsonVector.push_back(getTestMemorySinkSpec());
        _props.userBson = bsonVector;

        _props.metricManager = std::make_unique<MetricManager>();
        _props.context = getTestContext(nullptr, _props.metricManager.get());
        Parser parser(_props.context.get(), {}, testKafkaConnectionRegistry());
        _props.dag = parser.fromBson(bsonVector);

        _props.context->checkpointStorage = makeCheckpointStorage(svcCtx);
        _props.storage = _props.context->checkpointStorage.get();
        _props.input = std::move(input);

        init();
    }

    CheckpointTestWorkload(std::string pipeline, std::vector<BSONObj> input, ServiceContext* svcCtx)
        : CheckpointTestWorkload(
              pipeline, Input{{0 /* partitionId */, {std::move(input)}}}, svcCtx) {}

    Properties& props() {
        return _props;
    }

    void checkpointAndRun() {
        // In future PRs, this will happen inside Executor.
        auto id = _props.storage->createCheckpointId();
        _props.dag->source()->onControlMsg(
            0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{id}});
        _props.executor->runOnce();
    }

    void runAll() {
        while (_props.executor->runOnce() > 0) {
        }
    }

    std::vector<StreamMsgUnion> getResults() {
        std::vector<StreamMsgUnion> results;
        auto messages = _props.sink->getMessages();
        while (!messages.empty()) {
            results.push_back(messages.front());
            messages.pop();
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
        CheckpointRestore restore(_props.context.get());
        _props.dag =
            restore.createDag(checkpointId, _props.userBson, testKafkaConnectionRegistry());
        restore.restoreFromCheckpoint(_props.dag.get(), checkpointId);
        init();
    }

private:
    void init() {
        invariant(_props.dag && _props.context && _props.metricManager && _props.storage);
        _props.source = dynamic_cast<KafkaConsumerOperator*>(_props.dag->operators().front().get());
        _props.sink = dynamic_cast<InMemorySinkOperator*>(_props.dag->operators().back().get());
        _props.executor = std::make_unique<Executor>(
            Executor::Options{.streamProcessorName = "sp1", .operatorDag = _props.dag.get()});

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
    CheckpointTest() {}

    BSONObj toOriginalBson(Document doc) {
        return doc.toBson().removeField("_stream_meta").removeField("_ts");
    }

protected:
    QueryTestServiceContext _qtServiceContext;
    ServiceContext* _serviceContext{_qtServiceContext.getServiceContext()};
};

TEST_F(CheckpointTest, AlwaysCheckpoint_EmptyPipeline) {
    const int partitionCount = 1;

    std::vector<BSONObj> input = {
        fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839"})"),
        fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839"})"),
        fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:04:20.062839"})"),
        fromjson(R"({"id": 15, "timestamp": "2023-04-10T17:05:20.062839"})"),
        fromjson(R"({"id": 16, "timestamp": "2023-04-10T17:06:20.062839"})"),
    };
    CheckpointTestWorkload workload("[]", input, _serviceContext);
    auto storage = workload.props().storage;

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
        auto checkpointId = storage->readLatestCheckpointId();
        ASSERT(checkpointId);
        ASSERT_NE(lastId, *checkpointId);
        lastId = *checkpointId;

        // Verify the $source operator state in the checkpoint.
        ASSERT_EQ(0, workload.props().source->getOperatorId());
        auto state = storage->readState(
            *checkpointId, workload.props().source->getOperatorId(), /* chunkNumber */ 0);
        ASSERT(state);
        // Verify there is only a single $source state chunk.
        ASSERT_FALSE(storage->readState(
            *checkpointId, workload.props().source->getOperatorId(), /* chunkNumber */ 1));
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
            state = storage->readState(*checkpointId, operatorId, /* chunkNumber */ 0);
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

TEST_F(CheckpointTest, AlwaysCheckpoint_EmptyPipeline_MultiPartition) {
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
    auto storage = workload.props().storage;

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

    // increment the input index for all the partitions.
    // called after each loop execution.
    auto incAfterRun = [&]() {
        stdx::unordered_map<int, size_t> result;
        for (auto& [partition, idx] : inputIdx) {
            result[partition] = idx + input[partition].docsPerChunk;
        }
        inputIdx = result;
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
        for (auto& [partition, options] : input) {
            auto remainingDocs = options.docs.size() - currentIdx(partition);
            auto chunkSize = size_t(options.docsPerChunk);
            result += remainingDocs < chunkSize ? remainingDocs : chunkSize;
        }
        return result;
    };

    // tracks the expected number of docs to output after restoring from
    // each CheckpointId.
    stdx::unordered_map<CheckpointId, int> expectedCountOutputAfterRestore;
    auto docsRemaining = [&]() {
        size_t result = 0;
        for (auto& [partition, options] : input) {
            result += options.docs.size() - currentIdx(partition);
        }
        return result;
    };

    // Run the workload, sending a checkpoint before every document.
    std::vector<CheckpointId> checkpointIds;
    std::vector<BSONObj> fullResults;
    while (inputRemains()) {
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
        auto checkpointId = storage->readLatestCheckpointId();
        ASSERT(checkpointId);

        // Verify the $source operator state in the checkpoint.
        ASSERT_EQ(0, workload.props().source->getOperatorId());
        auto state = storage->readState(
            *checkpointId, workload.props().source->getOperatorId(), /* chunkNumber */ 0);
        ASSERT(state);
        auto sourceState = KafkaSourceCheckpointState::parse(IDLParserContext("test"), *state);
        ASSERT_EQ(input.size(), sourceState.getPartitions().size());

        // Verify the partition state, including log offsets.
        for (size_t partition = 0; partition < sourceState.getPartitions().size(); ++partition) {
            auto& partitionState = sourceState.getPartitions()[partition];
            ASSERT_EQ(partition, partitionState.getPartition());
            ASSERT_EQ(boost::none, partitionState.getWatermark());
            ASSERT_EQ(currentIdx(partition), partitionState.getOffset());
        }

        // Verify no other operators have state in the checkpoint.
        auto it = workload.props().dag->operators().begin() + 1;
        while (it != workload.props().dag->operators().end()) {
            auto operatorId = (*it)->getOperatorId();
            ASSERT_FALSE(storage->readState(*checkpointId, operatorId, 0));
            it = next(it);
        }

        // Save the checkpoint ID for later.
        checkpointIds.push_back(*checkpointId);
        expectedCountOutputAfterRestore[*checkpointId] = docsRemaining();

        incAfterRun();
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

}  // namespace streams
