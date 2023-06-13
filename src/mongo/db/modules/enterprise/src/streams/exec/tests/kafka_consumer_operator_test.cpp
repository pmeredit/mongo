/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>
#include <openssl/bn.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

class KafkaConsumerOperatorTest : public AggregationContextFixture {
public:
    KafkaConsumerOperatorTest();

    void createKafkaConsumerOperator(int32_t numPartitions);

    int32_t runOnce();

    KafkaConsumerOperator::ConsumerInfo& getConsumerInfo(int32_t partition);

    boost::optional<StreamDocument> processSourceDocument(KafkaSourceDocument sourceDoc,
                                                          WatermarkGenerator* watermarkGenerator);

    std::vector<std::vector<BSONObj>> ingestDocs(
        const std::vector<std::vector<int64_t>>& partitionOffsets,
        const std::vector<std::vector<int64_t>>& partitionAppendTimes);

    void verifyDocs(const StreamDataMsg& dataMsg,
                    int32_t expectednumDocs,
                    const std::vector<std::vector<BSONObj>>& expectedOutputDocs,
                    const std::vector<std::vector<int64_t>>& partitionAppendTimes);

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<DocumentTimestampExtractor> _timestampExtractor;
    std::unique_ptr<KafkaConsumerOperator> _source;
};

KafkaConsumerOperatorTest::KafkaConsumerOperatorTest() : _context(getTestContext()) {
    _context->dlq = std::make_unique<NoOpDeadLetterQueue>(NamespaceString{});
}

void KafkaConsumerOperatorTest::createKafkaConsumerOperator(int32_t numPartitions) {
    KafkaConsumerOperator::Options options;
    options.timestampExtractor = _timestampExtractor.get();
    options.timestampOutputFieldName = "_ts";
    options.watermarkCombiner = std::make_unique<WatermarkCombiner>(/*numInputs*/ numPartitions);
    _source = std::make_unique<KafkaConsumerOperator>(_context.get(), std::move(options));

    // Create FakeKafkaPartitionConsumer instances.
    _source->_options.partitionOptions.resize(numPartitions);
    auto watermarkCombiner = _source->_options.watermarkCombiner.get();
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        _source->_options.partitionOptions[partition].watermarkGenerator =
            std::make_unique<DelayedWatermarkGenerator>(
                /*inputIdx*/ partition,
                watermarkCombiner,
                /*allowedLatenessMs*/ 10);
        KafkaConsumerOperator::ConsumerInfo consumerInfo;
        consumerInfo.consumer = std::make_unique<FakeKafkaPartitionConsumer>();
        consumerInfo.watermarkGenerator =
            _source->_options.partitionOptions[partition].watermarkGenerator.get();
        _source->_consumers.push_back(std::move(consumerInfo));
    }
}

int32_t KafkaConsumerOperatorTest::runOnce() {
    return _source->runOnce();
}

KafkaConsumerOperator::ConsumerInfo& KafkaConsumerOperatorTest::getConsumerInfo(int32_t partition) {
    return _source->_consumers.at(partition);
}

boost::optional<StreamDocument> KafkaConsumerOperatorTest::processSourceDocument(
    KafkaSourceDocument sourceDoc, WatermarkGenerator* watermarkGenerator) {
    return _source->processSourceDocument(std::move(sourceDoc), watermarkGenerator);
}

std::vector<std::vector<BSONObj>> KafkaConsumerOperatorTest::ingestDocs(
    const std::vector<std::vector<int64_t>>& partitionOffsets,
    const std::vector<std::vector<int64_t>>& partitionAppendTimes) {
    std::vector<std::vector<BSONObj>> expectedOutputDocs;
    expectedOutputDocs.resize(partitionOffsets.size());
    for (size_t partition = 0; partition < partitionOffsets.size(); ++partition) {
        auto partitionConsumer =
            dynamic_cast<FakeKafkaPartitionConsumer*>(getConsumerInfo(partition).consumer.get());
        std::vector<KafkaSourceDocument> sourceDocs;
        int32_t numPartitionDocs = partitionOffsets[partition].size();
        for (int32_t i = 0; i < numPartitionDocs; ++i) {
            KafkaSourceDocument sourceDoc;
            sourceDoc.doc = fromjson(fmt::format("{{partition: {}}}", partition));
            sourceDoc.partition = partition;
            sourceDoc.offset = partitionOffsets[partition][i];
            sourceDoc.logAppendTimeMs = partitionAppendTimes[partition][i];
            BSONObjBuilder outputDocBuilder(*sourceDoc.doc);
            outputDocBuilder << "_ts" << Date_t::fromMillisSinceEpoch(*sourceDoc.logAppendTimeMs);
            outputDocBuilder << "_stream_meta"
                             << BSON("sourceType"
                                     << "kafka"
                                     << "sourcePartition" << sourceDoc.partition << "sourceOffset"
                                     << sourceDoc.offset << "timestamp"
                                     << Date_t::fromMillisSinceEpoch(*sourceDoc.logAppendTimeMs));
            expectedOutputDocs[partition].push_back(outputDocBuilder.obj());
            sourceDocs.push_back(std::move(sourceDoc));
        }
        partitionConsumer->addDocuments(std::move(sourceDocs));
    }
    return expectedOutputDocs;
}

void KafkaConsumerOperatorTest::verifyDocs(
    const StreamDataMsg& dataMsg,
    int32_t expectednumDocs,
    const std::vector<std::vector<BSONObj>>& expectedOutputDocs,
    const std::vector<std::vector<int64_t>>& partitionAppendTimes) {
    ASSERT_EQUALS(expectednumDocs, dataMsg.docs.size());
    std::vector<int32_t> nextDocIndexes = {0, 0};
    for (int32_t i = 0; i < expectednumDocs; ++i) {
        auto& streamDoc = dataMsg.docs[i];
        auto& doc = streamDoc.doc;
        int32_t partition = doc.getField("partition").getInt();
        int32_t docIdx = nextDocIndexes[partition]++;
        ASSERT_BSONOBJ_EQ(expectedOutputDocs[partition][docIdx], doc.toBson());
        ASSERT_EQUALS(partitionAppendTimes[partition][docIdx], streamDoc.minEventTimestampMs);
        ASSERT_EQUALS(partitionAppendTimes[partition][docIdx], streamDoc.maxEventTimestampMs);
    }
};

namespace {

WatermarkControlMsg createWatermarkControlMsg(int64_t watermark) {
    WatermarkControlMsg msg;
    msg.eventTimeWatermarkMs = watermark;
    return msg;
}

TEST_F(KafkaConsumerOperatorTest, Basic) {
    createKafkaConsumerOperator(/*numPartitions*/ 2);

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    // Test that runOnce() does not emit any documents yet, but emits a control message.
    ASSERT_EQUALS(0, runOnce());
    std::queue<StreamMsgUnion> msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop();
    ASSERT_FALSE(msgUnion.dataMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(0), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(0),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(0),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());

    // Test that runOnce() does not emit anything now, not even a control message.
    ASSERT_EQUALS(0, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(0, msgs.size());

    // Consume 5 docs each from 2 partitions.
    std::vector<std::vector<int64_t>> partitionOffsets = {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}};
    std::vector<std::vector<int64_t>> partitionAppendTimes = {{1, 2, 3, 4, 5}, {5, 10, 15, 20, 25}};
    auto expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    int32_t numDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that no control message was emitted as the watermark has not changed since the last
    // runOnce() call.
    // The reason the watermark has not changed is that the watermark of partition 0 is still less
    // than allowedLatenessMs of 10.
    ASSERT_FALSE(msgUnion.controlMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(0),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(25 - 10 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());

    // Consume 10 docs each from 2 partitions.
    partitionOffsets = {{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
                        {21, 22, 23, 24, 25, 26, 27, 28, 29, 30}};
    partitionAppendTimes = {{6, 7, 8, 9, 10, 12, 12, 12, 15, 15},
                            {30, 35, 40, 45, 50, 55, 60, 65, 70, 75}};
    expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    numDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that the control message is as expected.
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 10 - 1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 10 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(75 - 10 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());

    // Consume 1 doc from partition 0 to advance the watermark.
    partitionOffsets = {{21}, {}};
    partitionAppendTimes = {{60}, {}};
    expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    numDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that the control message is as expected.
    ASSERT_EQUALS(createWatermarkControlMsg(60 - 10 - 1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(60 - 10 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(75 - 10 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
}

TEST_F(KafkaConsumerOperatorTest, ProcessSourceDocument) {
    boost::intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest{});
    auto exprObj = fromjson("{$toDate: '$event_time_ms'}");
    auto expr = Expression::parseExpression(expCtx.get(), exprObj, expCtx->variablesParseState);
    _context->dlq = std::make_unique<InMemoryDeadLetterQueue>(NamespaceString{});
    auto inMemoryDeadLetterQueue = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    _timestampExtractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);

    createKafkaConsumerOperator(/*numPartitions*/ 2);

    // Test that processSourceDocument() works as expected when timestamp can be extracted
    // successfully.
    KafkaSourceDocument sourceDoc;
    sourceDoc.doc = fromjson("{partition: 0, event_time_ms: 1677876150055}");
    BSONObjBuilder outputDocBuilder(*sourceDoc.doc);
    outputDocBuilder << "_ts" << Date_t::fromMillisSinceEpoch(1677876150055);
    auto expectedOutputObj = outputDocBuilder.obj();
    auto streamDoc =
        processSourceDocument(std::move(sourceDoc), getConsumerInfo(0).watermarkGenerator);
    ASSERT_BSONOBJ_EQ(expectedOutputObj, streamDoc->doc.toBson());
    ASSERT_EQUALS(1677876150055, streamDoc->minEventTimestampMs);
    ASSERT_EQUALS(1677876150055, streamDoc->maxEventTimestampMs);

    // Test that processSourceDocument() works as expected when timestamp cannot be extracted
    // successfully.
    sourceDoc = KafkaSourceDocument{};
    sourceDoc.doc = fromjson("{partition: 0}");
    ASSERT_FALSE(
        processSourceDocument(std::move(sourceDoc), getConsumerInfo(0).watermarkGenerator));

    // Verify that the previous document was added to the DLQ.
    auto dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQUALS(1, dlqMsgs.size());
    auto dlqDoc = std::move(dlqMsgs.front());
    dlqMsgs.pop();
    ASSERT_BSONOBJ_EQ(fromjson("{partition: 0}"), dlqDoc["fullDocument"].Obj());
    ASSERT_TRUE(dlqDoc["errInfo"]["reason"].String().starts_with(
        "Failed to process input document with error"));

    // Test that processSourceDocument() works as expected when the source document was not parsed
    // successfully.
    sourceDoc = KafkaSourceDocument{};
    sourceDoc.error = "synthetic error";
    ASSERT_FALSE(
        processSourceDocument(std::move(sourceDoc), getConsumerInfo(0).watermarkGenerator));

    // Verify that the previous document was added to the DLQ.
    dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQUALS(1, dlqMsgs.size());
    dlqDoc = std::move(dlqMsgs.front());
    dlqMsgs.pop();
    ASSERT_EQUALS("synthetic error", dlqDoc["errInfo"]["reason"].String());
}

TEST_F(KafkaConsumerOperatorTest, DropLateDocuments) {
    _context->dlq = std::make_unique<InMemoryDeadLetterQueue>(NamespaceString{});
    auto inMemoryDeadLetterQueue = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    createKafkaConsumerOperator(/*numPartitions*/ 2);

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    // Consume 5 docs each from 2 partitions. Let the last 2 docs from partition 0 be late
    // (i.e. partitionAppendTime is <= the partition 0 watermark).
    std::vector<std::vector<int64_t>> partitionOffsets = {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}};
    std::vector<std::vector<int64_t>> partitionAppendTimes = {{1, 2, 15, 3, 4},
                                                              {5, 10, 15, 20, 25}};
    auto expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    // Remove the state for the 2 late docs on partition 0.
    std::vector<BSONObj> lateDocs(expectedOutputDocs[0].begin() + 3, expectedOutputDocs[0].end());
    expectedOutputDocs[0].resize(3);
    partitionOffsets[0].resize(3);
    partitionAppendTimes[0].resize(3);
    int32_t numDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numDocs, runOnce());
    auto msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop();

    // Test that the output docs are as expected and thus verify that the 2 late docs were dropeed.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that the control message is as expected.
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 10 - 1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 10 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(25 - 10 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());

    // Verify that the 2 late docs were added to the DLQ.
    auto dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQUALS(lateDocs.size(), dlqMsgs.size());
    for (size_t i = 0; i < lateDocs.size(); ++i) {
        auto dlqDoc = std::move(dlqMsgs.front());
        dlqMsgs.pop();
        ASSERT_BSONOBJ_EQ(lateDocs[i].removeField("_stream_meta"), dlqDoc["fullDocument"].Obj());
        ASSERT_EQ("Input document arrived late", dlqDoc["errInfo"]["reason"].String());
    }

    // Consume 5 docs each from 2 partitions. Let the first 2 docs from partition 1 be late
    // (i.e. partitionAppendTime is <= the partition 1 watermark).
    partitionOffsets = {{11, 12, 13, 14, 15}, {21, 22, 23, 24, 25}};
    partitionAppendTimes = {{5, 6, 7, 8, 9}, {12, 13, 15, 30, 35}};
    expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    // Remove the state for the 2 late docs on partition 1.
    lateDocs =
        std::vector<BSONObj>(expectedOutputDocs[1].begin(), expectedOutputDocs[1].begin() + 2);
    expectedOutputDocs[1].erase(expectedOutputDocs[1].begin(), expectedOutputDocs[1].begin() + 2);
    partitionOffsets[1].erase(partitionOffsets[1].begin(), partitionOffsets[1].begin() + 2);
    partitionAppendTimes[1].erase(partitionAppendTimes[1].begin(),
                                  partitionAppendTimes[1].begin() + 2);
    numDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that no control message was emitted as the watermark has not changed since the last
    // runOnce() call.
    ASSERT_FALSE(msgUnion.controlMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 10 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(35 - 10 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());

    // Verify that the 2 late docs were added to the DLQ.
    dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQUALS(lateDocs.size(), dlqMsgs.size());
    for (size_t i = 0; i < lateDocs.size(); ++i) {
        auto dlqDoc = std::move(dlqMsgs.front());
        dlqMsgs.pop();
        ASSERT_BSONOBJ_EQ(lateDocs[i].removeField("_stream_meta"), dlqDoc["fullDocument"].Obj());
        ASSERT_EQ("Input document arrived late", dlqDoc["errInfo"]["reason"].String());
    }
}

}  // namespace
}  // namespace streams
