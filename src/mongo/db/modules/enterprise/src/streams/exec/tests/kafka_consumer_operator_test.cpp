/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>
#include <memory>
#include <openssl/bn.h>
#include <rdkafkacpp.h>

#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/kafka_utils.h"
#include "streams/exec/message.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"
#include "streams/util/units.h"

namespace streams {

using namespace mongo;

class KafkaConsumerOperatorTest : public AggregationContextFixture {
public:
    KafkaConsumerOperatorTest();

    KafkaConsumerOperator::Options makeOptions(int32_t numPartitions) const;
    KafkaConsumerOperator::Options makeOptions(
        std::vector<KafkaConsumerOperator::TopicPartition> topicPartitions) const;
    void createKafkaConsumerOperator(KafkaConsumerOperator::Options options);

    void disableOverrideOffsets(int32_t numPartitions);

    int32_t runOnce();

    int32_t getNumConsumers() const;

    KafkaConsumerOperator::ConsumerInfo& getConsumerInfo(int32_t partition,
                                                         KafkaConsumerOperator* source);

    KafkaConsumerOperator::ConsumerInfo& getConsumerInfo(int32_t partition);

    const WatermarkControlMsg& getCombinedWatermarkMsg();

    boost::optional<StreamDocument> processSourceDocument(KafkaSourceDocument sourceDoc,
                                                          WatermarkGenerator* watermarkGenerator);

    std::vector<std::vector<BSONObj>> ingestDocs(
        const std::vector<std::vector<int64_t>>& partitionOffsets,
        const std::vector<std::vector<int64_t>>& partitionAppendTimes);

    void verifyDocs(const StreamDataMsg& dataMsg,
                    int32_t expectednumDocs,
                    const std::vector<std::vector<BSONObj>>& expectedOutputDocs,
                    const std::vector<std::vector<int64_t>>& partitionAppendTimes);

    int32_t getRunOnceMaxDocs(KafkaConsumerOperator* source);

    std::vector<KafkaConsumerOperator::TopicPartition> getTopicPartitions() const;
    boost::optional<int32_t> getPartitionIdx(const std::string& topic, int32_t partition) const;

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    std::unique_ptr<DocumentTimestampExtractor> _timestampExtractor;
    std::unique_ptr<KafkaConsumerOperator> _source;
    std::unique_ptr<JsonEventDeserializer> _deserializer;
    std::unique_ptr<Executor> _executor;
};

KafkaConsumerOperatorTest::KafkaConsumerOperatorTest() {
    _metricManager = std::make_unique<MetricManager>();
    std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
    _deserializer = std::make_unique<JsonEventDeserializer>();
    _context->dlq->registerMetrics(_executor->getMetricManager());
}

KafkaConsumerOperator::Options KafkaConsumerOperatorTest::makeOptions(int32_t numPartitions) const {
    KafkaConsumerOperator::Options options;
    options.timestampExtractor = _timestampExtractor.get();
    options.timestampOutputFieldName = "_ts";
    options.useWatermarks = true;
    options.isTest = true;
    options.topicNames = {"unitTestKafkaTopic"};
    for (int i = 0; i < numPartitions; i++) {
        options.testOnlyTopicPartitions.emplace_back(options.topicNames[0], i);
    }
    return options;
}

KafkaConsumerOperator::Options KafkaConsumerOperatorTest::makeOptions(
    std::vector<KafkaConsumerOperator::TopicPartition> topicPartitions) const {
    KafkaConsumerOperator::Options options;
    options.timestampExtractor = _timestampExtractor.get();
    options.timestampOutputFieldName = "_ts";
    options.useWatermarks = true;
    options.isTest = true;
    options.testOnlyTopicPartitions = std::move(topicPartitions);
    return options;
}


void KafkaConsumerOperatorTest::createKafkaConsumerOperator(
    KafkaConsumerOperator::Options options) {
    _source = std::make_unique<KafkaConsumerOperator>(_context.get(), std::move(options));
}

void KafkaConsumerOperatorTest::disableOverrideOffsets(int32_t numPartitions) {
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        auto partitionConsumer =
            dynamic_cast<FakeKafkaPartitionConsumer*>(getConsumerInfo(partition).consumer.get());
        partitionConsumer->_overrideOffsets = false;
    }
}

int32_t KafkaConsumerOperatorTest::runOnce() {
    return _source->runOnce();
}

KafkaConsumerOperator::ConsumerInfo& KafkaConsumerOperatorTest::getConsumerInfo(
    int32_t partition, KafkaConsumerOperator* source) {
    return source->_consumers.at(partition);
}

KafkaConsumerOperator::ConsumerInfo& KafkaConsumerOperatorTest::getConsumerInfo(int32_t partition) {
    return getConsumerInfo(partition, _source.get());
}

int32_t KafkaConsumerOperatorTest::getNumConsumers() const {
    return _source->_consumers.size();
}

const WatermarkControlMsg& KafkaConsumerOperatorTest::getCombinedWatermarkMsg() {
    return _source->_watermarkCombiner->getCombinedWatermarkMsg();
}

boost::optional<StreamDocument> KafkaConsumerOperatorTest::processSourceDocument(
    KafkaSourceDocument sourceDoc, WatermarkGenerator* watermarkGenerator) {
    return _source->processSourceDocument(std::move(sourceDoc), watermarkGenerator);
}

int KafkaConsumerOperatorTest::getRunOnceMaxDocs(KafkaConsumerOperator* source) {
    return source->_options.maxNumDocsToReturn;
}

std::vector<KafkaConsumerOperator::TopicPartition> KafkaConsumerOperatorTest::getTopicPartitions()
    const {
    return _source->getTopicPartitions();
}

boost::optional<int32_t> KafkaConsumerOperatorTest::getPartitionIdx(const std::string& topic,
                                                                    int32_t partition) const {
    return _source->getPartitionIdx(topic, partition);
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
            sourceDoc.topic = partitionConsumer->_options.topicName;
            sourceDoc.partition = partitionConsumer->_options.partition;
            BSONObjBuilder docBuilder;
            docBuilder << "topic" << sourceDoc.topic << "partition" << sourceDoc.partition;
            sourceDoc.doc = docBuilder.obj();
            sourceDoc.offset = partitionOffsets[partition][i];
            sourceDoc.logAppendTimeMs = partitionAppendTimes[partition][i];
            BSONObjBuilder outputDocBuilder(*sourceDoc.doc);
            outputDocBuilder << "_ts" << Date_t::fromMillisSinceEpoch(*sourceDoc.logAppendTimeMs);
            outputDocBuilder << "_stream_meta"
                             << BSON("source" << BSON("type"
                                                      << "kafka"
                                                      << "topic" << sourceDoc.topic << "partition"
                                                      << sourceDoc.partition << "offset"
                                                      << sourceDoc.offset));
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
    std::vector<int32_t> nextDocIndexes(getTopicPartitions().size(), 0);
    for (int32_t i = 0; i < expectednumDocs; ++i) {
        auto& streamDoc = dataMsg.docs[i];
        auto& doc = streamDoc.doc;
        std::string topic = doc.getField("topic").getString();
        int32_t partition = doc.getField("partition").getInt();
        boost::optional<int32_t> consumerIdx = _source->getPartitionIdx(topic, partition);
        ASSERT_TRUE(consumerIdx);
        int32_t docIdx = nextDocIndexes[*consumerIdx]++;
        ASSERT_BSONOBJ_EQ(expectedOutputDocs[*consumerIdx][docIdx], doc.toBson());
        ASSERT_EQUALS(partitionAppendTimes[*consumerIdx][docIdx], streamDoc.minEventTimestampMs);
        ASSERT_EQUALS(partitionAppendTimes[*consumerIdx][docIdx], streamDoc.maxEventTimestampMs);
    }
};

namespace {

WatermarkControlMsg createWatermarkControlMsg(int64_t watermark) {
    WatermarkControlMsg msg;
    msg.eventTimeWatermarkMs = watermark;
    return msg;
}

TEST_F(KafkaConsumerOperatorTest, Basic) {
    createKafkaConsumerOperator(makeOptions(/*numPartitions*/ 2));

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    _source->start();
    invariant(_source->getConnectionStatus().isConnected());
    disableOverrideOffsets(/*numPartitions*/ 2);

    std::vector<KafkaConsumerOperator::TopicPartition> expectedTopicPartitions{
        {"unitTestKafkaTopic", 0}, {"unitTestKafkaTopic", 1}};
    auto sourceTopicPartitions = getTopicPartitions();
    ASSERT_EQUALS(sourceTopicPartitions, expectedTopicPartitions);
    boost::optional<int32_t> pidx = getPartitionIdx("unitTestKafkaTopic", 0);
    ASSERT_TRUE(pidx);
    ASSERT_EQ(*pidx, 0);
    pidx = getPartitionIdx("unitTestKafkaTopic", 1);
    ASSERT_TRUE(pidx);
    ASSERT_EQ(*pidx, 1);

    // Test that runOnce() does not emit any documents yet, but emits a control message.
    ASSERT_EQUALS(0, runOnce());
    std::deque<StreamMsgUnion> msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop_front();
    ASSERT_FALSE(msgUnion.dataMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(-1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(-1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(-1),
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
    msgs.pop_front();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that no control message was emitted as the watermark has not changed since the last
    // runOnce() call.
    // The reason the watermark has not changed is that the watermark of partition 0 is still less
    // than allowedLatenessMs of 10.
    ASSERT_TRUE(msgUnion.controlMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(5 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(25 - 1),
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
    msgs.pop_front();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that the control message is as expected.
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(75 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 15 - 1);

    // Consume 1 doc from partition 0 to advance the watermark.
    partitionOffsets = {{21}, {}};
    partitionAppendTimes = {{60}, {}};
    expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    numDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop_front();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that the control message is as expected.
    ASSERT_EQUALS(createWatermarkControlMsg(60 - 1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(60 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(75 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 60 - 1);
}

TEST_F(KafkaConsumerOperatorTest, BasicMultiTopic) {
    // multiple topics with some non-contiguous partition ids
    std::vector<KafkaConsumerOperator::TopicPartition> topicPartitions{{"topic1", 1},
                                                                       {"topic1", 2},
                                                                       {"topic2", 0},
                                                                       {"topic2", 1},
                                                                       {"topic2", 2},
                                                                       {"topic4", 1},
                                                                       {"topic4", 3},
                                                                       {"topic4", 5},
                                                                       {"topic4", 7},
                                                                       {"topic4", 9}};
    createKafkaConsumerOperator(makeOptions(topicPartitions));

    auto sourceTopicPartitions = getTopicPartitions();
    ASSERT_EQUALS(sourceTopicPartitions, topicPartitions);

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    _source->start();
    invariant(_source->getConnectionStatus().isConnected());

    auto sourceNumConsumers = getNumConsumers();
    ASSERT_EQUALS(sourceNumConsumers, 10);

    std::vector<int> expectedPartitionIds{1, 2, 0, 1, 2, 1, 3, 5, 7, 9};
    for (int consumerIdx = 0; consumerIdx < sourceNumConsumers; consumerIdx++) {
        ASSERT_EQUALS(getConsumerInfo(consumerIdx).partition, expectedPartitionIds[consumerIdx]);
    }

    disableOverrideOffsets(sourceNumConsumers);

    // Test that runOnce() does not emit any documents yet, but emits a control message.
    ASSERT_EQUALS(0, runOnce());
    std::deque<StreamMsgUnion> msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop_front();
    ASSERT_FALSE(msgUnion.dataMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(-1), *msgUnion.controlMsg->watermarkMsg);
    for (auto consumerIdx = 0; consumerIdx < sourceNumConsumers; consumerIdx++) {
        ASSERT_EQUALS(createWatermarkControlMsg(-1),
                      getConsumerInfo(consumerIdx).watermarkGenerator->getWatermarkMsg());
    }

    // Test that runOnce() does not emit anything now, not even a control message.
    ASSERT_EQUALS(0, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(0, msgs.size());

    // Consume docs from the first three consumers. The first two are subscribed to "topic1" and the
    // third consumer is subscribed to "topic2"
    std::vector<std::vector<int64_t>> partitionOffsets = {
        {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3}};
    std::vector<std::vector<int64_t>> partitionAppendTimes = {
        {1, 2, 3, 4, 5}, {5, 10, 15, 20, 25}, {6, 7, 8}};
    auto expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    size_t numDocs = std::accumulate(
        partitionOffsets.begin(), partitionOffsets.end(), 0, [](auto accum, const auto& elem) {
            return accum + elem.size();
        });

    ASSERT_EQUALS(numDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop_front();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numDocs, expectedOutputDocs, partitionAppendTimes);
}

TEST_F(KafkaConsumerOperatorTest, ProcessSourceDocument) {
    boost::intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest{});
    auto exprObj = fromjson("{$toDate: '$event_time_ms'}");
    auto expr = Expression::parseExpression(expCtx.get(), exprObj, expCtx->variablesParseState);
    auto inMemoryDeadLetterQueue = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    _timestampExtractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);

    createKafkaConsumerOperator(makeOptions(/*numPartitions*/ 2));

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    _source->start();
    invariant(_source->getConnectionStatus().isConnected());
    disableOverrideOffsets(/*numPartitions*/ 2);

    // Test that processSourceDocument() works as expected when timestamp can be extracted
    // successfully.
    KafkaSourceDocument sourceDoc;
    sourceDoc.doc = fromjson("{partition: 0, event_time_ms: 1677876150055}");
    BSONObjBuilder outputDocBuilder(*sourceDoc.doc);
    outputDocBuilder << "_ts" << Date_t::fromMillisSinceEpoch(1677876150055);
    auto expectedOutputObj = outputDocBuilder.obj();
    auto streamDoc =
        processSourceDocument(std::move(sourceDoc), getConsumerInfo(0).watermarkGenerator.get());
    ASSERT_BSONOBJ_EQ(expectedOutputObj, streamDoc->doc.toBson());
    ASSERT_EQUALS(1677876150055, streamDoc->minEventTimestampMs);
    ASSERT_EQUALS(1677876150055, streamDoc->maxEventTimestampMs);

    // Test that processSourceDocument() works as expected when timestamp cannot be extracted
    // successfully.
    sourceDoc = KafkaSourceDocument{};
    sourceDoc.doc = fromjson("{partition: 0}");
    ASSERT_FALSE(
        processSourceDocument(std::move(sourceDoc), getConsumerInfo(0).watermarkGenerator.get()));

    // Verify that the previous document was added to the DLQ.
    auto dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQUALS(1, dlqMsgs.size());
    auto dlqDoc = std::move(dlqMsgs.front());
    dlqMsgs.pop();
    ASSERT_BSONOBJ_EQ(fromjson("{partition: 0}"), dlqDoc["doc"].Obj());
    ASSERT_TRUE(dlqDoc["errInfo"]["reason"].String().starts_with(
        "Failed to process input document with error"));
    ASSERT_EQ("KafkaConsumerOperator", dlqDoc["operatorName"].String());

    // Test that processSourceDocument() works as expected when the source document was not parsed
    // successfully.
    sourceDoc = KafkaSourceDocument{};
    sourceDoc.error = "synthetic error";
    ASSERT_FALSE(
        processSourceDocument(std::move(sourceDoc), getConsumerInfo(0).watermarkGenerator.get()));

    // Verify that the previous document was added to the DLQ.
    dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQUALS(1, dlqMsgs.size());
    dlqDoc = std::move(dlqMsgs.front());
    dlqMsgs.pop();
    ASSERT_EQUALS("synthetic error", dlqDoc["errInfo"]["reason"].String());
    ASSERT_EQ("KafkaConsumerOperator", dlqDoc["operatorName"].String());
}

TEST_F(KafkaConsumerOperatorTest, DoNotDropLateDocuments) {
    auto inMemoryDeadLetterQueue = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    createKafkaConsumerOperator(makeOptions(/*numPartitions*/ 2));

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    _source->start();
    invariant(_source->getConnectionStatus().isConnected());
    disableOverrideOffsets(/*numPartitions*/ 2);

    // Consume 5 docs each from 2 partitions. Let the last 2 docs from partition 0 be late
    // (i.e. partitionAppendTime is <= the partition 0 watermark).
    std::vector<std::vector<int64_t>> partitionOffsets = {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}};
    std::vector<std::vector<int64_t>> partitionAppendTimes = {{1, 2, 15, 3, 4},
                                                              {5, 10, 15, 20, 25}};
    auto expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    int32_t numAcceptedDocs = partitionOffsets[0].size() + partitionOffsets[1].size();

    // Late documents should still be tracked in the number of documents consumed count thats
    // returned from `runOnce()`.
    ASSERT_EQUALS(numAcceptedDocs, runOnce());

    auto msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop_front();

    // Test that the output docs are as expected and thus verify that the 2 late docs were dropeed.
    verifyDocs(*msgUnion.dataMsg, numAcceptedDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that the control message is as expected.
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 1), *msgUnion.controlMsg->watermarkMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(25 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 15 - 1);

    // Verify that the 2 late docs were added to the DLQ.
    auto dlqMsgs = inMemoryDeadLetterQueue->getMessages();
    ASSERT_EQ(0, dlqMsgs.size());
    // Consume 5 docs each from 2 partitions. Let the first 2 docs from partition 1 be late
    // (i.e. partitionAppendTime is <= the partition 1 watermark).
    partitionOffsets = {{11, 12, 13, 14, 15}, {21, 22, 23, 24, 25}};
    partitionAppendTimes = {{5, 6, 7, 8, 9}, {12, 13, 15, 30, 35}};
    expectedOutputDocs = ingestDocs(partitionOffsets, partitionAppendTimes);

    numAcceptedDocs = partitionOffsets[0].size() + partitionOffsets[1].size();
    ASSERT_EQUALS(numAcceptedDocs, runOnce());
    msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    msgUnion = std::move(msgs.front());
    msgs.pop_front();

    // Test that the output docs are as expected.
    verifyDocs(*msgUnion.dataMsg, numAcceptedDocs, expectedOutputDocs, partitionAppendTimes);
    // Test that no control message was emitted as the watermark has not changed since the last
    // runOnce() call.
    ASSERT_FALSE(msgUnion.controlMsg);
    ASSERT_EQUALS(createWatermarkControlMsg(15 - 1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(35 - 1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 15 - 1);
    ASSERT_EQ(0, dlqMsgs.size());
}

// Verify the first and second checkpoints for a KafkaConsumerOperator $source.
TEST_F(KafkaConsumerOperatorTest, FirstCheckpoint) {
    // Setup the context.
    auto svcCtx = getServiceContext();
    auto metricManager = std::make_unique<MetricManager>();
    auto context = std::get<0>(getTestContext(svcCtx));

    bool isFakeKafka = true;
    std::string localKafkaBrokers{""};
    // Note: this is not currently used in evergreen, just for local testing.
    // If this environment variable is set we point the test at an actual Kafka broker.
    if (const char* localBroker = std::getenv("KAFKA_TEST_BROKERS")) {
        isFakeKafka = false;
        localKafkaBrokers = localBroker;
    }

    using Input = stdx::unordered_map<int32_t, std::vector<BSONObj>>;
    struct Spec {
        // Per-partition input to the topic.
        Input input;
        // Whether to startAt the current beginning or end of the topic.
        KafkaSourceStartAtEnum startAt;
        // The expected offsets in the first checkpoint before data processing begins.
        stdx::unordered_map<int32_t, int64_t> expectedPartitionOffset;
    };

    auto insertData = [&](KafkaConsumerOperator* source, const Input& input) {
        if (isFakeKafka) {
            // Add input to the partitions.
            for (auto& [partition, docs] : input) {
                auto& info = getConsumerInfo(partition, source);
                auto consumer = dynamic_cast<FakeKafkaPartitionConsumer*>(info.consumer.get());
                std::vector<KafkaSourceDocument> partitionInput;
                for (auto& doc : docs) {
                    partitionInput.emplace_back(KafkaSourceDocument{
                        .doc = doc,
                        .topic = consumer->topicName(),
                        .partition = partition,
                        .logAppendTimeMs = Date_t::now().toMillisSinceEpoch(),
                    });
                }
                consumer->addDocuments(std::move(partitionInput));
            }
        } else {
            // Add input to the partitions.
            // Test only works for one topic
            ASSERT(source->getOptions().topicNames.size() == 1);
            for (auto& [partition, docs] : input) {
                // Create a KafkaEmitOperator and output data to the Kafka topic.
                KafkaEmitOperator emitForTest{context.get(),
                                              {.bootstrapServers = localKafkaBrokers,
                                               .topicName = source->getOptions().topicNames[0]}};
                emitForTest.start();
                std::vector<StreamDocument> partitionInput;
                for (auto& doc : docs) {
                    partitionInput.push_back(StreamDocument{Document{doc}});
                }
                emitForTest.onDataMsg(0, StreamDataMsg{.docs = std::move(partitionInput)});
                emitForTest.stop();
            }
        }
    };

    auto createAndAddInput = [&](const Spec& spec) {
        const int32_t partitionCount = spec.input.size();
        const auto topicName = UUID::gen().toString();
        const std::string consumerGroupId = UUID::gen().toString();

        // Create a KafkaConsumerOperator.
        KafkaConsumerOperator::Options options;
        options.useWatermarks = true;
        options.isTest = isFakeKafka;
        options.bootstrapServers = localKafkaBrokers;
        options.deserializer = _deserializer.get();
        options.topicNames = {topicName};
        options.consumerGroupId = consumerGroupId;
        if (isFakeKafka) {
            for (int i = 0; i < partitionCount; i++) {
                options.testOnlyTopicPartitions.emplace_back(topicName, i);
            }
        }
        options.startOffset = spec.startAt == KafkaSourceStartAtEnum::Earliest
            ? RdKafka::Topic::OFFSET_BEGINNING
            : RdKafka::Topic::OFFSET_END;
        // Create the source and sink and connect them.
        auto source = std::make_unique<KafkaConsumerOperator>(context.get(), std::move(options));
        auto sink = std::make_unique<InMemorySinkOperator>(context.get(), /*numInputs*/ 1);
        sink->setOperatorId(1);
        source->addOutput(sink.get(), 0);
        return std::make_pair(std::move(source), std::move(sink));
    };

    auto getStateFromCheckpoint = [&](CheckpointId checkpointId, OperatorId operatorId) {
        context->checkpointStorage->startCheckpointRestore(checkpointId);
        auto reader = context->checkpointStorage->createStateReader(checkpointId, operatorId);
        auto bsonState = context->checkpointStorage->getNextRecord(reader.get());
        ASSERT(bsonState);
        ASSERT(bsonState);
        return KafkaSourceCheckpointState::parseOwned(IDLParserContext("test"),
                                                      bsonState->toBson());
    };

    // Verify the first checkpoint that occurs before data processing.
    // We inspect the offset in the $source state, and verify it aligns with
    // the size of the input and whether startAt is end or beginning.
    auto innerTest = [&](Spec spec) {
        context->checkpointStorage = std::make_unique<InMemoryCheckpointStorage>(context.get());
        context->checkpointStorage->registerMetrics(_executor->getMetricManager());

        const int32_t partitionCount = spec.input.size();
        if (spec.input.size() > 1 && !isFakeKafka) {
            // More than one partition needs to be setup in the local kafka cluster,
            // so just skipping these tests for now.
            return;
        }

        auto [source, sink] = createAndAddInput(spec);
        if (isFakeKafka) {
            sink->start();
            source->start();
            invariant(source->getConnectionStatus().isConnected());

            // Insert the input into the fake Kafka source after starting the SourceOperator.
            insertData(source.get(), spec.input);

            // Start FakeKafkaPartitionConsumer instances one more time after inserting the data so
            // that they initialize their _startOffset as intended.
            for (int32_t partition = 0; partition < partitionCount; ++partition) {
                auto partitionConsumer = dynamic_cast<FakeKafkaPartitionConsumer*>(
                    getConsumerInfo(partition, source.get()).consumer.get());
                partitionConsumer->start();
            }
        } else {
            // Insert the input into the real Kafka source before starting the SourceOperator.
            insertData(source.get(), spec.input);

            // Wait for the source to be connected like the Executor does.
            while (!source->getConnectionStatus().isConnected()) {
                stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
            }
            sink->start();
            source->start();
        }

        // Before sending any data, send the checkpoint to the operator.
        // Verify the checkpoint contains a well defined starting point.
        auto checkpointId1 = context->checkpointStorage->startCheckpoint();
        source->onControlMsg(
            0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{checkpointId1}});
        // Verify the checkpoint was committed.
        ASSERT_EQ(checkpointId1,
                  dynamic_cast<InMemoryCheckpointStorage*>(context->checkpointStorage.get())
                      ->getLatestCommittedCheckpointId());
        context->checkpointStorage->onCheckpointFlushed(checkpointId1);
        for (const auto& description : context->checkpointStorage->getFlushedCheckpoints()) {
            source->onCheckpointFlush(description.getId());
        }

        // Get the state from checkpoint1 and verify each partitions offset.
        auto state1 = getStateFromCheckpoint(checkpointId1, source->getOperatorId());
        ASSERT_TRUE(state1.getConsumerGroupId());
        ASSERT_EQ(source->getOptions().consumerGroupId, *state1.getConsumerGroupId());
        ASSERT_EQ(partitionCount, state1.getPartitions().size());
        for (int32_t partition = 0; partition < partitionCount; ++partition) {
            auto& partitionState = state1.getPartitions()[partition];
            ASSERT_EQ(partition, partitionState.getPartition());
            ASSERT_EQ(spec.expectedPartitionOffset[partition], partitionState.getOffset());
        }

        // Send some more input to the source.
        Input nextInput;
        int docsPerPartition = getRunOnceMaxDocs(source.get()) * 2;
        for (int32_t partition = 0; partition < partitionCount; ++partition) {
            std::vector<BSONObj> partitionInput;
            auto startingOffset = spec.input[partition].size();
            for (auto i = 0; i < docsPerPartition; ++i) {
                partitionInput.push_back(BSON("idx" << int(i + startingOffset)));
            }
            nextInput[partition] = std::move(partitionInput);
        }
        insertData(source.get(), nextInput);
        stdx::this_thread::sleep_for(stdx::chrono::seconds(1));
        // Run the source and checkpoint until we see all the docs.
        int32_t docsSent{0};
        while (docsSent == 0) {
            docsSent += source->runOnce();
        }
        // Write a checkpoint.
        auto checkpointId2 = context->checkpointStorage->startCheckpoint();
        source->onControlMsg(
            0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{checkpointId2}});
        // Verify the checkpoint was committed.
        ASSERT_EQ(checkpointId2,
                  dynamic_cast<InMemoryCheckpointStorage*>(context->checkpointStorage.get())
                      ->getLatestCommittedCheckpointId());
        source->stop();
        sink->stop();
        // Verify the diff in the offsets between checkpoint2 and checkpoint1 agrees with
        // the docsSent returns from runOnce.
        int64_t docsSentFromOffsets{0};
        auto state2 = getStateFromCheckpoint(checkpointId2, source->getOperatorId());
        ASSERT_TRUE(state2.getConsumerGroupId());
        ASSERT_EQ(source->getOptions().consumerGroupId, *state2.getConsumerGroupId());
        ASSERT_EQ(partitionCount, state2.getPartitions().size());
        for (int32_t partition = 0; partition < partitionCount; ++partition) {
            docsSentFromOffsets += state2.getPartitions()[partition].getOffset() -
                state1.getPartitions()[partition].getOffset();
        }
        ASSERT_EQ(docsSent, docsSentFromOffsets);
    };

    innerTest({
        // 3 documents in the partition0 input.
        .input = {{0 /* partition */, {BSON("a" << 0), BSON("a" << 1), BSON("a" << 2)}}},
        .startAt = KafkaSourceStartAtEnum::Latest,
        // Expected partition0 offset is 3, the "end of topic" offset.
        .expectedPartitionOffset = {{0, {3}}},
    });
    innerTest({
        .input = {{0 /* partition */, {BSON("a" << 0), BSON("a" << 1), BSON("a" << 2)}}},
        .startAt = KafkaSourceStartAtEnum::Earliest,
        .expectedPartitionOffset = {{0, {0}}},
    });
    auto makeSpec = [&](int partitionCount, KafkaSourceStartAtEnum startAt) {
        Spec spec;
        spec.startAt = startAt;
        // Generate an input with a different amount of docs in each partition.
        // partition X will have 3*X docs in it.
        for (int32_t partition = 0; partition < partitionCount; ++partition) {
            int count = partition * 3;
            std::vector<BSONObj> partitionInput;
            for (auto i = 0; i < count; ++i) {
                partitionInput.push_back(BSON("idx" << i));
            }
            spec.input[partition] = std::move(partitionInput);
            // Set the expected offset based on the startAt.
            spec.expectedPartitionOffset[partition] =
                startAt == KafkaSourceStartAtEnum::Earliest ? 0 : spec.input[partition].size();
        }
        return spec;
    };
    innerTest(makeSpec(8 /* partitionCount */, KafkaSourceStartAtEnum::Latest));
    innerTest(makeSpec(8 /* partitionCount */, KafkaSourceStartAtEnum::Earliest));
}

TEST_F(KafkaConsumerOperatorTest, WatermarkAlignment) {
    auto opts = makeOptions(/* numPartitions */ 3);
    opts.partitionIdleTimeoutMs = mongo::stdx::chrono::milliseconds(10'000);
    createKafkaConsumerOperator(opts);

    auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);
    _source->addOutput(sink.get(), 0);

    _source->start();
    invariant(_source->getConnectionStatus().isConnected());

    ASSERT_EQUALS(createWatermarkControlMsg(-1),
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(-1),
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(-1),
                  getConsumerInfo(2).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, -1);

    // Make sure partition watermarks are initialized correctly
    auto partitionStates = _source->getPartitionStates();
    ASSERT_EQUALS(partitionStates.at(0).watermark, -1);
    ASSERT_EQUALS(partitionStates.at(1).watermark, -1);
    ASSERT_EQUALS(partitionStates.at(2).watermark, -1);

    // Consume 5 docs each for the first two partitions.
    std::vector<std::vector<int64_t>> partitionOffsets = {
        /* p0 */ {1, 2}, /* p1 */ {1, 2, 3, 4, 5}, /* p2 */ {1, 2, 3}};
    std::vector<std::vector<int64_t>> partitionAppendTimes = {
        /* p0 */ {20, 21}, /* p1 */ {5, 10, 15, 19, 25}, /* p2 */ {22, 23, 24}};
    (void)ingestDocs(partitionOffsets, partitionAppendTimes);
    ASSERT_EQUALS(10, runOnce());

    // Make sure p0 messages were processed first.
    auto front = sink->getMessages().front().dataMsg->docs.front();
    ASSERT_EQUALS(20, front.minEventTimestampMs);

    ASSERT_EQUALS(createWatermarkControlMsg(20),  // 21 - 1
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(24),  // 25 - 1
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(23),  // 24 - 1),
                  getConsumerInfo(2).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 20);

    // Make sure partition watermarks are correct
    partitionStates = _source->getPartitionStates();
    ASSERT_EQUALS(partitionStates.at(0).watermark, 20);
    ASSERT_EQUALS(partitionStates.at(1).watermark, 24);
    ASSERT_EQUALS(partitionStates.at(2).watermark, 23);

    // p0 and p1 watermark should pass p2 watermark, so p2 should be at the front of
    // the heap now.
    partitionOffsets = {/* p0 */ {3, 4}, /* p1 */ {6, 7}, /* p2 */ {}};
    partitionAppendTimes = {/* p0 */ {40, 41}, /* p1 */ {35, 36}, /* p2 */ {}};
    (void)ingestDocs(partitionOffsets, partitionAppendTimes);
    ASSERT_EQUALS(4, runOnce());

    // Make sure p0 messages were processed first.
    front = sink->getMessages().front().dataMsg->docs.front();
    ASSERT_EQUALS(40, front.minEventTimestampMs);

    ASSERT_EQUALS(createWatermarkControlMsg(40),  // 41 - 1
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(35),  // 36 - 1
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(23),  // 24 - 1
                  getConsumerInfo(2).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 23);

    // Make sure partition watermarks are correct
    partitionStates = _source->getPartitionStates();
    ASSERT_EQUALS(partitionStates.at(0).watermark, 40);
    ASSERT_EQUALS(partitionStates.at(1).watermark, 35);
    ASSERT_EQUALS(partitionStates.at(2).watermark, 23);

    // p0 and p1 have documents available, but p2 does not. Even though p2 has the lowest
    // watermark, since it has no documents, p0 and p1 should be processed even though
    // they have a higher watermark.
    partitionOffsets = {/* p0 */ {5, 6}, /* p1 */ {8, 9}, /* p2 */ {4, 5}};
    partitionAppendTimes = {/* p0 */ {42, 43}, /* p1 */ {37, 38}, /* p2 */ {34, 35}};
    (void)ingestDocs(partitionOffsets, partitionAppendTimes);
    ASSERT_EQUALS(6, runOnce());

    // Make sure p2 messages were processed first since p2 had the lowest local watermark.
    front = sink->getMessages().front().dataMsg->docs.front();
    ASSERT_EQUALS(34, front.minEventTimestampMs);

    ASSERT_EQUALS(createWatermarkControlMsg(42),  // 43 - 1
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(37),  // 38 - 1
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(34),  // 35 - 1
                  getConsumerInfo(2).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(34), getCombinedWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 34);

    // Make sure partition watermarks are correct
    partitionStates = _source->getPartitionStates();
    ASSERT_EQUALS(partitionStates.at(0).watermark, 42);
    ASSERT_EQUALS(partitionStates.at(1).watermark, 37);
    ASSERT_EQUALS(partitionStates.at(2).watermark, 34);

    // Force partition 2 to go idle.
    auto& p2 = getConsumerInfo(2);
    p2.watermarkGenerator->setIdle();

    partitionOffsets = {/* p0 */ {7}, /* p1 */ {10}, /* p2 */ {}};
    partitionAppendTimes = {/* p0 */ {50}, /* p1 */ {51}, /* p2 */ {}};
    (void)ingestDocs(partitionOffsets, partitionAppendTimes);
    ASSERT_EQUALS(2, runOnce());

    // Make sure p1 messages were processed first since p2 had the lowest local watermark.
    front = sink->getMessages().front().dataMsg->docs.front();
    ASSERT_EQUALS(51, front.minEventTimestampMs);

    ASSERT_EQUALS(createWatermarkControlMsg(49),  // 50 - 1
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(50),  // 51 - 1
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 49);

    // Make sure partition watermarks are correct
    partitionStates = _source->getPartitionStates();
    ASSERT_EQUALS(partitionStates.at(0).watermark, 49);
    ASSERT_EQUALS(partitionStates.at(1).watermark, 50);

    auto p2WatermarkMsg = createWatermarkControlMsg(34);  // 35 - 1
    p2WatermarkMsg.watermarkStatus = WatermarkStatus::kIdle;
    ASSERT_EQUALS(p2WatermarkMsg, getConsumerInfo(2).watermarkGenerator->getWatermarkMsg());

    // The combined watermark should be able to progress now that partition 2 is idle.
    ASSERT_EQUALS(createWatermarkControlMsg(49), getCombinedWatermarkMsg());

    // Force partition 2 to go active again.
    partitionOffsets = {/* p0 */ {8}, /* p1 */ {11}, /* p2 */ {6}};
    partitionAppendTimes = {/* p0 */ {60}, /* p1 */ {61}, /* p2 */ {59}};
    (void)ingestDocs(partitionOffsets, partitionAppendTimes);
    ASSERT_EQUALS(3, runOnce());

    // Make sure p2 messages were processed first since p2 just became active and
    // had the lowest local watermark.
    front = sink->getMessages().front().dataMsg->docs.front();
    ASSERT_EQUALS(59, front.minEventTimestampMs);

    ASSERT_EQUALS(createWatermarkControlMsg(59),  // 60 - 1
                  getConsumerInfo(0).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(60),  // 61 - 1
                  getConsumerInfo(1).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(58),  // 59 - 1
                  getConsumerInfo(2).watermarkGenerator->getWatermarkMsg());
    ASSERT_EQUALS(createWatermarkControlMsg(58), getCombinedWatermarkMsg());
    ASSERT_EQUALS(_source->getStats().watermark, 58);

    // Make sure partition watermarks are correct
    partitionStates = _source->getPartitionStates();
    ASSERT_EQUALS(partitionStates.at(0).watermark, 59);
    ASSERT_EQUALS(partitionStates.at(1).watermark, 60);
    ASSERT_EQUALS(partitionStates.at(2).watermark, 58);
}

TEST_F(KafkaConsumerOperatorTest, GetRdKafkaQueuedMaxMessagesKBytes) {
    StreamProcessorFeatureFlags emptyFlags{{}, Date_t::now().toSystemTimePoint()};
    ASSERT_EQ(boost::none, getRdKafkaQueuedMaxMessagesKBytes(emptyFlags, 1));
    ASSERT_EQ(boost::none, getRdKafkaQueuedMaxMessagesKBytes(emptyFlags, 2));
    ASSERT_EQ(boost::none, getRdKafkaQueuedMaxMessagesKBytes(emptyFlags, 4));

    auto makeFlags = [](Value val) {
        return StreamProcessorFeatureFlags{
            stdx::unordered_map<std::string, Value>{
                {FeatureFlags::kKafkaTotalQueuedBytes.name, std::move(val)}},
            Date_t::now().toSystemTimePoint()};
    };

    ASSERT_EQ(boost::none, getRdKafkaQueuedMaxMessagesKBytes(makeFlags(Value{}), 1));
    ASSERT_EQ(boost::none, getRdKafkaQueuedMaxMessagesKBytes(makeFlags(Value{}), 2));

    std::string sp10{"SP10"};
    auto sp10Default = Value::createIntOrLong(
        FeatureFlags::kKafkaTotalQueuedBytes.tierDefaultValues.find(sp10)->second.coerceToLong());
    ASSERT_EQ(128_MiB, sp10Default.coerceToLong());
    ASSERT_EQ(64_MiB / 1024, *getRdKafkaQueuedMaxMessagesKBytes(makeFlags(sp10Default), 1));
    ASSERT_EQ(64_MiB / 1024, *getRdKafkaQueuedMaxMessagesKBytes(makeFlags(sp10Default), 2));
    ASSERT_EQ(32_MiB / 1024, *getRdKafkaQueuedMaxMessagesKBytes(makeFlags(sp10Default), 4));
    ASSERT_EQ(16_MiB / 1024, *getRdKafkaQueuedMaxMessagesKBytes(makeFlags(sp10Default), 8));
    ASSERT_EQ(8_MiB / 1024, *getRdKafkaQueuedMaxMessagesKBytes(makeFlags(sp10Default), 16));
    ASSERT_EQ(1_MiB / 1024, *getRdKafkaQueuedMaxMessagesKBytes(makeFlags(sp10Default), 128));
}

}  // namespace
}  // namespace streams
