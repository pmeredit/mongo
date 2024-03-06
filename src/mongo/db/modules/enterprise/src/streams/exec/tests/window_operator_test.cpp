/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <boost/none.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <chrono>
#include <exception>
#include <memory>
#include <vector>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/exec/window_operator.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using namespace std;

class WindowOperatorTest : public AggregationContextFixture {
public:
    WindowOperatorTest() {
        _metricManager = std::make_unique<MetricManager>();
        _context = getTestContext(/*svcCtx*/ nullptr, _metricManager.get());
    }

    static StreamDocument generateDocMinutes(int minutes, int id, int value) {
        return generateDoc(
            Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(minutes)), id, value);
    }

    static StreamDocument generateDocMs(int ms, int id, int value) {
        return generateDoc(
            Date_t::fromDurationSinceEpoch(stdx::chrono::milliseconds(ms)), id, value);
    }

    static StreamDocument generateDocSeconds(int seconds, int id, int value) {
        return generateDoc(
            Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(seconds)), id, value);
    }

    static StreamDocument generateDoc(Date_t time, int id, int value) {
        Document doc(BSON("date" << time << "id" << id << "value" << value));
        StreamDocument streamDoc(std::move(doc));
        streamDoc.minEventTimestampMs = time.toMillisSinceEpoch();
        return streamDoc;
    }

    auto logResults(const StreamDataMsg& results, int tag) {
        for (auto& doc : results.docs) {
            LOGV2_INFO(5555501,
                       "dataMsg",
                       "tag"_attr = tag,
                       "minEventTimestampMs"_attr = doc.minEventTimestampMs,
                       "maxEventTimestampMs"_attr = doc.maxEventTimestampMs,
                       "doc"_attr = doc.doc.toString());
        }
    }

    auto logResults(const std::vector<StreamDataMsg> results, int tag) {
        for (auto& result : results) {
            logResults(result, tag);
        }
    }

    auto logResults(const std::vector<StreamMsgUnion>& results, int tag) {
        for (auto& result : results) {
            if (result.controlMsg) {
                LOGV2_INFO(5555500,
                           "controlMsg",
                           "watermark"_attr =
                               result.controlMsg->watermarkMsg->eventTimeWatermarkMs);
            } else {
                logResults(*result.dataMsg, tag);
            }
        }
    };

    auto toOldestWindowStartTime(int64_t time, WindowOperator* op) {
        return op->toOldestWindowStartTime(time);
    }

    std::pair<Date_t, Date_t> getBoundaries(StreamDocument windowOutputDocument) {
        auto& streamMeta = windowOutputDocument.streamMeta;
        return std::make_pair(*streamMeta.getWindowStartTimestamp(),
                              *streamMeta.getWindowEndTimestamp());
    }

    void verifyBoundaries(const boost::optional<StreamDataMsg>& msg,
                          Date_t expectedStart,
                          Date_t expectedEnd) {
        ASSERT(msg != boost::none);
        for (auto& doc : msg->docs) {
            auto [start, end] = getBoundaries(doc);
            ASSERT_EQ(expectedStart, start);
            ASSERT_EQ(expectedEnd, end);
        }
    }

    std::vector<StreamMsgUnion> toVector(std::deque<StreamMsgUnion> value) {
        return {value.begin(), value.end()};
    }

    int64_t toMillis(stdx::chrono::system_clock::time_point time) {
        return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(time.time_since_epoch())
            .count();
    }

    auto toMillis(int size, StreamTimeUnitEnum unit) {
        return streams::toMillis(unit, size);
    };

    auto generateDataMsg(Date_t date, int id, BSONObj obj) {
        return StreamMsgUnion{.dataMsg = StreamDataMsg{.docs = {generateDoc(date, id, 1)}}};
    };

    auto generateDataMsg(Date_t date, int id) {
        return StreamMsgUnion{.dataMsg = StreamDataMsg{.docs = {generateDoc(date, id, 1)}}};
    };

    auto generateDataMsg(Date_t date) {
        return generateDataMsg(date, 0);
    };

    auto generateControlMessage(Date_t date) {
        return StreamMsgUnion{.controlMsg = StreamControlMsg{
                                  .watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive,
                                                                      date.toMillisSinceEpoch()}}};
    };

    auto getResults(InMemorySourceOperator* source,
                    InMemorySinkOperator* sink,
                    std::vector<StreamMsgUnion> inputs) {
        for (auto& input : inputs) {
            if (input.controlMsg) {
                source->addControlMsg(*input.controlMsg);
            } else {
                source->addDataMsg(*input.dataMsg, boost::none);
            }

            source->runOnce();
        }

        return toVector(sink->getMessages());
    };

    auto date(int hour, int minute, int second, int milliseconds) {
        return timeZone.createFromDateParts(2020, 1, 1, hour, minute, second, milliseconds);
    };

    std::vector<BSONObj> parseBsonVector(std::string json) {
        const auto inputBson = fromjson("{pipeline: " + json + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
        return parsePipelineFromBSON(inputBson["pipeline"]);
    }

    std::vector<BSONObj> innerPipeline() {
        return parseBsonVector(_innerPipelineJson);
    }

    auto commonHoppingInnerTest(std::string innerPipeline,
                                StreamTimeUnitEnum windowSizeUnit,
                                int windowSize,
                                StreamTimeUnitEnum hopSizeUnit,
                                int hopSize,
                                std::vector<StreamMsgUnion> input) {
        auto bsonVector = parseBsonVector(innerPipeline);
        WindowOperator::Options options{
            bsonVector, windowSize, windowSizeUnit, hopSize, hopSizeUnit};
        WindowOperator op(_context.get(), options);
        InMemorySourceOperator source(_context.get(), InMemorySourceOperator::Options());
        InMemorySinkOperator sink(_context.get(), 1);
        source.addOutput(&op, 0);
        op.addOutput(&sink, 0);
        source.start();
        sink.start();

        return getResults(&source, &sink, input);
    }

    auto commonInnerTest(std::string innerPipeline,
                         StreamTimeUnitEnum sizeUnit,
                         int size,
                         std::vector<StreamMsgUnion> input) {
        auto bsonVector = parseBsonVector(innerPipeline);
        WindowOperator::Options options{bsonVector, size, sizeUnit, size, sizeUnit};
        WindowOperator op(_context.get(), options);
        InMemorySourceOperator source(_context.get(), InMemorySourceOperator::Options());
        InMemorySinkOperator sink(_context.get(), 1);
        source.addOutput(&op, 0);
        op.addOutput(&sink, 0);
        source.start();
        sink.start();

        return getResults(&source, &sink, input);
    }

    int64_t getCurrentMillis() {
        return duration_cast<stdx::chrono::milliseconds>(
                   stdx::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    auto commonKafkaInnerTest(std::vector<BSONObj> inputDocs,
                              std::string pipeline,
                              boost::optional<int> maxNumDocsToReturn = boost::none,
                              int allowedLatenessMs = 0) {
        auto sourceOptions = fromjson(R"({
            connectionName: "kafka1",
            topic: "topic1",
            timeField : { $dateFromString : { "dateString" : "$timestamp"} },
            testOnlyPartitionCount: 1
        })");
        sourceOptions = sourceOptions.addFields(
            BSON("allowedLateness" << BSON("size" << allowedLatenessMs << "unit"
                                                  << "ms")));
        auto sourceBson = BSON("$source" << sourceOptions);
        auto emitBson = getTestMemorySinkSpec();
        auto bsonVector = parseBsonVector(pipeline);
        bsonVector.insert(bsonVector.begin(), sourceBson);
        bsonVector.push_back(emitBson);
        KafkaConnectionOptions kafkaOptions("");
        kafkaOptions.setIsTestKafka(true);
        mongo::Connection connection(
            "kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
        Parser parser(_context.get(), /*options*/ {}, {{"kafka1", connection}});
        auto dag = parser.fromBson(bsonVector);
        dag->start();
        dag->source()->connect();

        auto source = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
        auto consumers = this->kafkaGetConsumers(source);
        if (maxNumDocsToReturn) {
            kafkaSetMaxNumDocsToReturn(source, *maxNumDocsToReturn);
            for (auto& consumer : consumers) {
                consumer->_docsPerChunk = *maxNumDocsToReturn;
            }
        }

        std::vector<KafkaSourceDocument> docs;
        for (auto& doc : inputDocs) {
            KafkaSourceDocument sourceDoc;
            sourceDoc.doc = doc;
            docs.emplace_back(std::move(sourceDoc));
        }
        consumers[0]->addDocuments(std::move(docs));
        kafkaRunOnce(source);

        auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());
        auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
        auto results = toVector(sink->getMessages());
        auto dlqMsgs = dlq->getMessages();
        dag->stop();
        return std::make_tuple(std::move(results), std::move(dlqMsgs));
    }

    void kafkaSetMaxNumDocsToReturn(KafkaConsumerOperator* kafkaOp, int num) {
        kafkaOp->_options.maxNumDocsToReturn = num;
    }

    std::vector<FakeKafkaPartitionConsumer*> kafkaGetConsumers(KafkaConsumerOperator* kafkaOp) {
        std::vector<FakeKafkaPartitionConsumer*> results;
        for (auto& consumer : kafkaOp->_consumers) {
            results.push_back(dynamic_cast<FakeKafkaPartitionConsumer*>(consumer.consumer.get()));
        }
        return results;
    }

    void kafkaRunOnce(KafkaConsumerOperator* kafkaOp) {
        kafkaOp->runOnce();
    }

    auto& getWindows(WindowOperator& windowOp) {
        return windowOp._openWindows;
    }

    auto& getInnerOperators(WindowPipeline& windowPipeline) {
        return windowPipeline._options.operators;
    }

    void verifyCommitted(CheckpointStorage* storage, CheckpointId checkpointId) {
        if (auto inMemoryStorage = dynamic_cast<InMemoryCheckpointStorage*>(storage)) {
            ASSERT(inMemoryStorage->_checkpoints[checkpointId].committed);
        } else {
            // TODO: With a real storage endpoint we don't actually verify this.
        }
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    const std::string _innerPipelineJson = R"(
[
    { $group: {
        _id: "$id",
        sum: { $sum: "$value" }
    }},
    { $sort: {
        sum: -1
    }},
    { $limit: 1 }
]
    )";
    const TimeZoneDatabase timeZoneDb{};
    const TimeZone timeZone = timeZoneDb.getTimeZone("UTC");
    ServiceContext* _serviceContext{getServiceContext()};
};

TEST_F(WindowOperatorTest, SmokeTestOperator) {
    const auto inputBson = fromjson("{pipeline: " + _innerPipelineJson + "}");
    ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
    auto bsonVector = parsePipelineFromBSON(inputBson["pipeline"]);

    WindowOperator::Options options{
        bsonVector,
        1,
        StreamTimeUnitEnum::Minute,
        1,
        StreamTimeUnitEnum::Minute,
    };

    WindowOperator op(_context.get(), options);
    InMemorySourceOperator source(_context.get(), InMemorySourceOperator::Options());
    InMemorySinkOperator sink(_context.get(), 1);
    source.addOutput(&op, 0);
    op.addOutput(&sink, 0);
    source.start();
    sink.start();

    StreamDataMsg inputs{{
        generateDocMinutes(0, 0, 0),
        generateDocMinutes(0, 0, 3),
        generateDocMinutes(1, 0, 1),
        generateDocMinutes(2, 0, 2),
        generateDocMinutes(3, 0, 3),
        generateDocMinutes(4, 0, 4),
        generateDocMinutes(0, 1, 5),
        generateDocMinutes(0, 1, 6),
        generateDocMinutes(1, 1, 42),
        generateDocMinutes(1, 1, 42),
        generateDocMinutes(2, 1, 42),
        generateDocMinutes(3, 1, 8),
        generateDocMinutes(4, 1, 9),
    }};

    source.addDataMsg(inputs, boost::none);
    source.runOnce();

    // Pass a watermark that should close windows that start at
    // 1970-01-01 00:00, 00:01, 00:02, 00:03, and 00:04 windows.
    WatermarkControlMsg watermarkMsg{
        WatermarkStatus::kActive,
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(5)).toMillisSinceEpoch()};
    StreamControlMsg controlMsg{std::move(watermarkMsg)};
    source.addControlMsg(std::move(controlMsg));
    source.runOnce();

    std::vector<StreamMsgUnion> results = toVector(sink.getMessages());

    ASSERT_EQ(results.size(), 6);
    ASSERT(results[0].dataMsg);     // [00:00, 00:01)
    ASSERT(results[1].dataMsg);     // [00:01, 00:02)
    ASSERT(results[2].dataMsg);     // [00:02, 00:03)
    ASSERT(results[3].dataMsg);     // [00:03, 00:04)
    ASSERT(results[4].dataMsg);     // [00:04, 00:05)
    ASSERT(results[5].controlMsg);  // Watermark of 00:05

    auto start =
        stdx::chrono::system_clock::time_point(stdx::chrono::minutes(0)).time_since_epoch();
    auto end = start + stdx::chrono::minutes(options.size);
    ASSERT_EQ(1, results[0].dataMsg->docs.size());
    auto [actualStart, actualEnd] = getBoundaries(results[0].dataMsg->docs[0]);
    ASSERT_EQ(Date_t::fromDurationSinceEpoch(start), actualStart);
    ASSERT_EQ(Date_t::fromDurationSinceEpoch(end), actualEnd);
    ASSERT_EQ(1, results[0].dataMsg->docs.size());
}

// Tests that we correctly generate windows for a $hoppingWindow stage where the hopSize is less
// than the window size.
TEST_F(WindowOperatorTest, TestHoppingWindowOverlappingWindows) {
    const auto inputBson = fromjson("{pipeline: " + _innerPipelineJson + "}");
    ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
    auto bsonVector = parsePipelineFromBSON(inputBson["pipeline"]);

    const size_t kWindowSize = 5;
    const size_t kHopSize = 2;

    WindowOperator::Options options{
        bsonVector,
        kWindowSize,
        StreamTimeUnitEnum::Minute,
        kHopSize,
        StreamTimeUnitEnum::Minute,
    };

    WindowOperator op(_context.get(), options);
    InMemorySourceOperator source(_context.get(), InMemorySourceOperator::Options());
    InMemorySinkOperator sink(_context.get(), 1);
    source.addOutput(&op, 0);
    op.addOutput(&sink, 0);
    source.start();
    sink.start();

    StreamDataMsg inputs{{
        generateDocMinutes(4, 0, 2),
        generateDocMinutes(6, 0, 3),
        generateDocMinutes(5, 0, 2),
        generateDocMinutes(7, 1, 2),
        generateDocMinutes(7, 1, 8),
        generateDocMinutes(8, 1, 8),
        generateDocMinutes(7, 1, 8),
        generateDocMinutes(9, 1, 8),
        generateDocMinutes(7, 1, 8),
        generateDocMinutes(10, 1, 8),
        generateDocMinutes(9, 1, 8),
        generateDocMinutes(12, 1, 8),
    }};

    source.addDataMsg(inputs, boost::none);
    source.runOnce();

    // Pass a watermark that should close windows that start at
    // 1970-01-01 00:01, 00:03, 00:05, 00:07, 00:09, and 00:11 windows.
    WatermarkControlMsg watermarkMsg{
        WatermarkStatus::kActive,
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(17)).toMillisSinceEpoch()};
    StreamControlMsg controlMsg{std::move(watermarkMsg)};
    source.addControlMsg(std::move(controlMsg));
    source.runOnce();

    std::vector<StreamMsgUnion> results = toVector(sink.getMessages());
    ASSERT_EQ(results.size(), 7);
    verifyBoundaries(results[0].dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(1)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(6)));  // [00::01, 00::06)
    verifyBoundaries(results[1].dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(3)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(8)));  // [00::03, 00::08)
    verifyBoundaries(
        results[2].dataMsg,
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(5)),
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(10)));  // [00::05, 00::10)
    verifyBoundaries(
        results[3].dataMsg,
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(7)),
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(12)));  // [00::07, 00::12)
    verifyBoundaries(
        results[4].dataMsg,
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(9)),
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(14)));  // [00::09, 00::14)
    verifyBoundaries(
        results[5].dataMsg,
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(11)),
        Date_t::fromDurationSinceEpoch(stdx::chrono::minutes(16)));  // [00::11, 00::16)
    ASSERT(results[6].controlMsg);                                   // Watermark of 00:17
}

TEST_F(WindowOperatorTest, SmokeTestParser) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ testInMemoryConnectionRegistry());
    std::string _basePipeline = R"(
[
    { $source: {
        connectionName: "__testMemory",
        allowedLateness: { size: 15, unit: "second" }
    }},
    { $tumblingWindow: {
      interval: { size: 1, unit: "second" },
      pipeline:
      [
        { $sort: { date: 1 }},
        { $group: {
            _id: "$id",
            sum: { $sum: "$value" },
            max: { $max: "$value" },
            min: { $min: "$value" },
            first: { $first: "$value" },
            stdDevPop: { $stdDevPop: "$value" },
            stdDevSamp: { $stdDevSamp: "$value" },
            firstN: { $firstN: { input: "$value", n: 2 } },
            last: { $last: "$value" },
            lastN: { $lastN: { input: "$value", n: 2 } },
            addToSet: { $addToSet: "$value" }
        }},
        { $sort: { _id: 1 }}
      ]
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";
    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + _basePipeline + "}")["pipeline"]));
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());
    dag->start();

    StreamDataMsg inputs{{
        generateDocMs(1, 0, 1),
        generateDocMs(2, 0, 2),
        generateDocMs(3, 0, 3),
        generateDocMs(4, 0, 4),
        generateDocMs(0, 0, 5),

        generateDocMs(1, 1, 42),
        generateDocMs(1, 1, 42),
        generateDocMs(2, 1, 42),
        generateDocMs(3, 1, 8),
        generateDocMs(4, 1, 9),
        generateDocMs(0, 1, 5),
    }};
    source->addDataMsg(inputs, boost::none);
    source->runOnce();

    source->addControlMsg({WatermarkControlMsg{
        WatermarkStatus::kActive,
        Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(1)).toMillisSinceEpoch()}});
    source->runOnce();

    auto results = toVector(sink->getMessages());
    ASSERT_EQ(2, results.size());

    for (auto& doc : results[0].dataMsg->docs) {
        auto [start, end] = getBoundaries(doc);
        ASSERT_EQ(Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(0)), start);
        ASSERT_EQ(Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(1)), end);
    }

    // group for id: 0
    // should always be docs[0] due to the { $sort _id: 1 }
    auto bson0 = results[0].dataMsg->docs[0].doc.toBson();
    ASSERT_EQ(0, bson0.getIntField("_id"));
    ASSERT_EQ(15, bson0.getIntField("sum"));
    ASSERT_EQ(5, bson0.getIntField("max"));
    ASSERT_EQ(1, bson0.getIntField("min"));
    // should always be 5 due to the { $sort date: 1 }
    ASSERT_EQ(5, bson0.getIntField("first"));

    // group for id: 1
    auto bson1 = results[0].dataMsg->docs[1].doc.toBson();
    ASSERT_EQ(1, bson1.getIntField("_id"));
    ASSERT_EQ(148, bson1.getIntField("sum"));
    ASSERT_EQ(42, bson1.getIntField("max"));
    ASSERT_EQ(5, bson1.getIntField("min"));
    ASSERT_EQ(5, bson1.getIntField("first"));
}

TEST_F(WindowOperatorTest, SmokeTestParserHoppingWindow) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ testInMemoryConnectionRegistry());
    std::string _basePipeline = R"(
[
    { $source: {
        connectionName: "__testMemory",
        allowedLateness: { size: 10, unit: "second" }
    }},
    { $hoppingWindow: {
      interval: {size: 3, unit: "second"},
      hopSize: {size: 1, unit: "second"},
      pipeline:
      [
        { $sort: { date: 1 }},
        { $group: {
            _id: "$id",
            sum: { $sum: "$value" },
            max: { $max: "$value" },
            min: { $min: "$value" },
            first: { $first: "$value" },
            stdDevPop: { $stdDevPop: "$value" },
            stdDevSamp: { $stdDevSamp: "$value" },
            firstN: { $firstN: { input: "$value", n: 2 } },
            last: { $last: "$value" },
            lastN: { $lastN: { input: "$value", n: 2 } },
            addToSet: { $addToSet: "$value" }
        }},
        { $sort: { _id: 1 }}
      ]
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";
    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + _basePipeline + "}")["pipeline"]));
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());
    dag->start();

    // The inputs below will produce the following windows and groups:
    // [0, 3) -> 0, 1
    // [1, 4) -> 0, 1
    // [2, 5) -> 0, 1
    // [3, 6) -> 0, 1
    // [4, 7) -> 0, 1
    StreamDataMsg inputs{{
        generateDocMs(2001, 0, 3),
        generateDocMs(2003, 0, 1),

        generateDocMs(2002, 1, 6),
        generateDocMs(2030, 1, 7),

        generateDocMs(3001, 0, 14),
        generateDocMs(4001, 0, 15),

        generateDocMs(3001, 1, 23),
        generateDocMs(4900, 1, 22),
    }};
    source->addDataMsg(inputs, boost::none);
    source->runOnce();

    source->addControlMsg(
        {WatermarkControlMsg{WatermarkStatus::kActive,
                             Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(8))
                                 .toMillisSinceEpoch()}});  // This will close all three windows.
    source->runOnce();

    auto results = toVector(sink->getMessages());
    ASSERT_EQ(6, results.size());

    auto result = results[0];
    // Verify the results of the [0, 3) window.
    verifyBoundaries(result.dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(0)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(3)));

    // group for id: 0
    // should always be docs[0] due to the { $sort _id: 1 }
    auto bson0 = result.dataMsg->docs[0].doc.toBson();
    ASSERT_EQ(0, bson0.getIntField("_id"));
    ASSERT_EQ(4, bson0.getIntField("sum"));
    ASSERT_EQ(3, bson0.getIntField("max"));
    ASSERT_EQ(1, bson0.getIntField("min"));
    // should always be 3 due to the { $sort date: 1 }
    ASSERT_EQ(3, bson0.getIntField("first"));

    // group for id: 1
    auto bson1 = result.dataMsg->docs[1].doc.toBson();
    ASSERT_EQ(1, bson1.getIntField("_id"));
    ASSERT_EQ(13, bson1.getIntField("sum"));
    ASSERT_EQ(7, bson1.getIntField("max"));
    ASSERT_EQ(6, bson1.getIntField("min"));
    ASSERT_EQ(6, bson1.getIntField("first"));

    result = results[1];
    // Verify the results of the [1, 4) window.
    verifyBoundaries(result.dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(1)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(4)));

    // group for id: 0
    // should always be docs[0] due to the { $sort _id: 1 }
    bson0 = result.dataMsg->docs[0].doc.toBson();
    ASSERT_EQ(0, bson0.getIntField("_id"));
    ASSERT_EQ(18, bson0.getIntField("sum"));
    ASSERT_EQ(14, bson0.getIntField("max"));
    ASSERT_EQ(1, bson0.getIntField("min"));
    // should always be 3 due to the { $sort date: 1 }
    ASSERT_EQ(3, bson0.getIntField("first"));

    // group for id: 1
    bson1 = result.dataMsg->docs[1].doc.toBson();
    ASSERT_EQ(1, bson1.getIntField("_id"));
    ASSERT_EQ(36, bson1.getIntField("sum"));
    ASSERT_EQ(23, bson1.getIntField("max"));
    ASSERT_EQ(6, bson1.getIntField("min"));
    ASSERT_EQ(6, bson1.getIntField("first"));

    result = results[2];
    // Verify the results of the [2, 5) window.
    verifyBoundaries(result.dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(2)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(5)));

    // group for id: 0
    // should always be docs[0] due to the { $sort _id: 1 }
    bson0 = result.dataMsg->docs[0].doc.toBson();
    ASSERT_EQ(0, bson0.getIntField("_id"));
    ASSERT_EQ(33, bson0.getIntField("sum"));
    ASSERT_EQ(15, bson0.getIntField("max"));
    ASSERT_EQ(1, bson0.getIntField("min"));
    // should always be 3 due to the { $sort date: 1 }
    ASSERT_EQ(3, bson0.getIntField("first"));

    // group for id: 1
    bson1 = result.dataMsg->docs[1].doc.toBson();
    ASSERT_EQ(1, bson1.getIntField("_id"));
    ASSERT_EQ(58, bson1.getIntField("sum"));
    ASSERT_EQ(23, bson1.getIntField("max"));
    ASSERT_EQ(6, bson1.getIntField("min"));
    ASSERT_EQ(6, bson1.getIntField("first"));

    result = results[3];
    // Verify the results of the [3, 6) window.
    verifyBoundaries(result.dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(3)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(6)));

    // group for id: 0
    // should always be docs[0] due to the { $sort _id: 1 }
    bson0 = result.dataMsg->docs[0].doc.toBson();
    ASSERT_EQ(0, bson0.getIntField("_id"));
    ASSERT_EQ(29, bson0.getIntField("sum"));
    ASSERT_EQ(15, bson0.getIntField("max"));
    ASSERT_EQ(14, bson0.getIntField("min"));
    // should always be 3 due to the { $sort date: 1 }
    ASSERT_EQ(14, bson0.getIntField("first"));

    // group for id: 1
    bson1 = result.dataMsg->docs[1].doc.toBson();
    ASSERT_EQ(1, bson1.getIntField("_id"));
    ASSERT_EQ(45, bson1.getIntField("sum"));
    ASSERT_EQ(23, bson1.getIntField("max"));
    ASSERT_EQ(22, bson1.getIntField("min"));
    ASSERT_EQ(23, bson1.getIntField("first"));

    result = results[4];
    // Verify the results of the [4, 7) window.
    verifyBoundaries(result.dataMsg,
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(4)),
                     Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(7)));

    // group for id: 0
    // should always be docs[0] due to the { $sort _id: 1 }
    bson0 = result.dataMsg->docs[0].doc.toBson();
    ASSERT_EQ(0, bson0.getIntField("_id"));
    ASSERT_EQ(15, bson0.getIntField("sum"));
    ASSERT_EQ(15, bson0.getIntField("max"));
    ASSERT_EQ(15, bson0.getIntField("min"));
    // should always be 3 due to the { $sort date: 1 }
    ASSERT_EQ(15, bson0.getIntField("first"));

    // group for id: 1
    bson1 = result.dataMsg->docs[1].doc.toBson();
    ASSERT_EQ(1, bson1.getIntField("_id"));
    ASSERT_EQ(22, bson1.getIntField("sum"));
    ASSERT_EQ(22, bson1.getIntField("max"));
    ASSERT_EQ(22, bson1.getIntField("min"));
    ASSERT_EQ(22, bson1.getIntField("first"));

    ASSERT(results[5].controlMsg);
}

TEST_F(WindowOperatorTest, LargeWindowState) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ testInMemoryConnectionRegistry());
    // Generate 10M unique docs using $range and $unwind.
    std::string _basePipeline = R"(
[
    { $source: { connectionName: "__testMemory" } },
    { $project: { i: { $range: [ 0, 10 ] } } },
    { $unwind: "$i" },
    { $project: { value: { $range: [ { $multiply: [ "$i", 1000000 ] }, { $multiply: [ { $add: [ "$i", 1 ] }, 1000000 ] } ] } } },
    { $unwind: "$value" },
    { "$addFields": { "id": "$value" } },
    { $tumblingWindow: {
      interval: { size: 1, unit: "second" },
      pipeline:
      [
        { $group: {
            _id: "$id",
            sum: { $sum: "$value" }
        }},
        { $sort: { _id: 1 }}
      ]
    }},
    { $emit: {connectionName: "__testMemory" }}
]
    )";
    auto pipeline =
        parsePipelineFromBSON(fromjson("{pipeline: " + _basePipeline + "}")["pipeline"]);
    int expectedNumDocs = 10'000'000;
    if (kDebugBuild) {
        // Use fewer documents in dev builds so the tests don't take too long to run.
        pipeline[3] = fromjson(R"(
            { $project: { value: { $range: [ { $multiply: [ "$i", 10000 ] }, { $multiply: [ { $add: [ "$i", 1 ] }, 10000 ] } ] } } },
        )");
        expectedNumDocs = 100'000;
    }
    auto dag = parser.fromBson(pipeline);
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());
    dag->start();

    StreamDocument doc{Document()};
    doc.minEventTimestampMs = 1000;
    StreamDataMsg dataMsg{{doc}};
    source->addDataMsg(std::move(dataMsg), boost::none);
    source->runOnce();

    // Close the open window.
    source->addControlMsg({WatermarkControlMsg{
        WatermarkStatus::kActive,
        Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(8)).toMillisSinceEpoch()}});
    source->runOnce();

    auto results = sink->getMessages();
    int32_t numResultDocs{0};
    while (!results.empty()) {
        auto msg = std::move(results.front());
        results.pop_front();
        if (msg.dataMsg) {
            numResultDocs += msg.dataMsg->docs.size();
        }
    }
    ASSERT_EQ(expectedNumDocs, numResultDocs);
}

TEST_F(WindowOperatorTest, DateRounding) {
    const TimeZoneDatabase timeZoneDb{};
    const TimeZone timeZone = timeZoneDb.getTimeZone("UTC");

    StreamTimeUnitEnum timeUnit = StreamTimeUnitEnum::Second;
    int sizeInUnits = 1;
    auto makeWindowOp = [&]() {
        return std::make_unique<WindowOperator>(_context.get(),
                                                WindowOperator::Options{.size = sizeInUnits,
                                                                        .sizeUnit = timeUnit,
                                                                        .slide = sizeInUnits,
                                                                        .slideUnit = timeUnit});
    };
    auto windowOp = makeWindowOp();
    auto date =
        [&](int year, int month, int day, int hour, int minute, int second, int milliseconds) {
            return Date_t::fromMillisSinceEpoch(toOldestWindowStartTime(
                timeZone.createFromDateParts(year, month, day, hour, minute, second, milliseconds)
                    .toMillisSinceEpoch(),
                windowOp.get()));
        };

    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 0, 0), date(2023, 4, 1, 0, 0, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 0, 0), date(2023, 4, 1, 0, 0, 0, 1));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 0, 0), date(2023, 4, 1, 0, 0, 0, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 1, 0), date(2023, 4, 1, 0, 0, 1, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 1, 0), date(2023, 4, 1, 0, 0, 1, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 5, 0), date(2023, 4, 1, 0, 0, 5, 55));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 1, 0, 0), date(2023, 4, 1, 0, 1, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 59, 0, 0), date(2023, 4, 1, 0, 59, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 59, 59, 0),
              date(2023, 4, 1, 0, 59, 59, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 23, 0, 0),
              date(2023, 4, 1, 0, 23, 0, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 2, 0, 0), date(2023, 4, 1, 0, 2, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 2, 2, 5, 59, 0),
              date(2023, 4, 2, 2, 5, 59, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 36, 46, 0),
              date(2023, 4, 5, 14, 36, 46, 549));

    sizeInUnits = 10;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 0, 0), date(2023, 4, 1, 0, 0, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 0, 0), date(2023, 4, 1, 0, 0, 9, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 1, 0, 0, 10, 0), date(2023, 4, 1, 0, 0, 10, 0));

    sizeInUnits = 13;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 36, 43, 0),
              date(2023, 4, 5, 14, 36, 46, 549));

    sizeInUnits = 3600;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 0, 0, 0),
              date(2023, 4, 5, 14, 36, 46, 549));

    sizeInUnits = 7200;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 0, 0, 0),
              date(2023, 4, 5, 15, 59, 59, 549));

    timeUnit = StreamTimeUnitEnum::Minute;
    sizeInUnits = 1;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 36, 0, 0),
              date(2023, 4, 5, 14, 36, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 37, 0, 0),
              date(2023, 4, 5, 14, 37, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 6, 14, 37, 0, 0),
              date(2023, 4, 6, 14, 37, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2024, 4, 6, 14, 37, 0, 0),
              date(2024, 4, 6, 14, 37, 46, 549));

    sizeInUnits = 2;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 36, 0, 0),
              date(2023, 4, 5, 14, 36, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 36, 0, 0),
              date(2023, 4, 5, 14, 37, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 6, 14, 36, 0, 0),
              date(2023, 4, 6, 14, 37, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2024, 4, 6, 14, 42, 0, 0),
              date(2024, 4, 6, 14, 43, 59, 999));

    sizeInUnits = 20;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 20, 0, 0),
              date(2023, 4, 5, 14, 36, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 14, 20, 0, 0),
              date(2023, 4, 5, 14, 37, 46, 549));
    ASSERT_EQ(timeZone.createFromDateParts(2024, 4, 6, 14, 40, 0, 0),
              date(2024, 4, 6, 14, 43, 59, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2024, 4, 6, 16, 0, 0, 0),
              date(2024, 4, 6, 16, 19, 59, 999));

    timeUnit = StreamTimeUnitEnum::Hour;
    sizeInUnits = 1;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 23, 0, 0, 0),
              date(2023, 4, 5, 23, 59, 59, 999));

    timeUnit = StreamTimeUnitEnum::Day;
    sizeInUnits = 1;
    windowOp = makeWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 0, 0, 0, 0), date(2023, 4, 5, 0, 0, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 0, 0, 0, 0),
              date(2023, 4, 5, 23, 59, 59, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 10, 0, 0, 0, 0), date(2023, 5, 10, 0, 0, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 15, 0, 0, 0, 0),
              date(2023, 5, 15, 23, 59, 59, 999));

    // TODO(STREAMS-219)-PrivatePreview: Fix these and support year
    // timeUnit = StreamTimeUnitEnum::Year;
    // sizeInUnits = 1;
    // size = toMillis(sizeInUnits, timeUnit);
    // ASSERT_EQ(timeZone.createFromDateParts(2023, 1, 1, 0, 0, 0, 0),
    //           date(2023, 4, 5, 23, 59, 59, 999));
    // ASSERT_EQ(timeZone.createFromDateParts(2024, 1, 1, 0, 0, 0, 0),
    //           date(2024, 5, 22, 0, 0, 0, 0));

    timeUnit = StreamTimeUnitEnum::Hour;
    sizeInUnits = 1;
    StreamTimeUnitEnum hopTimeUnit = StreamTimeUnitEnum::Minute;
    int hopSizeInUnits = 1;
    auto makeHoppingWindowOp = [&]() {
        return std::make_unique<WindowOperator>(_context.get(),
                                                WindowOperator::Options{.size = sizeInUnits,
                                                                        .sizeUnit = timeUnit,
                                                                        .slide = hopSizeInUnits,
                                                                        .slideUnit = hopTimeUnit});
    };
    windowOp = makeHoppingWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 1, 31, 0, 0), date(2023, 4, 5, 2, 30, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 1, 31, 0, 0),
              date(2023, 4, 5, 2, 30, 30, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 1, 31, 0, 0),
              date(2023, 4, 5, 2, 30, 30, 30));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 1, 32, 0, 0),
              date(2023, 4, 5, 2, 31, 30, 30));

    timeUnit = StreamTimeUnitEnum::Hour;
    sizeInUnits = 1;
    hopTimeUnit = StreamTimeUnitEnum::Minute;
    hopSizeInUnits = 30;
    windowOp = makeHoppingWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 00, 0, 0), date(2023, 4, 5, 2, 31, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 00, 0, 0), date(2023, 4, 5, 2, 45, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 00, 0, 0), date(2023, 4, 5, 2, 50, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 30, 0, 0), date(2023, 4, 5, 3, 10, 0, 0));

    timeUnit = StreamTimeUnitEnum::Second;
    sizeInUnits = 30;
    hopTimeUnit = StreamTimeUnitEnum::Second;
    hopSizeInUnits = 1;
    windowOp = makeHoppingWindowOp();
    // We use the function under test for both:
    //  1) Determining the oldest window an event fits into
    //  2) Determining the oldest window that would not have been closed by a watermark
    // For our 30 second size, 1 second slide window:
    // Suppose we get a watermark at 2023-5-1 00:00:00.000. That will close the
    // [2023-4-30 23:59:30.000, 2023-5-01 00:00:00.000) window.
    // So, the oldest window that is not closed is 1 slide (1 second) later, at 2023-4-30
    // 23:59:31.000.
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 30, 23, 59, 31, 0),
              date(2023, 5, 1, 0, 0, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 30, 23, 59, 31, 0),
              date(2023, 5, 1, 0, 0, 0, 999));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 30, 23, 59, 31, 0),
              date(2023, 5, 1, 0, 0, 0, 111));
    timeUnit = StreamTimeUnitEnum::Second;
    sizeInUnits = 45;
    hopTimeUnit = StreamTimeUnitEnum::Second;
    hopSizeInUnits = 5;
    windowOp = makeHoppingWindowOp();
    // Suppose we get a watermark at 2023-5-1 01:00:00.000. That will close the
    // [2023-5-01 00:59:15.000, 2023-5-01 01:00:00.000) window.
    // So, the oldest window that is not closed is 1 slide (5 seconds) later, at 2023-5-01
    // 00:59:20.000.
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 1, 0, 59, 20, 0), date(2023, 5, 1, 1, 0, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 1, 0, 59, 20, 0), date(2023, 5, 1, 1, 0, 4, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 1, 0, 59, 25, 0), date(2023, 5, 1, 1, 0, 5, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 1, 0, 59, 25, 0), date(2023, 5, 1, 1, 0, 9, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 5, 1, 0, 59, 30, 0),
              date(2023, 5, 1, 1, 0, 10, 0));

    timeUnit = StreamTimeUnitEnum::Hour;
    sizeInUnits = 1;
    hopTimeUnit = StreamTimeUnitEnum::Minute;
    hopSizeInUnits = 59;
    windowOp = makeHoppingWindowOp();
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 16, 0, 0), date(2023, 4, 5, 2, 31, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 3, 15, 0, 0), date(2023, 4, 5, 3, 31, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 4, 14, 0, 0), date(2023, 4, 5, 4, 31, 0, 0));

    // Test hop size greater than window size.
    timeUnit = StreamTimeUnitEnum::Minute;
    sizeInUnits = 2;
    hopTimeUnit = StreamTimeUnitEnum::Minute;
    hopSizeInUnits = 5;
    windowOp = makeHoppingWindowOp();

    // It can be the case that our date doesn't belong to any window (that is, it doesn't fit in the
    // window starting at the oldest time).
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 33, 0, 0), date(2023, 4, 5, 2, 30, 0, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 33, 0, 0),
              date(2023, 4, 5, 2, 30, 10, 0));
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 33, 0, 0),
              date(2023, 4, 5, 2, 30, 10, 10));

    // When the date advances far enough, our time will fit in the window.
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 33, 0, 0),
              date(2023, 4, 5, 2, 34, 10, 10));

    // When it advances again, it will not.
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 5, 2, 38, 0, 0), date(2023, 4, 5, 2, 35, 0, 0));
}

/**
 * Sends watermarks that shouldn't close any windows.
 * Verifies no windows are closed.
 * Then sends a watermark that should close the window and verifies.
 */
TEST_F(WindowOperatorTest, EpochWatermarks) {
    auto bsonVector = innerPipeline();
    WindowOperator::Options options{
        bsonVector,
        3600,
        StreamTimeUnitEnum::Second,
        3600,
        StreamTimeUnitEnum::Second,
    };

    WindowOperator op(_context.get(), options);
    InMemorySourceOperator source(_context.get(), InMemorySourceOperator::Options());
    InMemorySinkOperator sink(_context.get(), 1);
    source.addOutput(&op, 0);
    op.addOutput(&sink, 0);
    source.start();
    sink.start();

    auto epoch = Date_t::fromMillisSinceEpoch(0);

    auto results = getResults(&source,
                              &sink,
                              std::vector<StreamMsgUnion>{generateDataMsg(date(0, 0, 0, 0)),
                                                          generateControlMessage(epoch)});
    ASSERT_EQ(1, results.size());
    ASSERT(results[0].controlMsg);

    std::vector<StreamMsgUnion> inputs{
        generateControlMessage(epoch),
        generateDataMsg(date(0, 0, 0, 0)),
        generateControlMessage(date(0, 0, 0, 0)),
        generateDataMsg(date(0, 0, 0, 1)),
        generateControlMessage(date(0, 0, 0, 1)),
        generateDataMsg(date(0, 0, 1, 0)),
        generateControlMessage(date(0, 0, 1, 0)),
        generateDataMsg(date(0, 0, 1, 1)),
        generateControlMessage(date(0, 0, 1, 1)),
        generateDataMsg(date(0, 0, 35, 999)),
        generateControlMessage(date(0, 0, 35, 999)),
        generateDataMsg(date(0, 59, 35, 999)),
        generateDataMsg(date(0, 59, 35, 999)),
        generateDataMsg(date(0, 59, 35, 999)),

        generateDataMsg(date(0, 59, 35, 999)),
        generateDataMsg(date(0, 59, 59, 999)),
        generateDataMsg(date(1, 0, 0, 999)),
    };
    results = getResults(&source, &sink, inputs);
    ASSERT_EQ(6, results.size());
    for (auto& result : results) {
        ASSERT(result.controlMsg);
    }

    results = getResults(
        &source, &sink, std::vector<StreamMsgUnion>{generateControlMessage(date(1, 0, 0, 0))});
    ASSERT_EQ(2, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].controlMsg);
}

/**
 * Sends watermarks that shouldn't close any windows.
 * Verifies no windows are closed.
 * Then sends a sequence of watermarks that progressively close more and more windows.
 */
TEST_F(WindowOperatorTest, EpochWatermarksHoppingWindow) {
    auto bsonVector = innerPipeline();
    WindowOperator::Options options{
        bsonVector,
        3600,
        StreamTimeUnitEnum::Second,
        600,
        StreamTimeUnitEnum::Second,
    };

    WindowOperator op(_context.get(), options);
    InMemorySourceOperator source(_context.get(), InMemorySourceOperator::Options());
    InMemorySinkOperator sink(_context.get(), 1);
    source.addOutput(&op, 0);
    op.addOutput(&sink, 0);
    source.start();
    sink.start();

    auto epoch = Date_t::fromMillisSinceEpoch(0);

    auto results = getResults(&source,
                              &sink,
                              std::vector<StreamMsgUnion>{generateDataMsg(date(0, 0, 0, 0)),
                                                          generateControlMessage(epoch)});
    ASSERT_EQ(1, results.size());
    ASSERT(results[0].controlMsg);

    std::vector<StreamMsgUnion> inputs{
        generateControlMessage(epoch),
        generateDataMsg(date(1, 0, 0, 1)),         // Opens 6 new windows.
        generateControlMessage(date(0, 0, 0, 2)),  // This control message won't close any windows
        generateDataMsg(date(1, 10, 0, 0)),        // Opens 2 new windows
        generateControlMessage(date(0, 0, 0, 2)),  // This control message won't close any windows
        generateDataMsg(date(1, 5, 0, 0)),         // No new windows
        generateControlMessage(date(0, 0, 0, 3)),  // This control message won't close any windows
        generateDataMsg(date(1, 7, 0, 0)),         // No new windows
        generateControlMessage(date(0, 0, 0, 4)),  // This control message won't close any windows
        generateDataMsg(date(1, 15, 0, 0)),        // Opens 1 new window
        generateControlMessage(date(0, 0, 0, 5)),  // This control message won't close any windows
        generateDataMsg(date(1, 30, 0, 0)),        // Opens 2 new windows
        generateControlMessage(date(0, 0, 0, 8)),  // This control message won't close any windows
    };

    results = getResults(&source, &sink, inputs);
    ASSERT_EQ(7, results.size());
    for (auto& result : results) {
        ASSERT(result.controlMsg);
    }

    // Now, generate control messages to close some of our windows.

    // This will close the 6 windows created by our first data message.
    results = getResults(
        &source, &sink, std::vector<StreamMsgUnion>{generateControlMessage(date(1, 0, 0, 0))});

    ASSERT_EQ(7, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].dataMsg);
    ASSERT(results[2].dataMsg);
    ASSERT(results[3].dataMsg);
    ASSERT(results[4].dataMsg);
    ASSERT(results[5].dataMsg);
    ASSERT(results[6].controlMsg);

    // This will close 2 more windows.
    results = getResults(
        &source, &sink, std::vector<StreamMsgUnion>{generateControlMessage(date(1, 25, 0, 1))});
    ASSERT_EQ(3, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].dataMsg);
    ASSERT(results[2].controlMsg);

    // This will close the rest.
    results = getResults(
        &source, &sink, std::vector<StreamMsgUnion>{generateControlMessage(date(10, 0, 0, 1))});
    ASSERT_EQ(8, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].dataMsg);
    ASSERT(results[2].dataMsg);
    ASSERT(results[3].dataMsg);
    ASSERT(results[4].dataMsg);
    ASSERT(results[5].dataMsg);
    ASSERT(results[6].dataMsg);
    ASSERT(results[7].controlMsg);
}

/**
 * Verify an inner pipeline that does not contain a blocking stage.
 * I.e. no $group or $sort, instead just an inner pipeline like [$match].
 */
TEST_F(WindowOperatorTest, InnerPipelineMatch) {
    auto results = commonInnerTest(
        R"(
            [
                {$match: {id: 1}}
            ]
        )",
        StreamTimeUnitEnum::Second,
        1,
        {generateDataMsg(date(0, 0, 0, 0), 0),
         generateDataMsg(date(0, 0, 0, 100), 1),
         generateDataMsg(date(0, 0, 0, 999), 1),
         generateDataMsg(date(0, 0, 0, 500), 2),
         generateControlMessage(date(0, 0, 1, 0))});

    ASSERT_EQ(2, results.size());
    ASSERT(results[0].dataMsg);
    auto windowResults = *results[0].dataMsg;
    ASSERT_EQ(2, windowResults.docs.size());
    ASSERT(results[1].controlMsg);
}

TEST_F(WindowOperatorTest, InnerHoppingPipelineMatch) {
    auto results = commonHoppingInnerTest(
        R"(
            [
                {$match: {id: 1}}
            ]
        )",
        StreamTimeUnitEnum::Second,
        3,
        StreamTimeUnitEnum::Second,
        1,
        {generateDataMsg(date(0, 0, 0, 0), 0),
         generateDataMsg(date(0, 0, 0, 100), 1),
         generateDataMsg(date(0, 0, 0, 999), 1),
         generateDataMsg(date(0, 0, 0, 500), 2),
         generateDataMsg(date(0, 0, 1, 500), 1),
         generateDataMsg(date(0, 0, 1, 501), 2),
         generateDataMsg(date(0, 0, 1, 502), 1),
         generateDataMsg(date(0, 0, 1, 503), 2),
         generateDataMsg(date(0, 0, 2, 100), 1),
         generateDataMsg(date(0, 0, 2, 110), 2),
         generateDataMsg(date(0, 0, 3, 0), 1),
         generateDataMsg(date(0, 0, 3, 10), 1),
         generateDataMsg(date(0, 0, 3, 20), 1),
         generateDataMsg(date(0, 0, 3, 30), 1),
         generateDataMsg(date(0, 0, 3, 40), 1),
         generateDataMsg(date(0, 0, 2, 110), 2),
         generateControlMessage(date(1, 00, 0, 0))});  // Close all windows.

    ASSERT_EQ(7, results.size());
    ASSERT(results[0].dataMsg);  // 58 -> 01
    auto windowResults = *results[0].dataMsg;
    ASSERT_EQ(2, windowResults.docs.size());
    ASSERT(results[1].dataMsg);  // 59 -> 02
    windowResults = *results[1].dataMsg;
    ASSERT_EQ(4, windowResults.docs.size());
    ASSERT(results[2].dataMsg);  // 00 -> 03
    windowResults = *results[2].dataMsg;
    ASSERT_EQ(5, windowResults.docs.size());
    ASSERT(results[3].dataMsg);  // 01 -> 04
    windowResults = *results[3].dataMsg;
    ASSERT_EQ(8, windowResults.docs.size());
    ASSERT(results[4].dataMsg);  // 02 -> 05
    windowResults = *results[4].dataMsg;
    ASSERT_EQ(6, windowResults.docs.size());
    ASSERT(results[5].dataMsg);  // 03 -> 06
    windowResults = *results[5].dataMsg;
    ASSERT_EQ(5, windowResults.docs.size());
    ASSERT(results[6].controlMsg);
}

/**
 * This test validates a [$source, $match, $tumblingWindow, ...] pipeline.
 * A colleague found this repro and this data is copied from their setup.
 * The streaming results are compared to the equivalent agg pipeline results.
 */
TEST_F(WindowOperatorTest, MatchBeforeWindow) {
    std::string jsonInput = R"([
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:02:04.066143"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:02:12.165984"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:02:20.062839"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:02:21.601736"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:02:27.082192"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:02:30.516536"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:02:39.382811"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:02:48.402636"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:02:58.055808"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:03:05.119759"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 1, "Corner_Num": 4, "timestamp": "2023-04-10T17:03:07.824636"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 1, "Corner_Num": 4, "timestamp": "2023-04-10T17:03:09.567833"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 1, "Corner_Num": 4, "timestamp": "2023-04-10T17:03:14.256870"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 1, "Corner_Num": 4, "timestamp": "2023-04-10T17:03:15.481246"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 1, "Corner_Num": 4, "timestamp": "2023-04-10T17:03:16.554121"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 2, "Corner_Num": 1, "timestamp": "2023-04-10T17:03:18.792067"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 2, "Corner_Num": 1, "timestamp": "2023-04-10T17:03:28.295972"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 2, "Corner_Num": 1, "timestamp": "2023-04-10T17:03:33.168663"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 2, "Corner_Num": 1, "timestamp": "2023-04-10T17:03:38.496602"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 2, "Corner_Num": 1, "timestamp": "2023-04-10T17:03:45.303118"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 2, "Corner_Num": 2, "timestamp": "2023-04-10T17:03:54.889120"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 2, "Corner_Num": 2, "timestamp": "2023-04-10T17:04:01.741715"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 2, "Corner_Num": 2, "timestamp": "2023-04-10T17:04:05.482742"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 2, "Corner_Num": 2, "timestamp": "2023-04-10T17:04:12.183735"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 2, "Corner_Num": 2, "timestamp": "2023-04-10T17:04:14.483877"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 2, "Corner_Num": 3, "timestamp": "2023-04-10T17:04:20.421403"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 2, "Corner_Num": 3, "timestamp": "2023-04-10T17:04:25.766772"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 2, "Corner_Num": 3, "timestamp": "2023-04-10T17:04:34.611272"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 2, "Corner_Num": 3, "timestamp": "2023-04-10T17:04:44.233487"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 2, "Corner_Num": 3, "timestamp": "2023-04-10T17:04:50.427515"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 2, "Corner_Num": 4, "timestamp": "2023-04-10T17:04:50.938945"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 2, "Corner_Num": 4, "timestamp": "2023-04-10T17:04:55.874214"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 2, "Corner_Num": 4, "timestamp": "2023-04-10T17:05:03.730324"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 2, "Corner_Num": 4, "timestamp": "2023-04-10T17:05:09.226797"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:05:43.074669"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:05:50.990649"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:05:53.471270"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:05:56.523219"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 1, "Corner_Num": 2, "timestamp": "2023-04-10T17:06:00.690231"},
        {"Racer_Num": 5, "Racer_Name": "Go Mifune", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:06:09.243790"},
        {"Racer_Num": 11, "Racer_Name": "Captain Terror", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:06:09.985842"},
        {"Racer_Num": 12, "Racer_Name": "Snake Oiler", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:06:11.699844"},
        {"Racer_Num": 9, "Racer_Name": "Race X", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:06:15.566233"},
        {"Racer_Num": 0, "Racer_Name": "Pace Car", "lap": 1, "Corner_Num": 3, "timestamp": "2023-04-10T17:06:16.766648"}
    ])";

    std::string pipeline = R"(
[
    { $source: {
        connectionName: "kafka1",
        topic: "thunderhead_race",
        timeField : { $dateFromString : { "dateString" : "$timestamp"} },
        testOnlyPartitionCount: 1
    }},
    {
        $match: { "Racer_Name" : { "$ne" : "Pace Car" } }
    },
    {
        $tumblingWindow: {
            interval: {size: 5, unit: "second"},
            pipeline: [
                {
                    $group: {
                        "_id" : { "Racer_Num" : "$Racer_Num", "Racer_Name" : "$Racer_Name"},
                        "racer_status" : { $top: {
                            output: [ "$lap","$Corner_Num","$timestamp"],
                            sortBy: { "lap": -1, "Corner_Num" : -1, "timestamp": 1 }
                        } }
                    }
                },
                {
                    $project: {
                      "_id" : 0,
                      "Racer_Name" : "$_id.Racer_Name",
                      "Racer_Num" : "$_id.Racer_Num",
                      "Lap" : { $arrayElemAt : ["$racer_status", 0]},
                      "Corner" : { $arrayElemAt : ["$racer_status", 1]},
                      "Last_Update" : { $dateFromString : { "dateString" : { $arrayElemAt : ["$racer_status", 2]} } }
                    }
                 },
                 {
                    $sort : {
                        "Lap" : -1, "Corner" : -1, "Last_Update" : 1
                    }
                 }
            ]
         }
    },
    { $project: {
        "Racer_Name" : 1,
        "Racer_Num" : 1,
        "Lap" : 1,
        "Corner" : 1,
        "Last_Update" : 1
    }},
    {$emit: {"connectionName": "__testMemory"}}
]
    )";

    KafkaConnectionOptions kafkaOptions("");
    kafkaOptions.setIsTestKafka(true);
    mongo::Connection connection("kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
    Parser parser(_context.get(), /*options*/ {}, {{"kafka1", connection}});
    auto dag = parser.fromBson(parseBsonVector(pipeline));
    dag->start();
    dag->source()->connect();

    auto source = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
    auto consumers = this->kafkaGetConsumers(source);
    std::vector<KafkaSourceDocument> docs;
    auto inputDocs = fromjson(jsonInput);
    for (auto& doc : inputDocs) {
        KafkaSourceDocument sourceDoc;
        sourceDoc.doc = doc.Obj();
        docs.emplace_back(std::move(sourceDoc));
    }
    KafkaSourceDocument sourceDoc;
    sourceDoc.doc = fromjson(R"({"timestamp": "2023-04-10T18:00:00.000000"}))");
    docs.emplace_back(std::move(sourceDoc));
    consumers[0]->addDocuments(std::move(docs));

    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    kafkaRunOnce(source);
    auto results = toVector(sink->getMessages());
    std::vector<Document> streamResults;
    for (auto& result : results) {
        if (result.dataMsg) {
            for (auto& doc : result.dataMsg->docs) {
                // Remove _stream_meta field from the documents.
                MutableDocument mutableDoc{std::move(doc.doc)};
                mutableDoc.remove(kStreamsMetaField);
                streamResults.push_back(mutableDoc.freeze());
            }
        }
    }
    dag->stop();

    // Get pipeline results
    std::string aggPipelineJson = R"(
[
    { $addFields: {
        _ts : { $dateFromString : { "dateString" : "$timestamp"} }
    }},
    { $addFields: {
        minuteValue: { $minute: { date: "$_ts" } },
        secondValue: { $second: { date: "$_ts" } }
    }},
    {
        $match: { "Racer_Name" : { "$ne" : "Pace Car" } }
    },
    { $group: {
            "_id" : {
                "minuteValue" : "$minuteValue",
                "secondValue" : { $floor: { $divide: [ "$secondValue", 5 ] } },
                "Racer_Num" : "$Racer_Num",
                "Racer_Name" : "$Racer_Name"
            },
            "racer_status" : { $top: {
                output: [ "$lap","$Corner_Num","$timestamp"],
                sortBy: { "lap": -1, "Corner_Num" : -1, "timestamp": 1 }
            } }
        }
    },
    { $project: {
            "minuteValue": "$_id.minuteValue",
            "secondValue": "$_id.secondValue",
            "Racer_Name" : "$_id.Racer_Name",
            "Racer_Num" : "$_id.Racer_Num",
            "Lap" : { $arrayElemAt : ["$racer_status", 0]},
            "Corner" : { $arrayElemAt : ["$racer_status", 1]},
            "Last_Update" : { $dateFromString : { "dateString" : { $arrayElemAt : ["$racer_status", 2]} } }
    }},
    { $sort : {
            "minuteValue": 1, "secondValue": 1, "Lap" : -1, "Corner" : -1, "Last_Update" : 1
    }},
    { $project: {
        "_id" : 0,
        "Racer_Name" : 1,
        "Racer_Num" : 1,
        "Lap" : 1,
        "Corner" : 1,
        "Last_Update" : 1
    }}
]
    )";

    // Setup the pipeline with a feeder containing {input}.
    auto aggPipeline = Pipeline::parse(parseBsonVector(aggPipelineJson), _context->expCtx);
    auto feeder = boost::intrusive_ptr<DocumentSourceFeeder>(
        new DocumentSourceFeeder(aggPipeline->getContext()));
    feeder->setEndOfBufferSignal(DocumentSource::GetNextResult::makeEOF());
    for (auto& doc : inputDocs) {
        feeder->addDocument(Document(doc.Obj()));
    }
    aggPipeline->addInitialSource(feeder);
    std::vector<Document> pipelineResults;
    auto result = aggPipeline->getSources().back()->getNext();
    while (result.isAdvanced()) {
        pipelineResults.emplace_back(std::move(result.getDocument()));
        result = aggPipeline->getSources().back()->getNext();
    }
    ASSERT(result.isEOF());

    ASSERT_EQ(streamResults.size(), pipelineResults.size());
    for (size_t i = 0; i < streamResults.size(); i++) {
        ASSERT_VALUE_EQ(Value(streamResults[i]), Value(pipelineResults[i]));
    }
}

TEST_F(WindowOperatorTest, LateData) {
    // The third document is late and should be dis-regarded.
    // The 4th document will advance the watermark and close the 02:20-25 window.
    std::string jsonInput = R"([
        {"id": 12, "timestamp": "2023-04-10T17:02:20.062839"},
        {"id": 12, "timestamp": "2023-04-10T17:02:24.062000"},
        {"id": 12, "timestamp": "2023-04-10T17:02:19.062000"},
        {"id": 12, "timestamp": "2023-04-10T17:02:25.100000"}
    ])";

    std::string pipeline = R"(
[
    {
        $tumblingWindow: {
            interval: {size: 5, unit: "second"},
            pipeline: [
                {
                    $match: { "id" : 12 }
                }
            ]
        }
    }
]
    )";

    std::vector<BSONObj> inputDocs;
    auto inputBson = fromjson(jsonInput);
    for (auto& doc : inputBson) {
        inputDocs.push_back(doc.Obj());
    }
    auto [results, dlqMsgs] = commonKafkaInnerTest(inputDocs, pipeline);

    // Verify there is only 1 window and 1 control message.
    ASSERT_EQ(2, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].controlMsg);
    // The 1 window should have two document results.
    ASSERT_EQ(2, results[0].dataMsg->docs.size());
    for (auto& doc : results[0].dataMsg->docs) {
        auto [start, end] = getBoundaries(doc);
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 20, 0), start);
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 25, 0), end);
        // Verify the doc.minEventTimestampMs matches the event times observed
        auto min = doc.minEventTimestampMs;
        auto max = doc.maxEventTimestampMs;
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 20, 62),
                  Date_t::fromMillisSinceEpoch(min));
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 24, 62),
                  Date_t::fromMillisSinceEpoch(max));
    }

    // Verify the DLQ has 1 message.
    ASSERT_EQ(1, dlqMsgs.size());
    auto dlqDoc = std::move(dlqMsgs.front());
    ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 19, 62),
              dlqDoc["fullDocument"]["_ts"].Date());
    ASSERT_BSONOBJ_EQ(fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:19.062000"})"),
                      dlqDoc["fullDocument"].Obj().removeField("_ts"));
    ASSERT_EQ("Input document arrived late", dlqDoc["errInfo"]["reason"].String());
}

TEST_F(WindowOperatorTest, WindowPlusOffset) {
    // The 3rd document will advance the watermark and close the 02:22-32 window.
    std::string jsonInput = R"([
        {"id": 12, "timestamp": "2023-04-10T17:02:22.062839"},
        {"id": 12, "timestamp": "2023-04-10T17:02:31.062000"},
        {"id": 12, "timestamp": "2023-04-10T17:02:32.100000"}
    ])";

    std::string pipeline = R"(
[
    {
        $tumblingWindow: {
            interval: {size: 10, unit: "second"},
            offset: {offsetFromUtc: 2, unit: "second"},
            pipeline: [
                {
                    $match: { "id" : 12 }
                }
            ]
        }
    }
]
    )";

    std::vector<BSONObj> inputDocs;
    auto inputBson = fromjson(jsonInput);
    for (auto& doc : inputBson) {
        inputDocs.push_back(doc.Obj());
    }
    auto [results, dlqMsgs] = commonKafkaInnerTest(inputDocs, pipeline);

    // Verify there is only 1 window and 1 control message.
    ASSERT_EQ(2, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].controlMsg);
    // The 1 window should have two document results.
    ASSERT_EQ(2, results[0].dataMsg->docs.size());
    for (auto& doc : results[0].dataMsg->docs) {
        auto [start, end] = getBoundaries(doc);
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 22, 0), start);
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 32, 0), end);
        // Verify the doc.minEventTimestampMs matches the event times observed
        auto min = doc.minEventTimestampMs;
        auto max = doc.maxEventTimestampMs;
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 22, 62),
                  Date_t::fromMillisSinceEpoch(min));
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 31, 62),
                  Date_t::fromMillisSinceEpoch(max));
    }
}

TEST_F(WindowOperatorTest, WindowMinusOffset) {
    // The 3rd document will advance the watermark and close the 02:18-28 window.
    std::string jsonInput = R"([
        {"id": 12, "timestamp": "2023-04-10T17:02:18.062839"},
        {"id": 12, "timestamp": "2023-04-10T17:02:27.062000"},
        {"id": 12, "timestamp": "2023-04-10T17:02:28.100000"}
    ])";

    std::string pipeline = R"(
[
    {
        $tumblingWindow: {
            interval: {size: 10, unit: "second"},
            offset: {offsetFromUtc: -2, unit: "second"},
            pipeline: [
                {
                    $match: { "id" : 12 }
                }
            ]
        }
    }
]
    )";

    std::vector<BSONObj> inputDocs;
    auto inputBson = fromjson(jsonInput);
    for (auto& doc : inputBson) {
        inputDocs.push_back(doc.Obj());
    }
    auto [results, dlqMsgs] = commonKafkaInnerTest(inputDocs, pipeline);

    // Verify there is only 1 window and 1 control message.
    ASSERT_EQ(2, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].controlMsg);
    // The 1 window should have two document results.
    ASSERT_EQ(2, results[0].dataMsg->docs.size());
    for (auto& doc : results[0].dataMsg->docs) {
        auto [start, end] = getBoundaries(doc);
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 18, 0), start);
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 28, 0), end);
        // Verify the doc.minEventTimestampMs matches the event times observed
        auto min = doc.minEventTimestampMs;
        auto max = doc.maxEventTimestampMs;
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 18, 62),
                  Date_t::fromMillisSinceEpoch(min));
        ASSERT_EQ(timeZone.createFromDateParts(2023, 4, 10, 17, 2, 27, 62),
                  Date_t::fromMillisSinceEpoch(max));
    }
}

/**
 * WindowPipeline returns results in chunks of 1024. This test verifies when a
 * window outputs more than 1024 documents, things still work.
 */
TEST_F(WindowOperatorTest, LargeChunks) {
    const int numDocs = 1024 * 4 + 1;
    std::vector<BSONObj> input{};
    for (int i = 0; i < numDocs; i++) {
        input.push_back(BSON("id" << 12 << "timestamp"
                                  << "2023-04-10T17:02:20.062839"));
    }
    input.push_back(BSON("id" << 12 << "timestamp"
                              << "2023-04-10T17:10:00.000000"));

    std::string pipeline = R"(
[
    {
        $tumblingWindow: {
            interval: {size: 5, unit: "second"},
            pipeline: [{ $match: { "id" : 12 }}]
        }
    }
]
    )";

    auto [results, _] = commonKafkaInnerTest(input, pipeline, input.size());

    std::vector<BSONObj> bsonResults = {};
    for (auto& result : results) {
        if (result.dataMsg) {
            for (auto& doc : result.dataMsg->docs) {
                bsonResults.push_back(doc.doc.toBson());
            }
        }
    }

    ASSERT_EQ(numDocs, bsonResults.size());
    for (size_t i = 0; i < bsonResults.size(); i++) {
        ASSERT_BSONOBJ_EQ(input[i],
                          bsonResults[i].removeField(kStreamsMetaField).removeField("_ts"));
    }
}

/**
 * Creates documents with timing information that uses the system clock.
 * Verifies that the resulting tumbling windows have no gaps and don't overlap.
 */
TEST_F(WindowOperatorTest, WallclockTime) {
    auto innerTest = [&](int size, StreamTimeUnitEnum unit) {
        // Each innerTest runs for ~1200 milliseconds of wallclock time.
        const Milliseconds testDuration(1200);
        std::vector<BSONObj> bsonVector = {fromjson(R"(
        { $group: {
            _id: null,
            sum: { $sum: "$a" }
        }}
        )")};
        WindowOperator::Options options{
            bsonVector,
            size,
            unit,
            size,
            unit,
        };

        const int numPartitions = 1;
        KafkaConsumerOperator::Options sourceOptions;
        sourceOptions.isTest = true;
        sourceOptions.useWatermarks = true;
        sourceOptions.testOnlyNumPartitions = numPartitions;
        auto kafkaConsumerOperator =
            std::make_unique<KafkaConsumerOperator>(_context.get(), std::move(sourceOptions));

        WindowOperator op(_context.get(), options);
        InMemorySinkOperator sink(_context.get(), 1);
        kafkaConsumerOperator->addOutput(&op, 0);
        op.addOutput(&sink, 0);

        kafkaConsumerOperator->start();
        kafkaConsumerOperator->connect();
        auto consumers = kafkaGetConsumers(kafkaConsumerOperator.get());

        std::vector<KafkaSourceDocument> actualInput = {};
        auto start = getCurrentMillis();
        auto offset = 0;
        const int docsPerChunk = 10;
        auto diffMS = getCurrentMillis() - start;
        while (diffMS < testDuration.count()) {
            std::vector<KafkaSourceDocument> docs;
            auto officialTime = start + diffMS;
            for (int i = 0; i < docsPerChunk; i++) {
                KafkaSourceDocument sourceDoc;
                sourceDoc.doc = BSON("a" << 1);
                sourceDoc.partition = 0;
                sourceDoc.offset = offset++;
                sourceDoc.logAppendTimeMs = officialTime;
                actualInput.push_back(sourceDoc);
                docs.emplace_back(sourceDoc);
            }
            consumers[0]->addDocuments(docs);

            kafkaRunOnce(kafkaConsumerOperator.get());

            diffMS = getCurrentMillis() - start;
        }
        ASSERT_GT(actualInput.size(), 0);
        // Determine the last watermark from the input events.
        int64_t lastWatermarkTime = *actualInput.back().logAppendTimeMs - 1;
        // Based on the watermark, determine the windows expected to close.
        std::vector<int64_t> expectedWindowStartTimes;
        for (const auto& input : actualInput) {
            auto startTime = toOldestWindowStartTime(*input.logAppendTimeMs, &op);
            auto endTime = startTime + toMillis(size, unit);
            if (lastWatermarkTime >= endTime &&
                (expectedWindowStartTimes.empty() ||
                 expectedWindowStartTimes.back() != startTime)) {
                expectedWindowStartTimes.push_back(startTime);
            }
        }

        auto results = toVector(sink.getMessages());
        std::vector<StreamDataMsg> windowResults;
        int64_t countResultDocs = 0;
        for (auto& msg : results) {
            if (msg.dataMsg) {
                windowResults.emplace_back(*msg.dataMsg);
                countResultDocs += msg.dataMsg->docs.size();
            }
        }

        auto [firstWindowStart, firstWindowEnd] = getBoundaries(windowResults[0].docs[0]);

        // Verify first window start is aligned with the epoch.
        ASSERT_EQ(0, firstWindowStart.toMillisSinceEpoch() % toMillis(size, unit));

        // Verify the windows match the expected results.
        ASSERT_EQ(expectedWindowStartTimes.size(), countResultDocs);
        int idx = 0;
        for (auto& msg : windowResults) {
            for (auto& doc : msg.docs) {
                Date_t expectedBegin =
                    Date_t::fromMillisSinceEpoch(expectedWindowStartTimes[idx++]);
                Date_t expectedEnd = Date_t::fromMillisSinceEpoch(
                    expectedBegin.toMillisSinceEpoch() + toMillis(size, unit));
                auto [begin, end] = getBoundaries(doc);
                ASSERT_EQUALS(begin, expectedBegin);
                ASSERT_EQUALS(end, expectedEnd);
            }
        }
    };

    innerTest(1, StreamTimeUnitEnum::Second);
    innerTest(100, StreamTimeUnitEnum::Millisecond);
    innerTest(250, StreamTimeUnitEnum::Millisecond);
    innerTest(333, StreamTimeUnitEnum::Millisecond);
}

TEST_F(WindowOperatorTest, WindowMeta) {
    auto dataMsg = [](Date_t date, int id) {
        StreamDocument streamDoc(
            Document(BSON("date" << date << "id" << id << kStreamsMetaField << BSON("a" << 1))));
        streamDoc.minProcessingTimeMs = date.toMillisSinceEpoch();
        streamDoc.minEventTimestampMs = date.toMillisSinceEpoch();
        streamDoc.maxEventTimestampMs = date.toMillisSinceEpoch();
        return StreamMsgUnion{StreamDataMsg{{std::move(streamDoc)}}};
    };

    auto results = commonInnerTest(
        R"(
            [
                {$match: {id: 1}}
            ]
        )",
        StreamTimeUnitEnum::Second,
        1,
        {dataMsg(date(0, 0, 0, 0), 0),
         dataMsg(date(0, 0, 0, 100), 1),
         dataMsg(date(0, 0, 0, 999), 1),
         dataMsg(date(0, 0, 0, 500), 2),
         generateControlMessage(date(0, 0, 1, 0))});

    // Validate the overall results makes sense.
    ASSERT_EQ(2, results.size());
    ASSERT(results[0].dataMsg);
    ASSERT(results[1].controlMsg);
    auto windowResults = *results[0].dataMsg;
    ASSERT_EQ(2, windowResults.docs.size());

    auto& streamMeta = windowResults.docs[0].streamMeta;
    ASSERT_EQ(date(0, 0, 0, 0), streamMeta.getWindowStartTimestamp());
    ASSERT_EQ(date(0, 0, 1, 0), streamMeta.getWindowEndTimestamp());
}

TEST_F(WindowOperatorTest, DeadLetterQueue) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ testInMemoryConnectionRegistry());
    std::string _basePipeline = R"(
[
    { $source: {
        connectionName: "__testMemory",
        allowedLateness: { size: 10, unit: "second" }
    }},
    { $tumblingWindow: {
      interval: { size: 1, unit: "second" },
      pipeline:
      [
        { $group: {
            _id: "$id",
            sum: { $sum: "$value" }
        }},
        { $sort: { _id: 1 }},
        { $project: { sizes: { $divide: ["$sum", "$_id"] }}}
      ]
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";
    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + _basePipeline + "}")["pipeline"]));
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());
    dag->start();

    StreamDataMsg inputs{{
        generateDocMs(1, 0, 1),
        generateDocMs(1, 0, 2),
        generateDocMs(1, 0, 3),
    }};
    StreamControlMsg controlMsg{WatermarkControlMsg{
        WatermarkStatus::kActive,
        Date_t::fromDurationSinceEpoch(stdx::chrono::seconds(1)).toMillisSinceEpoch()}};
    source->addDataMsg(std::move(inputs), std::move(controlMsg));
    source->runOnce();

    auto results = toVector(sink->getMessages());
    ASSERT_EQ(1, results.size());
    ASSERT_FALSE(results[0].dataMsg);
    ASSERT_TRUE(results[0].controlMsg);

    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    ASSERT_EQ(1, dlqMsgs.size());
    auto dlqDoc = std::move(dlqMsgs.front());
    ASSERT_EQ(
        "Failed to process input document in ProjectOperator with error: "
        "can't $divide by zero",
        dlqDoc["errInfo"]["reason"].String());
    ASSERT_BSONOBJ_EQ(BSON("windowStartTimestamp" << Date_t::fromMillisSinceEpoch(0)
                                                  << "windowEndTimestamp"
                                                  << Date_t::fromMillisSinceEpoch(1000)),
                      dlqDoc["_stream_meta"].Obj());
}

TEST_F(WindowOperatorTest, OperatorId) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ testInMemoryConnectionRegistry());
    std::string _basePipeline = R"(
[
    { $source: { connectionName: "__testMemory" }},
    { $hoppingWindow: {
      interval: { size: 3, unit: "second" },
      hopSize: { size: 1, unit: "second" },
      pipeline:
      [
        { $group: {
            _id: "$id",
            sum: { $sum: "$value" }
        }},
        { $project: { sizes: { $divide: ["$sum", "$_id"] }}}
      ]
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";
    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + _basePipeline + "}")["pipeline"]));
    auto& ops = dag->operators();
    auto source = dynamic_cast<InMemorySourceOperator*>(ops[0].get());
    dag->start();

    // Verify the main pipeline operators.
    ASSERT_EQ(0, ops[0]->getOperatorId());
    ASSERT_EQ(1, ops[1]->getOperatorId());
    ASSERT_EQ(5, ops[2]->getOperatorId());
    // Send input that will open three windows.
    StreamDataMsg inputs{{
        generateDocSeconds(0, 0, 1),
        generateDocSeconds(2, 0, 2),
    }};
    source->addDataMsg(std::move(inputs));
    source->runOnce();
    // Verify the Operator IDs of all window's inner operators.
    auto windowOp = dynamic_cast<WindowOperator*>(ops[1].get());
    auto& windows = getWindows(*windowOp);
    ASSERT_EQ(3, windows.size());
    for (auto& [key, value] : windows) {
        auto& innerOps = getInnerOperators(value.pipeline);
        ASSERT_EQ(3, innerOps.size());

        ASSERT_EQ(2, innerOps[0]->getOperatorId());
        ASSERT_EQ("GroupOperator", innerOps[0]->getName());
        ASSERT_EQ(3, innerOps[1]->getOperatorId());
        ASSERT_EQ("ProjectOperator", innerOps[1]->getName());
        ASSERT_EQ(4, innerOps[2]->getOperatorId());
        ASSERT_EQ("CollectOperator", innerOps[2]->getName());
    }
}

TEST_F(WindowOperatorTest, Checkpointing_FastMode_TumblingWindow) {
    WindowOperator::Options options{
        innerPipeline(),
        1,
        StreamTimeUnitEnum::Second,
        1,
        StreamTimeUnitEnum::Second,
    };
    int64_t windowSizeMs = 1000;
    auto metricManager = std::make_unique<MetricManager>();
    auto context = getTestContext(_serviceContext, _metricManager.get());
    context->checkpointStorage =
        makeCheckpointStorage(_serviceContext, UUID::gen().toString(), UUID::gen().toString());
    CheckpointId checkpointId = context->checkpointStorage->createCheckpointId();
    OperatorId operatorId{1};

    WindowOperatorStateFastMode state{2000};
    context->checkpointStorage->addState(checkpointId, operatorId, state.toBSON(), 0);
    context->restoreCheckpointId = checkpointId;

    // Verify after restore, windows before minimum are ignored.
    WindowOperator op(context.get(), options);
    op.setOperatorId(operatorId);
    InMemorySinkOperator sink(context.get(), 1);
    op.addOutput(&sink, 0);
    op.start();
    std::vector<StreamDocument> input = {generateDocSeconds(0, 0, 0),
                                         generateDocSeconds(1, 0, 0),
                                         generateDocSeconds(2, 0, 0),
                                         generateDocSeconds(3, 0, 0),
                                         generateDocSeconds(4, 0, 0)};
    op.onDataMsg(0,
                 StreamDataMsg{input},
                 StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                      WatermarkStatus::kActive, int64_t(input.size()) * 1000}});

    auto results = toVector(sink.getMessages());
    ASSERT_EQ(4 /* windows 2,3,4 and the control msg */, results.size());
    ASSERT_EQ(
        2000,
        results[0].dataMsg->docs[0].streamMeta.getWindowStartTimestamp()->toMillisSinceEpoch());
    ASSERT_EQ(
        4000,
        results[2].dataMsg->docs[0].streamMeta.getWindowStartTimestamp()->toMillisSinceEpoch());
    ASSERT(results[3].controlMsg);

    // Now, send a checkpoint message. There are no open windows.
    checkpointId = context->checkpointStorage->createCheckpointId();
    op.onControlMsg(0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpointId}});
    // Since there are no open windows, verify checkpoint1 gets committed.
    ASSERT_EQ(checkpointId, context->checkpointStorage->readLatestCheckpointId());
    CheckpointId checkpoint1 = checkpointId;
    // Send a few docs to open a window 5, 6, 7.
    input = {
        generateDocSeconds(5, 0, 1),
        generateDocSeconds(6, 0, 1),
        generateDocSeconds(7, 0, 1),
    };
    op.onDataMsg(0, StreamDataMsg{input});
    // Windows 5,6,7 are open when this checkpoint happens.
    // So verify we don't checkpoint2.
    auto checkpoint2 = context->checkpointStorage->createCheckpointId();
    op.onControlMsg(0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpoint2}});
    ASSERT_EQ(checkpoint1, context->checkpointStorage->readLatestCheckpointId());
    ASSERT_NE(checkpoint2, context->checkpointStorage->readLatestCheckpointId());
    // Now close window 5,6, send two more checkpoints, and checkpoint2-4 are still
    // not committed.
    op.onControlMsg(0,
                    StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive,
                                                                         int64_t(7) * 1000}});
    auto checkpoint3 = context->checkpointStorage->createCheckpointId();
    op.onControlMsg(0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpoint3}});
    auto checkpoint4 = context->checkpointStorage->createCheckpointId();
    op.onControlMsg(0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpoint4}});
    // Verify nothing has been committed.
    ASSERT_EQ(checkpoint1, context->checkpointStorage->readLatestCheckpointId());
    // Now close window 7 and verify the most recent checkpointId is committed.
    op.onControlMsg(0,
                    StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive,
                                                                         int64_t(8) * 1000}});
    ASSERT_EQ(checkpoint4, context->checkpointStorage->readLatestCheckpointId());
    verifyCommitted(context->checkpointStorage.get(), checkpoint3);
    // Open a few windows.
    std::vector<StreamDocument> afterCheckpoint4Input = {
        generateDocSeconds(8, 0, 1),
        generateDocSeconds(9, 0, 1),
    };
    op.onDataMsg(0, StreamDataMsg{afterCheckpoint4Input});
    // Send a checkpoint and verify it is not committed.
    auto checkpoint5 = context->checkpointStorage->createCheckpointId();
    op.onControlMsg(0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpoint5}});
    ASSERT_EQ(checkpoint4, context->checkpointStorage->readLatestCheckpointId());
    auto afterCheckpoint5Input = {
        generateDocSeconds(10, 0, 1),
        generateDocSeconds(11, 0, 1),
    };
    op.onDataMsg(0, StreamDataMsg{afterCheckpoint5Input});
    ASSERT_EQ(checkpoint4, context->checkpointStorage->readLatestCheckpointId());
    // Close window8 and window9, afterwards verify checkpoint5 is committed.
    op.onControlMsg(
        0,
        StreamControlMsg{
            .watermarkMsg = WatermarkControlMsg{
                WatermarkStatus::kActive,
                afterCheckpoint4Input.back().doc.getField("date").getDate().toMillisSinceEpoch() +
                    windowSizeMs}});
    ASSERT_EQ(checkpoint5, context->checkpointStorage->readLatestCheckpointId());
    ASSERT_EQ(10000,
              WindowOperatorStateFastMode::parseOwned(
                  IDLParserContext("test"),
                  *context->checkpointStorage->readState(checkpoint5, op.getOperatorId(), 0))
                  .getMinimumWindowStartTime());
}

TEST_F(WindowOperatorTest, BasicIdleness) {
    std::string jsonInput = R"([
        {"id": 1, "timestamp": "2023-04-10T17:02:20.061Z", "val": 1},
        {"id": 1, "timestamp": "2023-04-10T17:02:20.062Z", "val": 2},
        {"id": 2, "timestamp": "2023-04-10T17:02:20.063Z", "val": 3},
        {"id": 2, "timestamp": "2023-04-10T17:02:20.100Z", "val": 4}
    ])";

    std::string pipeline = R"(
[
     { $source: {
        connectionName: "kafka1",
        topic: "inputTopic",
        timeField : { $dateFromString : { "dateString" : "$timestamp"} },
        testOnlyPartitionCount: 2,
        idlenessTimeout: { size: 5, unit: "second" }
    }},
    {
        $tumblingWindow: {
            interval: { size: 3, unit: "second" },
            pipeline: [
                {
                    $group: {_id: "$id", sum: { $sum: 1 }, avg: { $avg: "$val" }}
                },
                { $sort: { _id: 1 }}
            ]
         }
    },
    {$emit: {"connectionName": "__testMemory"}}
]
    )";

    KafkaConnectionOptions kafkaOptions("");
    kafkaOptions.setIsTestKafka(true);
    mongo::Connection connection("kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
    Parser parser(_context.get(), /*options*/ {}, {{"kafka1", connection}});
    auto dag = parser.fromBson(parseBsonVector(pipeline));
    dag->start();
    dag->source()->connect();

    auto source = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
    auto consumers = this->kafkaGetConsumers(source);
    std::vector<KafkaSourceDocument> docs;
    auto inputDocs = fromjson(jsonInput);
    for (auto& doc : inputDocs) {
        KafkaSourceDocument sourceDoc;
        sourceDoc.doc = doc.Obj();
        docs.emplace_back(std::move(sourceDoc));
    }

    // By populating one of our consumers and leaving the other empty, we can simulate an idle
    // partition.
    consumers[0]->addDocuments(std::move(docs));

    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    kafkaRunOnce(source);
    auto results = toVector(sink->getMessages());

    // Initially, we shouldn't have any results (that is, we should have open windows but no data
    // msg results in our sink).
    ASSERT(!results.empty());
    for (auto res : results) {
        ASSERT(!res.dataMsg);
        ASSERT(res.controlMsg);
        ASSERT(res.controlMsg->watermarkMsg);
        ASSERT_EQ(res.controlMsg->watermarkMsg->watermarkStatus, WatermarkStatus::kActive);
    }

    // If we sleep for longer than the idleness period, both partitions should be marked as idle.
    sleepmillis(6 * 1000);

    kafkaRunOnce(source);
    results = toVector(sink->getMessages());

    // After our idleness period passes, we still shouldn't have any results. Moreover, our
    // partitions should both be marked as idle.
    ASSERT(!results.empty());
    for (auto res : results) {
        ASSERT(!res.dataMsg);
        ASSERT(res.controlMsg);
        ASSERT(res.controlMsg->watermarkMsg);
        ASSERT_EQ(res.controlMsg->watermarkMsg->watermarkStatus, WatermarkStatus::kIdle);
    }

    // Add another document to our first partition. This should allow for our earliest window to be
    // closed, as this event is 4 seconds after the previous four events (exceeding the size of the
    // window). Crucially, we should close the window even if our second partition is idle.
    KafkaSourceDocument sourceDoc;
    const auto controlTimestampString = "2023-04-10T17:02:24.100Z";
    const auto dateWithStatus = dateFromISOString(controlTimestampString);
    ASSERT_OK(dateWithStatus);

    // The watermark time is computed as the timestamp minus the default allowed lateness minus one.
    const auto expectedMillisSinceEpoch = dateWithStatus.getValue().toMillisSinceEpoch() - 3000 - 1;
    sourceDoc.doc = BSON("_id" << 3 << "val" << 10 << "timestamp" << controlTimestampString);
    consumers[0]->addDocuments({std::move(sourceDoc)});

    kafkaRunOnce(source);

    results = toVector(sink->getMessages());
    std::vector<Document> streamResults;
    std::vector<StreamControlMsg> streamControlMsgs;
    for (auto& result : results) {
        if (result.dataMsg) {
            for (auto& doc : result.dataMsg->docs) {
                // Remove _stream_meta field from the documents.
                MutableDocument mutableDoc{std::move(doc.doc)};
                mutableDoc.remove(kStreamsMetaField);
                streamResults.push_back(mutableDoc.freeze());
            }
        } else if (result.controlMsg) {
            streamControlMsgs.emplace_back(std::move(*result.controlMsg));
        }
    }

    ASSERT_EQ(streamResults.size(), 2);
    ASSERT_BSONOBJ_EQ(streamResults[0].toBson(), fromjson("{_id: 1, sum: 2, avg: 1.5}"));
    ASSERT_BSONOBJ_EQ(streamResults[1].toBson(), fromjson("{_id: 2, sum: 2, avg: 3.5}"));
    ASSERT_EQ(streamControlMsgs.size(), 1);
    ASSERT(streamControlMsgs[0].watermarkMsg);
    ASSERT_EQ(streamControlMsgs[0].watermarkMsg->watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQ(streamControlMsgs[0].watermarkMsg->eventTimeWatermarkMs, expectedMillisSinceEpoch);
}


TEST_F(WindowOperatorTest, AllPartitionsIdleInhibitsWindowsClosing) {
    std::string jsonInputOne = R"([
        {"id": 1, "timestamp": "2023-04-10T17:02:20.062Z", "val": 2},
        {"id": 2, "timestamp": "2023-04-10T17:02:20.100Z", "val": 4}
    ])";

    std::string jsonInputTwo = R"([
        {"id": 1, "timestamp": "2023-04-10T17:02:20.061Z", "val": 1},
        {"id": 2, "timestamp": "2023-04-10T17:02:20.063Z", "val": 3}
    ])";
    std::string pipeline = R"(
[
     { $source: {
        connectionName: "kafka1",
        topic: "inputTopic",
        timeField : { $dateFromString : { "dateString" : "$timestamp"} },
        testOnlyPartitionCount: 2,
        idlenessTimeout: { size: 5, unit: "second" }
    }},
    {
        $tumblingWindow: {
            interval: { size: 3, unit: "second" },
            pipeline: [
                {
                    $group: {_id: "$id", sum: { $sum: 1 }, avg: { $avg: "$val" }}
                },
                { $sort: { _id: 1 }}
            ]
         }
    },
    {$emit: {"connectionName": "__testMemory"}}
]
    )";

    KafkaConnectionOptions kafkaOptions("");
    kafkaOptions.setIsTestKafka(true);
    mongo::Connection connection("kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
    Parser parser(_context.get(), /*options*/ {}, {{"kafka1", connection}});
    auto dag = parser.fromBson(parseBsonVector(pipeline));
    dag->start();
    dag->source()->connect();

    auto source = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
    auto consumers = this->kafkaGetConsumers(source);

    std::vector<KafkaSourceDocument> docsOne;
    auto inputDocsOne = fromjson(jsonInputOne);
    for (auto& doc : inputDocsOne) {
        KafkaSourceDocument sourceDoc;
        sourceDoc.doc = doc.Obj();
        docsOne.emplace_back(std::move(sourceDoc));
    }

    std::vector<KafkaSourceDocument> docsTwo;
    auto inputDocsTwo = fromjson(jsonInputTwo);
    for (auto& doc : inputDocsTwo) {
        KafkaSourceDocument sourceDoc;
        sourceDoc.doc = doc.Obj();
        docsTwo.emplace_back(std::move(sourceDoc));
    }

    consumers[0]->addDocuments(std::move(docsOne));
    consumers[1]->addDocuments(std::move(docsTwo));

    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    kafkaRunOnce(source);
    auto results = toVector(sink->getMessages());

    // Initially, we shouldn't have any results (that is, we should have open windows but no data
    // msg results in our sink).
    ASSERT(!results.empty());
    for (auto res : results) {
        ASSERT(!res.dataMsg);
        ASSERT(res.controlMsg);
        ASSERT(res.controlMsg->watermarkMsg);
        ASSERT_EQ(res.controlMsg->watermarkMsg->watermarkStatus, WatermarkStatus::kActive);
    }

    // If we sleep for longer than the idleness period and run again, both partitions should be
    // marked as idle. In the absence of new events, we should not close any windows, no matter how
    // many times we run or how long we wait.
    sleepmillis(8 * 1000);
    kafkaRunOnce(source);
    kafkaRunOnce(source);
    kafkaRunOnce(source);

    results = toVector(sink->getMessages());
    ASSERT(!results.empty());
    for (auto res : results) {
        ASSERT(!res.dataMsg);
        ASSERT(res.controlMsg);
        ASSERT(res.controlMsg->watermarkMsg);
        ASSERT_EQ(res.controlMsg->watermarkMsg->watermarkStatus, WatermarkStatus::kIdle);
    }
}

TEST_F(WindowOperatorTest, WindowSizeLargerThanIdlenessTimeout) {
    std::string jsonInput = R"([
        {"id": 1, "timestamp": "2023-04-10T17:02:20.061Z", "val": 1},
        {"id": 1, "timestamp": "2023-04-10T17:02:20.062Z", "val": 2},
        {"id": 2, "timestamp": "2023-04-10T17:02:20.063Z", "val": 3},
        {"id": 2, "timestamp": "2023-04-10T17:02:20.100Z", "val": 4}
    ])";

    std::string pipeline = R"(
[
     { $source: {
        connectionName: "kafka1",
        topic: "inputTopic",
        timeField : { $dateFromString : { "dateString" : "$timestamp"} },
        testOnlyPartitionCount: 2,
        idlenessTimeout: { size: 1, unit: "second" }
    }},
    {
        $tumblingWindow: {
            interval: { size: 5, unit: "second" },
            pipeline: [
                {
                    $group: {_id: "$id", sum: { $sum: 1 }, avg: { $avg: "$val" }}
                },
                { $sort: { _id: 1 }}
            ]
         }
    },
    {$emit: {"connectionName": "__testMemory"}}
]
    )";

    KafkaConnectionOptions kafkaOptions("");
    kafkaOptions.setIsTestKafka(true);
    mongo::Connection connection("kafka1", mongo::ConnectionTypeEnum::Kafka, kafkaOptions.toBSON());
    Parser parser(_context.get(), /*options*/ {}, {{"kafka1", connection}});
    auto dag = parser.fromBson(parseBsonVector(pipeline));
    dag->start();
    dag->source()->connect();

    auto source = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
    auto consumers = this->kafkaGetConsumers(source);
    std::vector<KafkaSourceDocument> docs;
    auto inputDocs = fromjson(jsonInput);
    for (auto& doc : inputDocs) {
        KafkaSourceDocument sourceDoc;
        sourceDoc.doc = doc.Obj();
        docs.emplace_back(std::move(sourceDoc));
    }

    // By populating one of our consumers and leaving the other empty, we can simulate an idle
    // partition.
    consumers[0]->addDocuments(std::move(docs));

    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    kafkaRunOnce(source);
    auto results = toVector(sink->getMessages());

    // Initially, we shouldn't have any results (that is, we should have open windows but no data
    // msg results in our sink).
    ASSERT(!results.empty());
    for (auto res : results) {
        ASSERT(!res.dataMsg);
        ASSERT(res.controlMsg);
        ASSERT(res.controlMsg->watermarkMsg);
        ASSERT_EQ(res.controlMsg->watermarkMsg->watermarkStatus, WatermarkStatus::kActive);
    }

    // If we sleep for longer than the idleness period, but shorter than the window size, both
    // partitions should be marked as idle.
    sleepmillis(2 * 1000);
    kafkaRunOnce(source);
    results = toVector(sink->getMessages());

    // We should still have no data messages, and our partition should be marked as idle.
    ASSERT(!results.empty());
    for (auto res : results) {
        ASSERT(!res.dataMsg);
        ASSERT(res.controlMsg);
        ASSERT(res.controlMsg->watermarkMsg);
        ASSERT_EQ(res.controlMsg->watermarkMsg->watermarkStatus, WatermarkStatus::kIdle);
    }

    // Add another document. This should allow for our earliest window to be closed, as this event
    // is more than 5 seconds after the previous four events (exceeding the size of the window),
    // plus the default allowed lateness. Crucially, we should close the window even if our second
    // partition is idle.
    KafkaSourceDocument sourceDoc;
    const auto controlTimestampString = "2023-04-10T17:02:28.200Z";
    const auto dateWithStatus = dateFromISOString(controlTimestampString);
    ASSERT_OK(dateWithStatus);

    // The watermark time is computed as the timestamp minus the allowed lateness minus 1.
    const auto expectedMillisSinceEpoch = dateWithStatus.getValue().toMillisSinceEpoch() - 3000 - 1;
    sourceDoc.doc = BSON("_id" << 3 << "val" << 10 << "timestamp" << controlTimestampString);
    consumers[0]->addDocuments({std::move(sourceDoc)});

    kafkaRunOnce(source);

    results = toVector(sink->getMessages());
    std::vector<Document> streamResults;
    std::vector<StreamControlMsg> streamControlMsgs;
    for (auto& result : results) {
        if (result.dataMsg) {
            for (auto& doc : result.dataMsg->docs) {
                // Remove _stream_meta field from the documents.
                MutableDocument mutableDoc{std::move(doc.doc)};
                mutableDoc.remove(kStreamsMetaField);
                streamResults.push_back(mutableDoc.freeze());
            }
        } else if (result.controlMsg) {
            streamControlMsgs.emplace_back(std::move(*result.controlMsg));
        }
    }

    ASSERT_EQ(streamResults.size(), 2);
    ASSERT_BSONOBJ_EQ(streamResults[0].toBson(), fromjson("{_id: 1, sum: 2, avg: 1.5}"));
    ASSERT_BSONOBJ_EQ(streamResults[1].toBson(), fromjson("{_id: 2, sum: 2, avg: 3.5}"));
    ASSERT_EQ(streamControlMsgs.size(), 1);
    ASSERT(streamControlMsgs[0].watermarkMsg);
    ASSERT_EQ(streamControlMsgs[0].watermarkMsg->watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQ(streamControlMsgs[0].watermarkMsg->eventTimeWatermarkMs, expectedMillisSinceEpoch);
}

TEST_F(WindowOperatorTest, StatsStateSize) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ testInMemoryConnectionRegistry());
    std::vector<BSONObj> pipeline = {
        fromjson("{ $source: { connectionName: '__testMemory' }}"),
        fromjson(R"({
            $tumblingWindow: {
                interval: { size: 1, unit: 'second' },
                pipeline: [
                    {
                        $group: {
                            _id: '$id',
                            sum: { $sum: '$value' }
                        }
                    },
                    { $sort: { sum: 1 } },
                    { $limit: 1 }
                ]
            }
        })"),
        fromjson("{ $emit: { connectionName: '__testMemory' }}"),
    };
    auto dag = parser.fromBson(std::move(pipeline));
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->source());
    dag->start();

    // Send input that will open three windows.
    source->addDataMsg(StreamDataMsg{{
        generateDocSeconds(5, 1, 1),
        generateDocSeconds(6, 2, 1),
        generateDocSeconds(7, 3, 1),
    }});
    source->runOnce();

    auto windowOperator = dynamic_cast<WindowOperator*>(dag->operators()[1].get());
    auto stats = windowOperator->getStats();
    ASSERT_EQUALS(3, stats.numInputDocs);
    ASSERT_EQUALS(48, stats.memoryUsageBytes);

    source->addControlMsg(StreamControlMsg{
        .watermarkMsg =
            WatermarkControlMsg{
                // This should close ts=5s and ts=6s windows,
                .eventTimeWatermarkMs = 7000,
            },
    });
    source->runOnce();

    // The memory usage should go down now that two of the windows closed.
    stats = windowOperator->getStats();
    ASSERT_EQUALS(16, stats.memoryUsageBytes);

    // Add three new windows, with one window receiving two unique group keys.
    source->addDataMsg(StreamDataMsg{{
        generateDocSeconds(8, 4, 1),
        generateDocSeconds(9, 5, 1),
        generateDocSeconds(9, 6, 1),
        generateDocSeconds(10, 7, 1),
    }});
    source->runOnce();

    // The memory usage should go back up now that we have three new windows.
    stats = windowOperator->getStats();
    ASSERT_EQUALS(80, stats.memoryUsageBytes);
}

}  // namespace streams
