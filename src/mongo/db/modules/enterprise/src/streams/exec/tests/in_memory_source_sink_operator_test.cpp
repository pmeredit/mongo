/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/stdx/chrono.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class InMemorySourceSinkOperatorTest : public AggregationContextFixture {
public:
    InMemorySourceSinkOperatorTest() {
        _metricManager = std::make_unique<MetricManager>();
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
    }

    std::vector<StreamMsgUnion> getSourceMessages(InMemorySourceOperator& source);

    InMemorySourceOperator::Options makeSourceOptions() const;

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

InMemorySourceOperator::Options InMemorySourceSinkOperatorTest::makeSourceOptions() const {
    return InMemorySourceOperator::Options(SourceOperator::Options{
        .timestampOutputFieldName = "_ts",
        .useWatermarks = true,
    });
}

std::vector<StreamMsgUnion> InMemorySourceSinkOperatorTest::getSourceMessages(
    InMemorySourceOperator& source) {
    stdx::lock_guard<stdx::mutex> lock(source._mutex);
    auto msgs = source.getMessages(lock);
    return msgs;
}

// Test that message passing works as expected for the simple case when there are only 2 operators
// in the dag.
TEST_F(InMemorySourceSinkOperatorTest, Basic) {
    InMemorySourceOperator source(_context.get(), makeSourceOptions());
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);

    // Connect the source to the sink.
    source.addOutput(&sink, 0);
    source.start();
    sink.start();

    for (int i = 0; i < 10; ++i) {
        auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}}}", i))));

        source.addDataMsg(dataMsg);
        source.addDataMsg(dataMsg);
    }

    // Push all the messages from the source to the sink.
    source.runOnce();
    ASSERT_EQUALS(getSourceMessages(source).size(), 0);

    auto messages = sink.getMessages();
    ASSERT_EQUALS(messages.size(), 20);
    for (int i = 0; i < 10; ++i) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop_front();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));

        msg = std::move(messages.front());
        messages.pop_front();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));
    }
    ASSERT_TRUE(messages.empty());
}

// Test that message passing works as expected when an operator has 2 inputs.
TEST_F(InMemorySourceSinkOperatorTest, TwoInputs) {
    InMemorySourceOperator source1(_context.get(), makeSourceOptions());
    for (int i = 0; i < 10; ++i) {
        auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}}}", i))));

        source1.addDataMsg(dataMsg);
        source1.addDataMsg(dataMsg);
    }

    InMemorySourceOperator source2(_context.get(), makeSourceOptions());
    for (int i = 0; i < 10; ++i) {
        auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{b: {}}}", i))));

        source2.addDataMsg(dataMsg);
        source2.addDataMsg(dataMsg);
    }

    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 2);

    // Connect the 2 sources to the sink.
    source1.addOutput(&sink, 0);
    source2.addOutput(&sink, 1);

    sink.start();
    source1.start();
    source2.start();

    // Push all the messages from the sources to the sink.
    source2.runOnce();
    ASSERT_EQUALS(getSourceMessages(source2).size(), 0);
    source1.runOnce();
    ASSERT_EQUALS(getSourceMessages(source1).size(), 0);

    auto messages = sink.getMessages();
    ASSERT_EQUALS(messages.size(), 40);
    // Check that all messages from source2 have been received.
    for (int i = 0; i < 10; ++i) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop_front();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("b"));

        msg = std::move(messages.front());
        messages.pop_front();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("b"));
    }
    // Check that all messages from source1 have been received.
    for (int i = 0; i < 10; ++i) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop_front();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));

        msg = std::move(messages.front());
        messages.pop_front();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));
    }
    ASSERT_TRUE(messages.empty());
}

TEST_F(InMemorySourceSinkOperatorTest, TimestampAndWatermark) {
    boost::intrusive_ptr<ExpressionContextForTest> exprCtx(new ExpressionContextForTest{});
    auto timestampExpr =
        Expression::parseExpression(exprCtx.get(),
                                    fromjson("{$convert: { input: '$timestampMs', to: 'date' }}"),
                                    exprCtx->variablesParseState);
    auto timestampExtractor = std::make_unique<DocumentTimestampExtractor>(exprCtx, timestampExpr);

    auto options = makeSourceOptions();
    options.timestampExtractor = timestampExtractor.get();

    InMemorySourceOperator source(_context.get(), std::move(options));
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);

    source.addOutput(&sink, 0);
    source.start();
    sink.start();

    auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
    dataMsg.docs.emplace_back(Document(fromjson("{timestampMs: 1693933335000, event: 'abc'}")));
    dataMsg.docs.emplace_back(Document(fromjson("{timestampMs: 1693933341000, event: 'def'}")));
    source.addDataMsg(dataMsg);

    source.runOnce();
    ASSERT_TRUE(getSourceMessages(source).empty());

    auto msgs = sink.getMessages();
    ASSERT_EQUALS(1, msgs.size());

    auto& msg = msgs[0];
    ASSERT_TRUE(msg.dataMsg);
    ASSERT_EQUALS(dataMsg.docs.size(), msg.dataMsg->docs.size());

    auto timestamp = msg.dataMsg->docs[0].doc.getField("_ts").getDate();
    ASSERT_EQUALS(Date_t::fromMillisSinceEpoch(1693933335000), timestamp);

    timestamp = msg.dataMsg->docs[1].doc.getField("_ts").getDate();
    ASSERT_EQUALS(Date_t::fromMillisSinceEpoch(1693933341000), timestamp);

    // watermark should be that of last message timestamp - 1
    ASSERT_TRUE(msg.controlMsg);
    ASSERT_TRUE(msg.controlMsg->watermarkMsg);
    ASSERT_FALSE(msg.controlMsg->checkpointMsg);

    ASSERT_EQUALS(WatermarkStatus::kActive, msg.controlMsg->watermarkMsg->watermarkStatus);
    ASSERT_EQUALS(1693933341000 - 1, msg.controlMsg->watermarkMsg->eventTimeWatermarkMs);
}

}  // namespace streams
