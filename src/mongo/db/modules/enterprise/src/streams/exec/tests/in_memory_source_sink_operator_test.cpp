/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {
namespace {

using namespace mongo;

class InMemorySourceSinkOperatorTest : public AggregationContextFixture {
protected:
    InMemorySourceSinkOperatorTest() : _context(getTestContext()) {}

    std::unique_ptr<Context> _context;
};

// Test that message passing works as expected for the simple case when there are only 2 operators
// in the dag.
TEST_F(InMemorySourceSinkOperatorTest, Basic) {
    InMemorySourceOperator source(_context.get(), /*numOutputs*/ 1);
    for (int i = 0; i < 10; ++i) {
        StreamDataMsg dataMsg;
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}}}", i))));
        StreamControlMsg controlMsg;

        source.addDataMsg(dataMsg, controlMsg);
        source.addDataMsg(dataMsg, controlMsg);
        source.addControlMsg(controlMsg);
    }

    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);

    // Connect the source to the sink.
    source.addOutput(&sink, 0);

    sink.start();
    source.start();

    // Push all the messages from the source to the sink.
    source.runOnce();
    ASSERT_EQUALS(source.getMessages().size(), 0);

    auto messages = sink.getMessages();
    ASSERT_EQUALS(messages.size(), 30);
    for (int i = 0; i < 10; ++i) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));
        ASSERT_TRUE(msg.controlMsg);

        msg = std::move(messages.front());
        messages.pop();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));
        ASSERT_TRUE(msg.controlMsg);

        msg = std::move(messages.front());
        messages.pop();
        ASSERT_FALSE(msg.dataMsg);
        ASSERT_TRUE(msg.controlMsg);
    }
    ASSERT_TRUE(messages.empty());
}

// Test that message passing works as expected when an operator has 2 inputs.
TEST_F(InMemorySourceSinkOperatorTest, TwoInputs) {
    InMemorySourceOperator source1(_context.get(), /*numOutputs*/ 1);
    for (int i = 0; i < 10; ++i) {
        StreamDataMsg dataMsg;
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}}}", i))));
        StreamControlMsg controlMsg;

        source1.addDataMsg(dataMsg, controlMsg);
        source1.addDataMsg(dataMsg, controlMsg);
        source1.addControlMsg(controlMsg);
    }

    InMemorySourceOperator source2(_context.get(), /*numOutputs*/ 1);
    for (int i = 0; i < 10; ++i) {
        StreamDataMsg dataMsg;
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{b: {}}}", i))));
        StreamControlMsg controlMsg;

        source2.addDataMsg(dataMsg, controlMsg);
        source2.addDataMsg(dataMsg, controlMsg);
        source2.addControlMsg(controlMsg);
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
    ASSERT_EQUALS(source2.getMessages().size(), 0);
    source1.runOnce();
    ASSERT_EQUALS(source1.getMessages().size(), 0);

    auto messages = sink.getMessages();
    ASSERT_EQUALS(messages.size(), 60);
    // Check that all messages from source2 have been received.
    for (int i = 0; i < 10; ++i) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("b"));
        ASSERT_TRUE(msg.controlMsg);

        msg = std::move(messages.front());
        messages.pop();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("b"));
        ASSERT_TRUE(msg.controlMsg);

        msg = std::move(messages.front());
        messages.pop();
        ASSERT_FALSE(msg.dataMsg);
        ASSERT_TRUE(msg.controlMsg);
    }
    // Check that all messages from source1 have been received.
    for (int i = 0; i < 10; ++i) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));
        ASSERT_TRUE(msg.controlMsg);

        msg = std::move(messages.front());
        messages.pop();
        ASSERT_TRUE(msg.dataMsg);
        ASSERT_EQUALS(msg.dataMsg->docs.size(), 1);
        ASSERT_VALUE_EQ(Value(i), msg.dataMsg->docs[0].doc.getField("a"));
        ASSERT_TRUE(msg.controlMsg);

        msg = std::move(messages.front());
        messages.pop();
        ASSERT_FALSE(msg.dataMsg);
        ASSERT_TRUE(msg.controlMsg);
    }
    ASSERT_TRUE(messages.empty());
}

}  // namespace
}  // namespace streams
