/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "streams/exec/context.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

class ValidateOperatorTest : public AggregationContextFixture {
public:
    ValidateOperatorTest() {
        _metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Executor> _executor;
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
        _context->dlq->registerMetrics(_executor->getMetricManager());
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

// Test that $validate works for $jsonSchema with required fields in it.
TEST_F(ValidateOperatorTest, JsonSchemaRequiredFields) {
    _context->connections = testInMemoryConnectionRegistry();
    Planner planner(_context.get(), {});
    std::string pipeline = R"(
[
    { $source: { connectionName: "__testMemory" }},
    { $validate: {
      validator: {
        $jsonSchema: { required: ["a", "b"] }
      }
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";

    auto dag =
        planner.plan(parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
    dag->start();
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 5; ++i) {
        // Good doc.
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}, b: {}}}", i, i))));
        // Bad doc.
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}, z: {}}}", i, i))));
    }
    source->addDataMsg(dataMsg);
    source->runOnce();

    auto messages = sink->getMessages();
    ASSERT_EQUALS(messages.size(), 1);
    StreamMsgUnion msg = std::move(messages.front());
    ASSERT_TRUE(msg.dataMsg);
    ASSERT_EQUALS(msg.dataMsg->docs.size(), 5);
    for (int i = 0; i < 5; ++i) {
        auto actual = sanitizeDoc(msg.dataMsg->docs[i].doc.toBson());
        ASSERT_BSONOBJ_EQ(dataMsg.docs[i * 2].doc.toBson(), actual);
    }
    ASSERT_EQUALS(getNumDlqDocsFromOperatorDag(*dag), 0);
    dag->stop();
}

// Test that $validate works for $jsonSchema that specifies a specific range of values for a field.
TEST_F(ValidateOperatorTest, JsonSchemaValueRange) {
    _context->connections = testInMemoryConnectionRegistry();
    Planner planner(_context.get(), {});
    std::string pipeline = R"(
[
    { $source: { connectionName: "__testMemory" }},
    { $validate: {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          properties: {
            quantity: {
              bsonType: "int",
              minimum: 0,
              maximum: 10,
              exclusiveMaximum: true
            }
          }
        }
      }
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";

    auto dag =
        planner.plan(parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
    dag->start();
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 5; ++i) {
        // Good doc.
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{quantity: {}}}", i * 2))));
        // Bad doc.
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{quantity: {}}}", (5 + i) * 2))));
    }
    source->addDataMsg(dataMsg);
    source->runOnce();

    auto messages = sink->getMessages();
    ASSERT_EQUALS(messages.size(), 1);
    StreamMsgUnion msg = std::move(messages.front());
    ASSERT_TRUE(msg.dataMsg);
    ASSERT_EQUALS(msg.dataMsg->docs.size(), 5);
    for (int i = 0; i < 5; ++i) {
        auto actual = sanitizeDoc(msg.dataMsg->docs[i].doc.toBson());
        ASSERT_BSONOBJ_EQ(dataMsg.docs[i * 2].doc.toBson(), actual);
    }
    ASSERT_EQUALS(getNumDlqDocsFromOperatorDag(*dag), 0);
    dag->stop();
}

// Test that $validate works when validator uses query operators.
// DLQ failed docs
TEST_F(ValidateOperatorTest, QueryExpression) {
    _context->connections = testInMemoryConnectionRegistry();
    Planner planner(_context.get(), {});
    std::string pipeline = R"(
[
    { $source: { connectionName: "__testMemory" }},
    { $validate: {
      validator: {
		$expr: {
          $eq: [
             "$y",
             { $multiply: [ "$a", { $sum:[ 1, "$b" ] } ] }
          ]
        }
      },
      validationAction: "dlq"
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";

    auto dag =
        planner.plan(parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
    dag->start();
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 5; ++i) {
        int32_t a = i * 2;
        int32_t b = i * 3;
        int32_t y = a * (1 + b);
        // Good doc.
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{a: {}, b: {}, y: {}}}", a, b, y))));
        // Bad doc.
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{a: {}, b: {}, y: {}}}", a, b, y - 1))));
    }
    source->addDataMsg(dataMsg);
    source->runOnce();

    auto messages = sink->getMessages();
    ASSERT_EQUALS(messages.size(), 1);
    StreamMsgUnion msg = std::move(messages.front());
    ASSERT_TRUE(msg.dataMsg);
    ASSERT_EQUALS(msg.dataMsg->docs.size(), 5);
    for (int i = 0; i < 5; ++i) {
        auto actual = sanitizeDoc(msg.dataMsg->docs[i].doc.toBson());
        ASSERT_BSONOBJ_EQ(dataMsg.docs[i * 2].doc.toBson(), actual);
    }
    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    ASSERT_EQ(5, dlqMsgs.size());
    while (!dlqMsgs.empty()) {
        ASSERT_EQ("ValidateOperator", dlqMsgs.front()["operatorName"].String());
        dlqMsgs.pop();
    }

    dag->stop();
}

}  // namespace
}  // namespace streams
