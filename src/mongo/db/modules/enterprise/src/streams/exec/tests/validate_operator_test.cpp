/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/context.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/parser.h"
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
        _context = getTestContext(/*svcCtx*/ nullptr, _metricManager.get());
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

// Test that $validate works for $jsonSchema with required fields in it.
TEST_F(ValidateOperatorTest, JsonSchemaRequiredFields) {
    _context->connections = testInMemoryConnectionRegistry();
    Parser parser(_context.get(), {});
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

    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
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
    dag->stop();
}

// Test that $validate works for $jsonSchema that specifies a specific range of values for a field.
TEST_F(ValidateOperatorTest, JsonSchemaValueRange) {
    _context->connections = testInMemoryConnectionRegistry();
    Parser parser(_context.get(), {});
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

    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
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
    dag->stop();
}

// Test that $validate works when validator uses query operators.
TEST_F(ValidateOperatorTest, QueryExpression) {
    _context->connections = testInMemoryConnectionRegistry();
    Parser parser(_context.get(), {});
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
      }
    }},
    { $emit: {connectionName: "__testMemory"}}
]
    )";

    auto dag = parser.fromBson(
        parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
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
    dag->stop();
}

}  // namespace
}  // namespace streams
