/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/limit_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

class LimitOperatorTest : public AggregationContextFixture {
public:
    LimitOperatorTest() {
        _metricManager = std::make_unique<MetricManager>();
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

TEST_F(LimitOperatorTest, Basic) {
    std::vector<Document> inputDocs;
    inputDocs.reserve(40);
    for (int i = 0; i < 40; i += 2) {
        inputDocs.push_back(Document(fromjson(fmt::format("{{a: {}}}", i))));
        inputDocs.push_back(Document(fromjson(fmt::format("{{a: {}}}", i + 1))));
    }

    for (int limit = 0; limit < 20; ++limit) {
        InMemorySourceOperator::Options options;
        options.timestampOutputFieldName = "_ts";

        InMemorySourceOperator source(_context.get(), std::move(options));
        LimitOperator limitOp(_context.get(), limit);
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);

        source.addOutput(&limitOp, 0);
        limitOp.addOutput(&sink, 0);

        source.start();
        limitOp.start();
        sink.start();

        for (int i = 0; i < 40;) {
            for (auto msgSize : {2, 3, 5}) {
                StreamDataMsg dataMsg;
                for (int j = 0; j < msgSize; ++j, ++i) {
                    dataMsg.docs.emplace_back(inputDocs[i]);
                }
                source.addDataMsg(std::move(dataMsg));
            }
        }

        // Push all the messages from the source to the sink.
        source.runOnce();

        auto messages = sink.getMessages();
        std::vector<mongo::BSONObj> outputDocs;
        outputDocs.reserve(limit);
        while (!messages.empty()) {
            StreamMsgUnion msg = std::move(messages.front());
            messages.pop_front();
            ASSERT_TRUE(msg.dataMsg);
            for (auto& doc : msg.dataMsg->docs) {
                outputDocs.push_back(doc.doc.toBson());
            }
        }
        ASSERT_EQUALS(outputDocs.size(), limit);

        for (int i = 0; i < int(outputDocs.size()); ++i) {
            ASSERT_BSONOBJ_EQ(sanitizeDoc(outputDocs[i]), fromjson(fmt::format("{{a: {}}}", i)));
        }
    }
}

}  // namespace
}  // namespace streams
