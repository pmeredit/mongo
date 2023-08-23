/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/bsonobj.h"
#include "mongo/unittest/assert.h"
#include "streams/exec/message.h"
#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

class GroupOperatorTest : public AggregationContextFixture {
public:
    GroupOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
        _context = getTestContext(/*svcCtx*/ nullptr, _metricManager.get());
    }

    boost::intrusive_ptr<DocumentSourceGroup> createGroupStage(BSONObj spec) {
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSourceGroup> groupStage = dynamic_cast<DocumentSourceGroup*>(
            DocumentSourceGroup::createFromBson(specElem, _context->expCtx).get());
        ASSERT_TRUE(groupStage);
        return groupStage;
    }

    std::vector<BSONObj> testGroup(BSONObj groupSpec, const std::vector<BSONObj>& inputDocs) {
        auto groupStage = createGroupStage(std::move(groupSpec));
        ASSERT(groupStage);

        GroupOperator::Options options{.documentSource = groupStage.get()};
        auto groupOperator = std::make_unique<GroupOperator>(_context.get(), std::move(options));

        // Add a InMemorySinkOperator after the GroupOperator.
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
        groupOperator->addOutput(&sink, 0);
        sink.start();
        groupOperator->start();

        StreamDataMsg dataMsg;
        for (auto& inputDoc : inputDocs) {
            dataMsg.docs.emplace_back(Document(inputDoc));
        }

        StreamControlMsg controlMsg;
        controlMsg.eofSignal = true;
        groupOperator->onDataMsg(0, std::move(dataMsg), std::move(controlMsg));

        auto messages = sink.getMessages();
        ASSERT_EQUALS(messages.size(), 2);
        auto outputMsg = std::move(messages.front().dataMsg);
        messages.pop_front();
        ASSERT_TRUE(messages.front().controlMsg);
        messages.pop_front();

        std::vector<BSONObj> outputDocs;
        outputDocs.reserve(outputMsg->docs.size());
        for (auto& streamDoc : outputMsg->docs) {
            outputDocs.push_back(streamDoc.doc.toBson());
        }
        return outputDocs;
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

TEST_F(GroupOperatorTest, Simple) {
    std::vector<BSONObj> inputDocs;
    inputDocs.reserve(10);
    for (size_t i = 0; i < 10; ++i) {
        inputDocs.emplace_back(fromjson(fmt::format("{{id: {}, val: {}}}", i, i)));
    }

    const std::string groupSpec = R"(
{
    $group: {
        _id: null,
        sum: { $sum: "$val" }
    }
})";
    auto outputDocs = testGroup(fromjson(groupSpec), inputDocs);
    ASSERT_EQUALS(outputDocs.size(), 1);
    ASSERT_EQUALS(45, outputDocs[0]["sum"].Int());
}

TEST_F(GroupOperatorTest, DeadLetterQueue) {
    std::vector<BSONObj> inputDocs;
    inputDocs.reserve(10);
    for (size_t i = 0; i < 10; ++i) {
        inputDocs.emplace_back(fromjson(fmt::format("{{a: {}, b: {}}}", i, i)));
        inputDocs.emplace_back(fromjson(fmt::format("{{a: {}, b: {}}}", 2 * i, i)));
    }

    const std::string groupSpec = R"(
{
    $group: {
        _id: {
            $divide: ["$a", "$b"]
        },
        sum: { $sum: 1 }
    }
})";
    auto outputDocs = testGroup(fromjson(groupSpec), inputDocs);
    ASSERT_EQUALS(outputDocs.size(), 2);
    for (auto& doc : outputDocs) {
        if (1 == doc["_id"].Double()) {
            ASSERT_EQUALS(9, doc["sum"].Int());
        } else {
            ASSERT_EQUALS(2, doc["_id"].Double());
            ASSERT_EQUALS(9, doc["sum"].Int());
        }
    }

    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    ASSERT_EQ(2, dlqMsgs.size());
    while (!dlqMsgs.empty()) {
        auto dlqDoc = std::move(dlqMsgs.front());
        ASSERT_EQ(
            "Failed to process input document in GroupOperator with error: can't $divide by zero",
            dlqDoc["errInfo"]["reason"].String());
        dlqMsgs.pop();
    }
}

}  // namespace
}  // namespace streams
