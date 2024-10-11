/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/unittest/assert.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class GroupOperatorTest : public AggregationContextFixture {
public:
    GroupOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Executor> _executor;
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
        _context->dlq->registerMetrics(_executor->getMetricManager());
    }

    boost::intrusive_ptr<DocumentSourceGroup> createGroupStage(BSONObj spec) {
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSourceGroup> groupStage = dynamic_cast<DocumentSourceGroup*>(
            DocumentSourceGroup::createFromBson(specElem, _context->expCtx).get());
        ASSERT_TRUE(groupStage);
        return groupStage;
    }

    std::tuple<std::vector<BSONObj>, OperatorStats> testGroup(
        BSONObj groupSpec, const std::vector<BSONObj>& inputDocs) {
        auto groupStage = createGroupStage(std::move(groupSpec));
        ASSERT(groupStage);

        WindowAwareOperator::Options options{.sendWindowSignals = false};
        GroupOperator::Options groupOptions{std::move(options)};
        groupOptions.documentSource = groupStage.get();
        auto groupOperator =
            std::make_unique<GroupOperator>(_context.get(), std::move(groupOptions));

        // Add a InMemorySinkOperator after the GroupOperator.
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
        groupOperator->addOutput(&sink, 0);
        sink.start();
        groupOperator->start();

        StreamDataMsg dataMsg;
        auto windowStartTime = Date_t::fromMillisSinceEpoch(1);
        for (auto& inputDoc : inputDocs) {
            StreamDocument streamDoc{Document{inputDoc}};
            StreamMetaWindow streamMetaWindow;
            streamMetaWindow.setStart(windowStartTime);
            streamMetaWindow.setEnd(windowStartTime + Milliseconds{1});
            streamDoc.streamMeta.setWindow(streamMetaWindow);
            dataMsg.docs.emplace_back(std::move(streamDoc));
        }

        groupOperator->onDataMsg(0, std::move(dataMsg));
        StreamControlMsg controlMsg{
            .windowCloseSignal = streams::WindowCloseMsg{
                Value(), static_cast<int64_t>(windowStartTime.toMillisSinceEpoch())}};
        groupOperator->onControlMsg(0, std::move(controlMsg));

        auto messages = sink.getMessages();
        ASSERT_EQUALS(messages.size(), 1);
        auto msg = std::move(messages.front());
        messages.pop_front();

        ASSERT(msg.dataMsg);
        ASSERT(!msg.controlMsg);

        std::vector<BSONObj> outputDocs;
        outputDocs.reserve(msg.dataMsg->docs.size());
        for (auto& streamDoc : msg.dataMsg->docs) {
            outputDocs.push_back(streamDoc.doc.toBson());
        }
        return {outputDocs, groupOperator->getStats()};
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
    auto [outputDocs, _] = testGroup(fromjson(groupSpec), inputDocs);
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
    int64_t dlqBytes = inputDocs[0].objsize() + inputDocs[1].objsize();

    const std::string groupSpec = R"(
{
    $group: {
        _id: {
            $divide: ["$a", "$b"]
        },
        sum: { $sum: 1 }
    }
})";
    auto [outputDocs, opstats] = testGroup(fromjson(groupSpec), inputDocs);
    ASSERT_EQUALS(outputDocs.size(), 2);
    ASSERT_EQUALS(opstats.numDlqDocs, 2);
    ASSERT(opstats.numDlqBytes >= dlqBytes);
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
        ASSERT_EQ("GroupOperator", dlqDoc["operatorName"].String());
        dlqMsgs.pop();
    }
}

};  // namespace streams
