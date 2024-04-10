/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/window_aware_sort_operator.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class SortOperatorTest : public AggregationContextFixture {
public:
    SortOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
    }

    boost::intrusive_ptr<DocumentSourceSort> createSortStage(BSONObj spec) {
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSourceSort> sortStage = dynamic_cast<DocumentSourceSort*>(
            DocumentSourceSort::createFromBson(specElem, _context->expCtx).get());
        ASSERT_TRUE(sortStage);
        return sortStage;
    }

    void testSort(const std::vector<BSONObj>& inputDocs,
                  const std::vector<BSONObj>& expectedOutputDocs) {
        auto spec = BSON("$sort" << BSON("val" << 1));
        auto sortStage = createSortStage(std::move(spec));
        ASSERT(sortStage);

        WindowAwareOperator::Options options{.sendWindowCloseSignal = false};
        WindowAwareSortOperator::Options sortOptions{std::move(options)};
        sortOptions.documentSource = sortStage.get();
        auto sortOperator =
            std::make_unique<WindowAwareSortOperator>(_context.get(), std::move(sortOptions));

        // Add a InMemorySinkOperator after the SortOperator.
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
        sortOperator->addOutput(&sink, 0);
        sink.start();
        sortOperator->start();

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

        ASSERT_EQUALS(0, sortOperator->getStats().memoryUsageBytes);
        sortOperator->onDataMsg(0, std::move(dataMsg));
        StreamControlMsg controlMsg{.windowCloseSignal = windowStartTime.toMillisSinceEpoch()};
        sortOperator->onControlMsg(0, std::move(controlMsg));

        ASSERT_EQUALS(0, sortOperator->getStats().memoryUsageBytes);
        auto messages = sink.getMessages();
        ASSERT_EQUALS(messages.size(), 1);
        auto msg = std::move(messages.front());
        messages.pop_front();

        // This should have only a data message.
        ASSERT(msg.dataMsg);
        ASSERT(!msg.controlMsg);

        ASSERT_EQUALS(msg.dataMsg->docs.size(), expectedOutputDocs.size());

        for (size_t i = 0; i < expectedOutputDocs.size(); ++i) {
            const auto& streamDoc = msg.dataMsg->docs[i];
            auto bson = streamDoc.doc.toBson();
            bson = bson.removeField("_stream_meta");
            ASSERT_BSONOBJ_EQ(expectedOutputDocs[i], bson);
        }
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

TEST_F(SortOperatorTest, SimpleInt) {
    std::vector<BSONObj> inputDocs;
    std::vector<BSONObj> expectedOutputDocs;
    std::vector<int> vals = {5, 2, -10, 13, 85, 22, 36, 12, 529, -20};

    inputDocs.reserve(vals.size());
    for (auto& val : vals) {
        inputDocs.emplace_back(fromjson(fmt::format("{{val: {}}}", val)));
    }
    std::sort(vals.begin(), vals.end());
    expectedOutputDocs.reserve(vals.size());
    for (auto& val : vals) {
        expectedOutputDocs.emplace_back(fromjson(fmt::format("{{val: {}}}", val)));
    }
    testSort(inputDocs, expectedOutputDocs);
}

TEST_F(SortOperatorTest, SimpleString) {
    std::vector<BSONObj> inputDocs;
    std::vector<BSONObj> expectedOutputDocs;
    std::vector<std::string> vals = {"-a", "axd", "hey", ",n0", "hi", "$$", "aa"};

    inputDocs.reserve(vals.size());
    for (auto& val : vals) {
        inputDocs.emplace_back(fromjson(fmt::format("{{val: \"{}\"}}", val)));
    }
    std::sort(vals.begin(), vals.end());
    expectedOutputDocs.reserve(vals.size());
    for (auto& val : vals) {
        expectedOutputDocs.emplace_back(fromjson(fmt::format("{{val: \"{}\"}}", val)));
    }
    testSort(inputDocs, expectedOutputDocs);
}

};  // namespace streams
