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
#include "streams/exec/sort_operator.h"
#include "streams/exec/tests/test_utils.h"
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

        SortOperator::Options options{.documentSource = sortStage.get()};
        auto sortOperator = std::make_unique<SortOperator>(_context.get(), std::move(options));

        // Add a InMemorySinkOperator after the SortOperator.
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
        sortOperator->addOutput(&sink, 0);
        sink.start();
        sortOperator->start();

        StreamDataMsg dataMsg;
        for (auto& inputDoc : inputDocs) {
            dataMsg.docs.emplace_back(Document(inputDoc));
        }

        ASSERT_EQUALS(0, sortOperator->getStats().memoryUsageBytes);
        sortOperator->onDataMsg(0, std::move(dataMsg));

        // Stream results from the sort operator to the sink until we exhaust all documents
        // in the sort operator.
        while (!sortOperator->_reachedEof) {
            ASSERT_GT(sortOperator->getStats().memoryUsageBytes, 0);
            StreamControlMsg controlMsg{.eofSignal = true};
            sortOperator->onControlMsg(0, std::move(controlMsg));
        }

        ASSERT_EQUALS(0, sortOperator->getStats().memoryUsageBytes);
        auto messages = sink.getMessages();
        ASSERT_EQUALS(messages.size(), 1);
        auto msg = std::move(messages.front());
        messages.pop_front();

        // This should have both a data message and a control message, the control message
        // should be the EOF signal that was sent alongside the last batch.
        ASSERT(msg.dataMsg);
        ASSERT(msg.controlMsg);

        ASSERT_EQUALS(msg.dataMsg->docs.size(), expectedOutputDocs.size());

        for (size_t i = 0; i < expectedOutputDocs.size(); ++i) {
            const auto& streamDoc = msg.dataMsg->docs[i];
            ASSERT_BSONOBJ_EQ(expectedOutputDocs[i], streamDoc.doc.toBson());
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

TEST_F(SortOperatorTest, MemoryTracking) {
    std::string spec = "{ $sort: { value: 1 } }";
    auto sortStage = createSortStage(fromjson(spec));
    ASSERT(sortStage);

    SortOperator::Options options{.documentSource = sortStage.get()};
    auto sortOperator = std::make_unique<SortOperator>(_context.get(), std::move(options));

    // Add a InMemorySinkOperator after the SortOperator.
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    sortOperator->addOutput(&sink, 0);
    sink.start();
    sortOperator->start();

    sortOperator->onDataMsg(0,
                            StreamDataMsg{{
                                Document(fromjson(fmt::format("{{id: {}, value: {}}}", 1, 1))),
                                Document(fromjson(fmt::format("{{id: {}, value: {}}}", 2, 2))),
                            }});
    ASSERT_EQUALS(288, _context->memoryAggregator->getCurrentMemoryUsageBytes());

    sortOperator->onDataMsg(0,
                            StreamDataMsg{{
                                Document(fromjson(fmt::format("{{id: {}, value: {}}}", 3, 3))),
                            }});
    ASSERT_EQUALS(432, _context->memoryAggregator->getCurrentMemoryUsageBytes());

    sortOperator->onControlMsg(0, StreamControlMsg{.eofSignal = true});
    auto outputMsgs = sink.getMessages();
    ASSERT_EQUALS(1, outputMsgs.size());
    ASSERT_EQUALS(0, _context->memoryAggregator->getCurrentMemoryUsageBytes());
}

};  // namespace streams
