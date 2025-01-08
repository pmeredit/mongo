/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/limit_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/window_assigner.h"
#include "streams/exec/window_aware_operator.h"
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
    std::unique_ptr<Operator> makeLimitOperator(int64_t limit, bool useNewLimit) {
        LimitOperator::Options opts{WindowAwareOperator::Options{
            .windowAssigner = std::make_unique<WindowAssigner>(_windowOptions)}};
        opts.limit = limit;
        return std::make_unique<LimitOperator>(_context.get(), std::move(opts));
    }

    void testBasic(bool useNewLimit);

    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    WindowAssigner::Options _windowOptions{.size = 1,
                                           .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                           .slide = 1,
                                           .slideUnit = mongo::StreamTimeUnitEnum::Second};
    const TimeZoneDatabase _timeZoneDb{};
    const TimeZone _timeZone{_timeZoneDb.getTimeZone("UTC")};
};

void LimitOperatorTest::testBasic(bool useNewLimit) {
    const auto startTime = _timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0);
    const auto endTime =
        startTime + Milliseconds{toMillis(_windowOptions.sizeUnit, _windowOptions.size)};
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
        auto limitOp = makeLimitOperator(limit, useNewLimit);
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);

        source.addOutput(limitOp.get(), 0);
        limitOp->addOutput(&sink, 0);

        source.start();
        limitOp->start();
        sink.start();

        for (int i = 0; i < 40;) {
            for (auto msgSize : {2, 3, 5}) {
                auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
                for (int j = 0; j < msgSize; ++j, ++i) {
                    StreamDocument doc{inputDocs[i]};
                    doc.minDocTimestampMs = startTime.toMillisSinceEpoch();
                    dataMsg.docs.emplace_back(std::move(doc));
                }
                source.addDataMsg(std::move(dataMsg));
            }
        }
        source.addControlMsg(StreamControlMsg{
            WatermarkControlMsg{.watermarkTimestampMs = endTime.toMillisSinceEpoch()}});

        // Push all the messages from the source to the sink.
        source.runOnce();

        auto messages = sink.getMessages();
        std::vector<mongo::BSONObj> outputDocs;
        outputDocs.reserve(limit);
        while (messages.size() > 1) {
            StreamMsgUnion msg = std::move(messages.front());
            messages.pop_front();
            ASSERT_TRUE(msg.dataMsg);
            for (auto& doc : msg.dataMsg->docs) {
                outputDocs.push_back(doc.doc.toBson());
            }
        }
        ASSERT(messages.front().controlMsg);
        if (useNewLimit) {
            // The WindowAwareLimitOperator will send an output watermark 1 ms before the earliest
            // allowed window start time.
            ASSERT_EQ(endTime.toMillisSinceEpoch() - 1,
                      messages.front().controlMsg->watermarkMsg->watermarkTimestampMs);
        } else {
            // LimitOperator just passes the input watermark along.
            ASSERT_EQ(endTime.toMillisSinceEpoch(),
                      messages.front().controlMsg->watermarkMsg->watermarkTimestampMs);
        }
        ASSERT_EQUALS(outputDocs.size(), limit);

        for (int i = 0; i < int(outputDocs.size()); ++i) {
            ASSERT_BSONOBJ_EQ(sanitizeDoc(outputDocs[i]), fromjson(fmt::format("{{a: {}}}", i)));
        }
    }
}

TEST_F(LimitOperatorTest, Basic) {
    testBasic(true /* useNewLimit */);
}

}  // namespace
}  // namespace streams
