/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/log_test.h"
#include "mongo/util/duration.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/limit_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/single_document_transformation_operator.h"
#include "streams/exec/sort_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_operator.h"

namespace streams {
using namespace mongo;

class SessionWindowAwareOperatorTest : public AggregationContextFixture {
public:
    SessionWindowAwareOperatorTest()
        : AggregationContextFixture(), windowOptions(WindowAssigner::Options{}) {
        auto [context, executor] = getTestContext(/*svcCtx*/ nullptr);
        _context = std::move(context);
        _executor = std::move(executor);
        _context->checkpointStorage = std::make_unique<InMemoryCheckpointStorage>(_context.get());
        _context->checkpointStorage->registerMetrics(_executor->getMetricManager());

        windowOptions.gapSize = 1;
        windowOptions.gapUnit = mongo::StreamTimeUnitEnum::Hour;
        windowOptions.partitionBy = ExpressionFieldPath::parse(
            _context->expCtx.get(), "$a", _context->expCtx->variablesParseState);
        windowOptions.allowedLatenessMs = 0;
        gapMs = toMillis(windowOptions.gapUnit, windowOptions.gapSize);
    }

    auto createSortStage(std::string spec,
                         boost::optional<SessionWindowAssigner::Options> windowOptions,
                         bool canSendSignal = true) {
        boost::intrusive_ptr<DocumentSourceSort> sortStage = dynamic_cast<DocumentSourceSort*>(
            DocumentSourceSort::createFromBson(fromjson(spec).firstElement(), _context->expCtx)
                .get());
        ASSERT_TRUE(sortStage);
        SortOperator::Options sortOptions{WindowAwareOperator::Options{
            .sendWindowSignals = canSendSignal,
            .isSessionWindow = true,
        }};
        if (windowOptions) {
            sortOptions.windowAssigner = std::make_unique<SessionWindowAssigner>(*windowOptions);
        }
        sortOptions.documentSource = sortStage.get();
        return std::make_tuple<>(
            std::move(sortStage),
            std::make_unique<SortOperator>(_context.get(), std::move(sortOptions)));
    }

    auto createGroupStage(
        std::string spec,
        boost::optional<SessionWindowAssigner::Options> windowOptions = boost::none,
        bool canSendSignals = false) {
        boost::intrusive_ptr<DocumentSourceGroup> groupStage = dynamic_cast<DocumentSourceGroup*>(
            DocumentSourceGroup::createFromBson(mongo::fromjson(spec).firstElement(),
                                                _context->expCtx)
                .get());
        ASSERT_TRUE(groupStage);
        GroupOperator::Options options(WindowAwareOperator::Options{
            .sendWindowSignals = canSendSignals,
            .isSessionWindow = true,
        });
        if (windowOptions) {
            options.windowAssigner = std::make_unique<SessionWindowAssigner>(*windowOptions);
        }
        options.documentSource = groupStage.get();
        return std::make_tuple<>(
            std::move(groupStage),
            std::make_unique<GroupOperator>(_context.get(), std::move(options)));
    }

    auto createLimitStage(std::string spec,
                          boost::optional<SessionWindowAssigner::Options> windowOptions,
                          bool canSendSignal = true) {
        boost::intrusive_ptr<DocumentSourceLimit> limitStage = dynamic_cast<DocumentSourceLimit*>(
            DocumentSourceLimit::createFromBson(fromjson(spec).firstElement(), _context->expCtx)
                .get());
        ASSERT_TRUE(limitStage);
        limitStage->setLimit(std::numeric_limits<int64_t>::max());
        LimitOperator::Options limitOptions{WindowAwareOperator::Options{
            .sendWindowSignals = canSendSignal, .isSessionWindow = true}};
        if (windowOptions) {
            limitOptions.windowAssigner = std::make_unique<SessionWindowAssigner>(*windowOptions);
        }

        limitOptions.limit = std::numeric_limits<int64_t>::max();

        return std::make_tuple<>(
            std::move(limitStage),
            std::make_unique<LimitOperator>(_context.get(), std::move(limitOptions)));
    }

    auto createMatchStage(std::string spec) {
        boost::intrusive_ptr<DocumentSourceMatch> matchStage = dynamic_cast<DocumentSourceMatch*>(
            DocumentSourceMatch::createFromBson(fromjson(spec).firstElement(), _context->expCtx)
                .get());
        MatchOperator::Options matchOptions{.documentSource = matchStage.get()};
        return std::make_tuple<>(
            std::move(matchStage),
            std::make_unique<MatchOperator>(_context.get(), std::move(matchOptions)));
    }

    auto testSortGroupPipeline(auto windowOptions, auto& messages) {
        auto [sortStage, sortOp] = createSortStage(sortSpec, windowOptions);
        auto [groupStage, groupOp] = createGroupStage(groupSpec);
        InMemorySinkOperator sink(_context.get(), 1);
        sortOp->addOutput(groupOp.get(), 0);
        groupOp->addOutput(&sink, 0);

        sortOp->start();
        sink.start();

        for (auto& msg : messages) {
            if (std::holds_alternative<StreamDataMsg>(msg)) {
                sortOp->onDataMsg(0, std::get<StreamDataMsg>(msg));
            } else if (std::holds_alternative<StreamControlMsg>(msg)) {
                sortOp->onControlMsg(0, std::get<StreamControlMsg>(msg));
            } else {
                continue;
            }
        }

        return queueToVector(sink.getMessages());
    }

    auto testLimitMatchGroupPipeline(auto windowOptions, auto& messages) {
        auto [limitStage, limitOp] = createLimitStage(limitSpec, windowOptions);
        auto [matchStage, matchOp] = createMatchStage(matchSpec);
        auto [groupStage, groupOp] = createGroupStage(groupSpec);

        InMemorySinkOperator sink(_context.get(), 1);
        limitOp->addOutput(matchOp.get(), 0);
        matchOp->addOutput(groupOp.get(), 0);
        groupOp->addOutput(&sink, 0);

        limitOp->start();
        sink.start();

        for (auto& msg : messages) {
            if (std::holds_alternative<StreamDataMsg>(msg)) {
                limitOp->onDataMsg(0, std::get<StreamDataMsg>(msg));
            } else if (std::holds_alternative<StreamControlMsg>(msg)) {
                limitOp->onControlMsg(0, std::get<StreamControlMsg>(msg));
            } else {
                continue;
            }
        }

        return queueToVector(sink.getMessages());
    }

    auto testLimitMatchLimitGroupPipeline(auto windowOptions, auto& messages) {
        auto [limitStage, limitOp] = createLimitStage(limitSpec, windowOptions);
        auto [matchStage, matchOp] = createMatchStage(matchSpec);
        auto [limitStage2, limitOp2] = createLimitStage(limitSpec, boost::none, true);
        auto [groupStage, groupOp] = createGroupStage(groupSpec);

        InMemorySinkOperator sink(_context.get(), 1);
        limitOp->addOutput(matchOp.get(), 0);
        matchOp->addOutput(limitOp2.get(), 0);
        limitOp2->addOutput(groupOp.get(), 0);
        groupOp->addOutput(&sink, 0);

        limitOp->start();
        sink.start();

        for (auto& msg : messages) {
            if (std::holds_alternative<StreamDataMsg>(msg)) {
                limitOp->onDataMsg(0, std::get<StreamDataMsg>(msg));
            } else if (std::holds_alternative<StreamControlMsg>(msg)) {
                limitOp->onControlMsg(0, std::get<StreamControlMsg>(msg));
            } else {
                continue;
            }
        }

        return queueToVector(sink.getMessages());
    }

    auto testLimitMatchSortGroupPipeline(auto windowOptions, auto& messages) {
        auto [limitStage, limitOp] = createLimitStage(limitSpec, windowOptions);
        auto [matchStage, matchOp] = createMatchStage(matchSpec);
        auto [sortStage, sortOp] = createSortStage(sortSpec, boost::none, true);
        auto [groupStage, groupOp] = createGroupStage(groupSpec);

        InMemorySinkOperator sink(_context.get(), 1);
        limitOp->addOutput(matchOp.get(), 0);
        matchOp->addOutput(sortOp.get(), 0);
        sortOp->addOutput(groupOp.get(), 0);
        groupOp->addOutput(&sink, 0);

        limitOp->start();
        sink.start();

        for (auto& msg : messages) {
            if (std::holds_alternative<StreamDataMsg>(msg)) {
                limitOp->onDataMsg(0, std::get<StreamDataMsg>(msg));
            } else if (std::holds_alternative<StreamControlMsg>(msg)) {
                limitOp->onControlMsg(0, std::get<StreamControlMsg>(msg));
            } else {
                continue;
            }
        }

        return queueToVector(sink.getMessages());
    }

    void validateDocIds(auto msg, auto expectedIds, bool shouldSort = false) {
        auto arr = msg.dataMsg.get().docs[0].doc.getField("allIds").getArray();

        ASSERT_EQUALS(arr.size(), expectedIds.size());

        if (shouldSort) {
            std::sort(arr.begin(), arr.end(), [](const mongo::Value& a, const mongo::Value& b) {
                return a.coerceToInt() < b.coerceToInt();
            });
        }

        auto it = arr.begin();
        for (auto id : expectedIds) {
            ASSERT_EQUALS((*it).coerceToInt(), id);
            ++it;
        }
    }

    bool validateOpenWindowCount(auto expectedTotalCount) {
        return true;
    }

    void validateWindowBounds(auto msg, auto expectedStart, auto expectedEnd) {
        auto start =
            msg.dataMsg.get().docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch();
        auto end = msg.dataMsg.get().docs[0].streamMeta.getWindow()->getEnd()->toMillisSinceEpoch();
        ASSERT_EQUALS(start, expectedStart);
        ASSERT_EQUALS(end, expectedEnd + gapMs);
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;
    SessionWindowAssigner::Options windowOptions;
    int64_t gapMs;


    constexpr static const char sortSpec[] = R"(
            {
                $sort: {
                    id: 1
                }
            }
        )";

    constexpr static const char groupSpec[] = R"(
            {
                $group: {
                    _id: null,
                    allIds: {$push: "$id"}
                }
            }
        )";

    constexpr static const char limitSpec[] = R"(
            {
                $limit: 100
            }
        )";

    constexpr static const char matchSpec[] = R"(
            {
                $match: {
                    "b": 1
                }
            }
        )";
};

// no merge
TEST_F(SessionWindowAwareOperatorTest, NoMerge_OneWindow) {
    std::vector<StreamDocument> inputDocs;

    auto leftTS = 0;
    auto myTS = leftTS + gapMs - 1;

    inputDocs.push_back(Document{fromjson("{ a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{ a: 1, b: 1, id: 1}")});
    inputDocs.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, myTS + gapMs}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1});
    validateWindowBounds((*result.begin()), leftTS, myTS);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1}, true);
    validateWindowBounds((*result.begin()), leftTS, myTS);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1}, true);
    validateWindowBounds((*result.begin()), leftTS, myTS);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1});
    validateWindowBounds((*result.begin()), leftTS, myTS);
}

// no filtering out, merge left
TEST_F(SessionWindowAwareOperatorTest, MergeLeft_OneWindow) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = leftTS + 1;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive, rightTS}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1});
    validateWindowBounds((result[0]), leftTS, myTS);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1}, true);
    validateWindowBounds((result[0]), leftTS, myTS);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1}, true);
    validateWindowBounds((result[0]), leftTS, myTS);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{0, 1});
    validateWindowBounds((result[0]), leftTS, myTS);
}

// no filtering out, merge right
TEST_F(SessionWindowAwareOperatorTest, MergeRight_OneWindow) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = rightTS + 1;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, myTS + gapMs}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1, 2});
    validateWindowBounds(result[1], rightTS, myTS);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1, 2}, true);
    validateWindowBounds(result[1], rightTS, myTS);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1, 2}, true);
    validateWindowBounds(result[1], rightTS, myTS);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1, 2});
    validateWindowBounds(result[1], rightTS, myTS);
}

// yes filtering out, merge left
TEST_F(SessionWindowAwareOperatorTest, MergeLeftFilteredOut_OneWindow) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = leftTS + 1;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive, rightTS}}};

    auto result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{1}, true);
    validateWindowBounds((result[0]), leftTS, myTS);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{1}, true);
    validateWindowBounds((result[0]), leftTS, myTS);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds((result[0]), std::vector<int>{1});
    validateWindowBounds((result[0]), leftTS, myTS);
}

// yes filtering out, merge right
TEST_F(SessionWindowAwareOperatorTest, MergeRightFilteredOut_OneWindow) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = rightTS + 1;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, myTS + gapMs}}};

    auto result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1}, true);
    validateWindowBounds(result[1], rightTS, myTS);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1}, true);
    validateWindowBounds(result[1], rightTS, myTS);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], leftTS, leftTS);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], rightTS, myTS);
}

// no filtering out, merge multiple neighbors
TEST_F(SessionWindowAwareOperatorTest, MergeMultipleDisjoint_OneWindow) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = (leftTS + rightTS) / 2;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, rightTS + gapMs}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2});
    validateWindowBounds(result[0], leftTS, rightTS);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2}, true);
    validateWindowBounds(result[0], leftTS, rightTS);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2}, true);
    validateWindowBounds(result[0], leftTS, rightTS);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2});
    validateWindowBounds(result[0], leftTS, rightTS);
}

// no filtering out, merge multiple neighbors that are spanned by 1 window
TEST_F(SessionWindowAwareOperatorTest, MergeMultipleSpan_OneWindow) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = (leftTS + rightTS) / 2;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 3}")});
    inputDocs.back().minDocTimestampMs = rightTS;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs1.back().minDocTimestampMs = leftTS - 1;
    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 2}")});
    inputDocs1.back().minDocTimestampMs = myTS;
    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 4}")});
    inputDocs1.back().minDocTimestampMs = rightTS + 1;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, rightTS + 1 + gapMs}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2, 3, 4});
    validateWindowBounds(result[0], leftTS - 1, rightTS + 1);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2, 3, 4}, true);
    validateWindowBounds(result[0], leftTS - 1, rightTS + 1);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2, 3, 4}, true);
    validateWindowBounds(result[0], leftTS - 1, rightTS + 1);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 2, 3, 4});
    validateWindowBounds(result[0], leftTS - 1, rightTS + 1);
}

// closing but nothing to close bc empty map
TEST_F(SessionWindowAwareOperatorTest, CloseEmptyMap) {
    std::vector<StreamDocument> inputDocs;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive, 1}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);
}

// closing but nothing to close bc everything is within gap
TEST_F(SessionWindowAwareOperatorTest, WatermarkWithinGap) {
    std::vector<StreamDocument> inputDocs;

    auto leftTS = 0;
    auto myTS = leftTS + toMillis(windowOptions.gapUnit, windowOptions.gapSize) - 1;

    inputDocs.push_back(Document{fromjson("{ a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{ a: 1, b: 1, id: 1}")});
    inputDocs.back().minDocTimestampMs = myTS;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive, leftTS}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);
}

// closing but nothing to close bc everything filtered out (also tests case where entire partition
// filtered out)
TEST_F(SessionWindowAwareOperatorTest, CloseEmptyMapFilteredOut) {
    std::vector<StreamDocument> inputDocs;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 0}")});
    inputDocs.back().minDocTimestampMs = 0;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 1}")});
    inputDocs.back().minDocTimestampMs = gapMs + 1;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 2}")});
    inputDocs.back().minDocTimestampMs = 2 * gapMs + 1;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, 3 * gapMs + 1}}};

    auto result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 0);
}

// merging but match operator filters out one of the "mergees"
TEST_F(SessionWindowAwareOperatorTest, OneMergeeFilteredOut) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = (leftTS + rightTS) / 2;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 3}")});
    inputDocs.back().minDocTimestampMs = rightTS + gapMs + 1;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;
    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 4}")});
    inputDocs1.back().minDocTimestampMs = myTS + gapMs - 1;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive,
                                                             rightTS + 2 * gapMs + 1}}};

    auto result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 3, 4}, true);
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 3, 4}, true);
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1, 3, 4});
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);
}

// merging but match operator filters out destination window
TEST_F(SessionWindowAwareOperatorTest, DestinationMergeeFilteredOut) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = (leftTS + rightTS) / 2;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 3}")});
    inputDocs.back().minDocTimestampMs = rightTS + gapMs + 1;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;
    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 4}")});
    inputDocs1.back().minDocTimestampMs = myTS + gapMs - 1;


    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive,
                                                             rightTS + gapMs * 2 + 1}}};

    auto result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{1, 2, 3, 4}, true);
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{1, 2, 3, 4}, true);
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{1, 2, 3, 4});
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);
}

// merging but match operator filters out all windows (also tests case where entire partition
// filtered out)
TEST_F(SessionWindowAwareOperatorTest, AllMergeesFilteredOut) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    auto leftTS = 1;
    auto rightTS = leftTS + gapMs + 1;
    auto myTS = (leftTS + rightTS) / 2;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 0}")});
    inputDocs.back().minDocTimestampMs = leftTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 2}")});
    inputDocs.back().minDocTimestampMs = rightTS;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 2, id: 3}")});
    inputDocs.back().minDocTimestampMs = rightTS + gapMs + 1;

    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = myTS;
    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 4}")});
    inputDocs1.back().minDocTimestampMs = myTS + gapMs - 1;

    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive,
                                                             rightTS + 2 * gapMs + 1}}};

    auto result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{1, 4}, true);
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{1, 4}, true);
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{1, 4});
    validateWindowBounds(result[0], leftTS, rightTS + gapMs + 1);
}

// input data is across multiple partitions (and multiple windows exist at once)
TEST_F(SessionWindowAwareOperatorTest, MultiplePartitions) {
    auto severityGuard = unittest::MinimumLoggedSeverityGuard{logv2::LogComponent::kStreams,
                                                              logv2::LogSeverity::Debug(5)};

    std::vector<StreamDocument> inputDocs;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = 0;
    inputDocs.push_back(Document{fromjson("{a: 2, b: 1, id: 1}")});
    inputDocs.back().minDocTimestampMs = 1;


    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive, gapMs + 1}}};


    auto result = testSortGroupPipeline(windowOptions, messages);
    std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return lhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt() <
            rhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt();
    });
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], 1, 1);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return lhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt() <
            rhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt();
    });
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], 1, 1);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return lhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt() <
            rhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt();
    });
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0}, true);
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1}, true);
    validateWindowBounds(result[1], 1, 1);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return lhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt() <
            rhs.dataMsg.get().docs[0].streamMeta.getWindow()->getPartition()->coerceToInt();
    });
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0}, true);
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1}, true);
    validateWindowBounds(result[1], 1, 1);
}

// multiple sessions open for a partition and all get closed by the same watermark
TEST_F(SessionWindowAwareOperatorTest, MultipleSessionsClosed) {
    std::vector<StreamDocument> inputDocs;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = 0;
    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs.back().minDocTimestampMs = gapMs + 1000;


    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamControlMsg{.watermarkMsg =
                             WatermarkControlMsg{WatermarkStatus::kActive, gapMs * 2 + 1000}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], gapMs + 1000, gapMs + 1000);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], gapMs + 1000, gapMs + 1000);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], gapMs + 1000, gapMs + 1000);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 2);
    validateDocIds(result[0], std::vector<int>{0});
    validateWindowBounds(result[0], 0, 0);
    validateDocIds(result[1], std::vector<int>{1});
    validateWindowBounds(result[1], gapMs + 1000, gapMs + 1000);
}

// input starts a session for a partition, closes it, then opens another session for a partition,
// and closes it
TEST_F(SessionWindowAwareOperatorTest, OpenClosePartition) {
    std::vector<StreamDocument> inputDocs;
    std::vector<StreamDocument> inputDocs1;

    inputDocs.push_back(Document{fromjson("{a: 1, b: 1, id: 0}")});
    inputDocs.back().minDocTimestampMs = 0;
    inputDocs1.push_back(Document{fromjson("{a: 1, b: 1, id: 1}")});
    inputDocs1.back().minDocTimestampMs = 0;


    std::vector<std::variant<StreamDataMsg, StreamControlMsg>> messages{
        StreamDataMsg{.docs = std::move(inputDocs)},
        StreamDataMsg{.docs = std::move(inputDocs1)},
        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{WatermarkStatus::kActive, gapMs}}};

    auto result = testSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1});
    validateWindowBounds(result[0], 0, 0);

    result = testLimitMatchSortGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1});
    validateWindowBounds(result[0], 0, 0);

    result = testLimitMatchGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1});
    validateWindowBounds(result[0], 0, 0);

    result = testLimitMatchLimitGroupPipeline(windowOptions, messages);
    ASSERT_EQUALS(result.size(), 1);
    validateDocIds(result[0], std::vector<int>{0, 1});
    validateWindowBounds(result[0], 0, 0);
}

TEST_F(SessionWindowAwareOperatorTest, shouldCloseWindow) {
    auto windowAssigner = std::make_unique<SessionWindowAssigner>(windowOptions);
    ASSERT_EQ(windowAssigner->shouldCloseWindow(0, gapMs - 1), false);
    ASSERT_EQ(windowAssigner->shouldCloseWindow(0, gapMs), true);
}

TEST_F(SessionWindowAwareOperatorTest, ShouldMergeWindows) {
    auto windowAssigner = std::make_unique<SessionWindowAssigner>(windowOptions);
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, 0, 0, 0), true);  // same interval
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, 0, gapMs - 1, gapMs - 1),
              true);  // disjoint within session on right
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(gapMs - 1, gapMs - 1, 0, 0),
              true);  // disjoint within session on left
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, gapMs - 1, gapMs - 1, gapMs - 1),
              true);  // disjoint, adjacent within session on right
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, gapMs - 1, 0, 0),
              true);  // disjoint, adjacent within session on left
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, gapMs - 1, gapMs - 2, gapMs),
              true);  // overlapping within session on right
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(1, gapMs, 0, 2),
              true);  // overlapping within session on left
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, gapMs - 1, gapMs - 2, gapMs - 1),
              true);  // overlapping + subsetted within session
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(0, 0, gapMs, gapMs),
              false);  // disjoint, not in same session on left
    ASSERT_EQ(windowAssigner->shouldMergeSessionWindows(gapMs, gapMs, 0, 0),
              false);  // disjoint, not in same session on right
}

}  // namespace streams
