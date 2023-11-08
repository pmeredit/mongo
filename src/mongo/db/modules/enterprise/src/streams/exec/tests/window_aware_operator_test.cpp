/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/duration.h"
#include "streams/exec/message.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_group_operator.h"
#include "streams/exec/window_aware_operator.h"
#include "streams/exec/window_aware_sort_operator.h"
#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/single_document_transformation_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

class WindowAwareOperatorTest : public AggregationContextFixture {
public:
    WindowAwareOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
        auto [context, _] = getTestContext(/*svcCtx*/ nullptr);
        _context = std::move(context);
    }

    boost::intrusive_ptr<DocumentSourceGroup> createGroupStage(BSONObj spec) {
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSourceGroup> groupStage = dynamic_cast<DocumentSourceGroup*>(
            DocumentSourceGroup::createFromBson(specElem, _context->expCtx).get());
        ASSERT_TRUE(groupStage);
        return groupStage;
    }

    // This verifies the WindowAwareGroupOperator when it is _not_ acting as the windowing stage.
    // In this flow it expects an upstream operator to assign each document to a window (in its
    // stream_meta).
    std::tuple<std::vector<StreamDocument>, OperatorStats> testGroupNotWindowAssigner(
        BSONObj groupSpec,
        std::vector<StreamDocument> inputDocs,
        WindowAssigner::Options windowOptions,
        std::vector<Date_t> windowsToClose,
        int expectedOutputMessages) {
        WindowAssigner windowAssigner{windowOptions};
        auto groupStage = createGroupStage(std::move(groupSpec));
        ASSERT(groupStage);

        WindowAwareGroupOperator::Options options{
            WindowAwareOperator::Options{.sendWindowCloseSignal = true}};
        options.documentSource = groupStage.get();
        auto groupOperator =
            std::make_unique<WindowAwareGroupOperator>(_context.get(), std::move(options));

        // Add a InMemorySinkOperator after the GroupOperator.
        InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
        groupOperator->addOutput(&sink, 0);
        sink.start();
        groupOperator->start();

        std::map<int64_t, StreamDataMsg> inputMessages;
        for (auto& inputDoc : inputDocs) {
            // Assign the document to window(s).
            auto docTime = inputDoc.minEventTimestampMs;
            auto oldestStartTime = windowAssigner.toOldestWindowStartTime(docTime);
            std::vector<int64_t> times;
            for (auto start = oldestStartTime; start <= docTime;
                 start = windowAssigner.getNextWindowStartTime(start)) {
                times.push_back(start);
            }

            // For each window the doc belongs to, append the doc to the input messsage for that
            // window.
            for (auto& time : times) {
                StreamDataMsg& dataMsg = inputMessages[time];
                inputDoc.streamMeta.setWindowStartTimestamp(
                    mongo::Date_t::fromMillisSinceEpoch(time));
                inputDoc.streamMeta.setWindowEndTimestamp(
                    mongo::Date_t::fromMillisSinceEpoch(windowAssigner.getWindowEndTime(time)));
                dataMsg.docs.emplace_back(inputDoc);
            }
        }

        for (auto& [windowStartTime, dataMsg] : inputMessages) {
            // Send the data message for this window.
            groupOperator->onDataMsg(0, std::move(dataMsg));
        }
        auto messages = sink.getMessages();
        ASSERT_EQUALS(messages.size(), 0);

        // Send the window close message(s) to the group.
        for (Date_t windowToClose : windowsToClose) {
            StreamControlMsg closeWindowMsg{.windowCloseSignal = windowToClose.asInt64()};
            groupOperator->onControlMsg(0, std::move(closeWindowMsg));
        }

        messages = sink.getMessages();
        ASSERT_EQUALS(messages.size(), expectedOutputMessages);

        // Assert an output pattern of:
        //  Data message(s) for window X
        //  Window close signal for window X
        //  Date messages(s) for window Y
        //  Window close signal for window Y
        //  ...
        std::vector<StreamDocument> outputDocs;
        boost::optional<Date_t> currentStartTimestamp;
        while (!messages.empty()) {
            auto& msg = messages.front();
            if (msg.dataMsg) {
                for (auto& doc : msg.dataMsg->docs) {
                    ASSERT(doc.streamMeta.getWindowStartTimestamp());
                    if (!currentStartTimestamp) {
                        currentStartTimestamp = *doc.streamMeta.getWindowStartTimestamp();
                    }
                    ASSERT_EQ(currentStartTimestamp, *doc.streamMeta.getWindowStartTimestamp());
                    outputDocs.push_back(doc);
                }
            } else {
                invariant(msg.controlMsg);
                currentStartTimestamp = boost::none;
            }
            messages.pop_front();
        }

        return {outputDocs, groupOperator->getStats()};
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    const TimeZoneDatabase timeZoneDb{};
    const TimeZone timeZone = timeZoneDb.getTimeZone("UTC");
};

TEST_F(WindowAwareOperatorTest, SingleGroup_OneWindow) {
    const auto windowSize = 5;
    const auto numDocuments = 10;
    const auto expectedWindowStartTime = timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0);
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = 5,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};

    const std::string groupSpec = R"(
{
    $group: {
        _id: null,
        sum: { $sum: "$val" }
    }
})";

    // Create the input. Each doc is assigned a timestamp within the first window.
    std::vector<StreamDocument> inputDocs;
    inputDocs.reserve(numDocuments);
    for (size_t i = 0; i < numDocuments; ++i) {
        StreamDocument doc{Document{fromjson(fmt::format("{{id: {}, val: {}}}", i, i))}};
        // Assign the document a timestamp that will put it in the first window.
        doc.minEventTimestampMs =
            (expectedWindowStartTime + Seconds(int64_t(i % windowSize))).toMillisSinceEpoch();
        inputDocs.push_back(std::move(doc));
    }

    // Run the test scenario.
    // 1 message for the window output, 1 control message for the window close signal.
    auto expectedOutputMessages = 2;
    auto [outputDocs, _] = testGroupNotWindowAssigner(fromjson(groupSpec),
                                                      inputDocs,
                                                      windowOptions,
                                                      {expectedWindowStartTime},
                                                      expectedOutputMessages);

    // Verify the expect number of output docs and their contents.
    ASSERT_EQUALS(outputDocs.size(), 1);
    ASSERT_EQUALS(45, outputDocs[0].doc["sum"].getInt());
    // Verify the output documents have the correct window assignments.
    for (auto& doc : outputDocs) {
        ASSERT(doc.streamMeta.getWindowStartTimestamp());
        ASSERT_EQUALS(expectedWindowStartTime, *doc.streamMeta.getWindowStartTimestamp());
    }
}

TEST_F(WindowAwareOperatorTest, OneGroup_MultipleWindows) {
    const std::string groupSpec = R"(
{
    $group: {
        _id: null,
        sum: { $sum: "$val" }
    }
})";
    const auto windowSize = 5;
    const auto numDocuments = 10;
    const auto documentsPerWindow = 2;
    std::vector<Date_t> expectedWindowStartTimes = {
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0),
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 5, 0),
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 10, 0),
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 15, 0),
    };
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};

    // Create the input.
    // There are {numDocuments} in each of the {expectedWindowStartTimes}.
    std::vector<StreamDocument> inputDocs;
    inputDocs.reserve(numDocuments);
    for (auto& window : expectedWindowStartTimes) {
        for (size_t i = 0; i < documentsPerWindow; ++i) {
            StreamDocument doc{Document{fromjson(fmt::format("{{id: {}, val: 1}}", i))}};
            // Assign the document a timestamp that will put it in the window.
            doc.minEventTimestampMs =
                (window + Seconds(int64_t(i % windowSize))).toMillisSinceEpoch();
            inputDocs.push_back(std::move(doc));
        }
    }

    // Run the test scenario.
    // For each window, 1 data message and 1 control msg.
    auto expectedOutputMessages = expectedWindowStartTimes.size() * 2;
    auto [outputDocs, _] = testGroupNotWindowAssigner(fromjson(groupSpec),
                                                      inputDocs,
                                                      windowOptions,
                                                      expectedWindowStartTimes,
                                                      expectedOutputMessages);

    // Verify the expect number of output docs and their contents.
    // We expect 1 output document for each window.
    ASSERT_EQUALS(outputDocs.size(), expectedWindowStartTimes.size());
    int idx = 0;
    for (auto& doc : outputDocs) {
        // Verify the sum for each window is correct.
        ASSERT_EQUALS(documentsPerWindow, doc.doc["sum"].getInt());
        // Verify the output docs have the correct window assignments.
        ASSERT_EQUALS(expectedWindowStartTimes[idx++], *doc.streamMeta.getWindowStartTimestamp());
    }
}

TEST_F(WindowAwareOperatorTest, TwoGroupsAndASort_MultipleWindows) {
    const std::string group1Spec = R"(
{
    $group: {
        _id: "$customerId",
        sum: { $sum: "$val" }
    }
})";

    const std::string projectSpec = R"(
{
    $project: {
        customerId: "$_id",
        sum: 1
    }
})";

    const std::string sortSpec = R"(
{
    $project: {
        customerId: 1
    }
})";

    const std::string group2Spec = R"(
{
    $group: {
        _id: null,
        results: { $push: { sum: '$sum', customerId: '$customerId' } }
    }
})";

    const auto windowSize = 5;
    std::vector<Date_t> expectedWindowStartTimes = {
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0),
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 5, 0),
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 10, 0),
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, 15, 0),
    };
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};

    // Create the input.
    // For there are windowStartTime.getSeconds() unique customers.
    // Each customer has 4 docs each with a val of 1.
    auto docsPerCustomer = 4;
    std::vector<StreamDocument> inputDocs;
    stdx::unordered_map<int64_t, std::vector<int>> expectedCustomersInWindows;
    for (auto& window : expectedWindowStartTimes) {
        auto seconds = timeZone.dateParts(window).second;
        std::vector<int> customersInThisWindow;
        for (int customerId = seconds; customerId < seconds + seconds; ++customerId) {
            for (int docIdx = 0; docIdx < docsPerCustomer; ++docIdx) {
                StreamDocument doc{
                    Document{fromjson(fmt::format("{{customerId: {}, val: 1}}", customerId))}};
                // Assign the document a timestamp that will put it in the window.
                doc.minEventTimestampMs =
                    (window + Seconds(int64_t(customerId % windowSize))).toMillisSinceEpoch();
                inputDocs.push_back(std::move(doc));
                // Add this customer to the expected window results.
                if (customersInThisWindow.empty() || customersInThisWindow.back() != customerId) {
                    customersInThisWindow.push_back(customerId);
                }
            }
        }
        expectedCustomersInWindows[window.toMillisSinceEpoch()] = customersInThisWindow;
    }

    // Create the operators: [$group, $project, $sort, $group, $inMemorySink].
    auto group1Stage = createGroupStage(fromjson(group1Spec));
    ASSERT(group1Stage);
    WindowAwareGroupOperator::Options options(WindowAwareOperator::Options{
        std::make_unique<WindowAssigner>(windowOptions), true /* sendWindowCloseSignal */});
    options.documentSource = group1Stage.get();
    auto group1 = std::make_unique<WindowAwareGroupOperator>(_context.get(), std::move(options));

    auto projectDocumentSource = DocumentSourceProject::createFromBson(
        fromjson(projectSpec).firstElement(), _context->expCtx);
    SingleDocumentTransformationOperator::Options projectOptions{
        .documentSource =
            dynamic_cast<DocumentSourceSingleDocumentTransformation*>(projectDocumentSource.get())};
    auto project = std::make_unique<ProjectOperator>(_context.get(), std::move(projectOptions));

    boost::intrusive_ptr<DocumentSourceSort> sortStage = dynamic_cast<DocumentSourceSort*>(
        DocumentSourceSort::createFromBson(fromjson(sortSpec).firstElement(), _context->expCtx)
            .get());
    ASSERT_TRUE(sortStage);
    WindowAwareSortOperator::Options sortOptions{
        WindowAwareOperator::Options{.sendWindowCloseSignal = true}};
    sortOptions.documentSource = sortStage.get();
    auto sort = std::make_unique<WindowAwareSortOperator>(_context.get(), std::move(sortOptions));

    WindowAwareGroupOperator::Options group2Options(
        WindowAwareOperator::Options{.sendWindowCloseSignal = true});
    auto group2Stage = createGroupStage(fromjson(group2Spec));
    group2Options.documentSource = group2Stage.get();
    ASSERT(group2Stage);
    auto group2 =
        std::make_unique<WindowAwareGroupOperator>(_context.get(), std::move(group2Options));

    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);

    // Link the operators and start them.
    group1->addOutput(project.get(), 0);
    project->addOutput(sort.get(), 0);
    sort->addOutput(group2.get(), 0);
    group2->addOutput(&sink, 0);
    sink.start();
    group2->start();
    project->start();
    group1->start();

    // Split the input across two data messages.
    StreamDataMsg dataMsg1;
    StreamDataMsg dataMsg2;
    for (size_t i = 0; i < inputDocs.size() / 2; ++i) {
        dataMsg1.docs.push_back(inputDocs[i]);
    }
    for (size_t i = inputDocs.size() / 2; i < inputDocs.size(); ++i) {
        dataMsg2.docs.push_back(inputDocs[i]);
    }

    // Send the data messages.
    group1->onDataMsg(0, dataMsg1);
    group1->onDataMsg(0, dataMsg2);
    // Send a watermark message that should close the last window.
    int64_t lastWindowEndTime = expectedWindowStartTimes.back().toMillisSinceEpoch() +
        toMillis(windowOptions.sizeUnit, windowOptions.size);
    group1->onControlMsg(0,
                         StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                              .eventTimeWatermarkMs = lastWindowEndTime}});

    auto results = sink.getMessages();
    for (Date_t startTime : expectedWindowStartTimes) {
        auto windowStartTime = startTime.toMillisSinceEpoch();
        auto expectedCustomers = expectedCustomersInWindows[windowStartTime];
        if (expectedCustomers.empty()) {
            // If there's no expected results in the window,
            // we won't see any messages related to it.
            continue;
        }

        auto msg = results.front();
        results.pop_front();

        ASSERT(msg.dataMsg);
        ASSERT_EQ(1, msg.dataMsg->docs.size());
        auto& doc = msg.dataMsg->docs[0];
        ASSERT_EQ(windowStartTime, doc.streamMeta.getWindowStartTimestamp()->toMillisSinceEpoch());
        ASSERT_EQ(windowStartTime + toMillis(windowOptions.sizeUnit, windowOptions.size),
                  doc.streamMeta.getWindowEndTimestamp()->toMillisSinceEpoch());

        ASSERT(doc.doc["results"].isArray());
        std::cout << doc.doc["results"].toString() << std::endl;
        auto arr = doc.doc["results"].getArray();
        ASSERT_EQ(expectedCustomers.size(), arr.size());
        for (int idx = 0; size_t(idx) < expectedCustomers.size(); ++idx) {
            ASSERT_EQ(expectedCustomers[idx],
                      arr[idx].getDocument().getField("customerId").getInt());
        }

        msg = results.front();
        results.pop_front();

        ASSERT(msg.controlMsg);
        ASSERT(msg.controlMsg->windowCloseSignal);
        ASSERT_EQ(windowStartTime, *msg.controlMsg->windowCloseSignal);
    }
    auto msg = results.front();
    results.pop_front();
    ASSERT(msg.controlMsg);
    ASSERT(msg.controlMsg->watermarkMsg);
    ASSERT_EQ(lastWindowEndTime, msg.controlMsg->watermarkMsg->eventTimeWatermarkMs);
}


}  // namespace
}  // namespace streams
