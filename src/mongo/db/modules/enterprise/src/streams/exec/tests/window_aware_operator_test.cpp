/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/duration.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/limit_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/single_document_transformation_operator.h"
#include "streams/exec/sort_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_operator.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class WindowAwareOperatorTest : public AggregationContextFixture {
public:
    WindowAwareOperatorTest() : AggregationContextFixture() {
        auto [context, executor] = getTestContext(/*svcCtx*/ nullptr);
        _context = std::move(context);
        _executor = std::move(executor);
        _context->checkpointStorage = std::make_unique<InMemoryCheckpointStorage>(_context.get());
        _context->checkpointStorage->registerMetrics(_executor->getMetricManager());
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

        GroupOperator::Options options{WindowAwareOperator::Options{.sendWindowSignals = true}};
        options.documentSource = groupStage.get();
        auto groupOperator = std::make_unique<GroupOperator>(_context.get(), std::move(options));

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
                StreamMetaWindow streamMetaWindow;
                streamMetaWindow.setStart(mongo::Date_t::fromMillisSinceEpoch(time));
                streamMetaWindow.setEnd(
                    mongo::Date_t::fromMillisSinceEpoch(windowAssigner.getWindowEndTime(time)));
                inputDoc.streamMeta.setWindow(streamMetaWindow);
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
            StreamControlMsg closeWindowMsg{
                .windowCloseSignal = streams::WindowCloseMsg{Value(), windowToClose.asInt64()}};
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
                    ASSERT(doc.streamMeta.getWindow()->getStart());
                    if (!currentStartTimestamp) {
                        currentStartTimestamp = *doc.streamMeta.getWindow()->getStart();
                    }
                    ASSERT_EQ(currentStartTimestamp, *doc.streamMeta.getWindow()->getStart());
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

    void checkLimitWindow(LimitOperator* limitOperator,
                          int64_t windowStart,
                          int64_t expectedNumSent) const {
        auto it = limitOperator->_windows.find(windowStart);
        ASSERT_NOT_EQUALS(it, limitOperator->_windows.end());
        auto limitWindow = limitOperator->getLimitWindow(it->second.get());
        ASSERT_EQUALS(expectedNumSent, limitWindow->numSent);
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;
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
        ASSERT(doc.streamMeta.getWindow()->getStart());
        ASSERT_EQUALS(expectedWindowStartTime, *doc.streamMeta.getWindow()->getStart());
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
        ASSERT_EQUALS(expectedWindowStartTimes[idx++], *doc.streamMeta.getWindow()->getStart());
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
    GroupOperator::Options options(WindowAwareOperator::Options{
        std::make_unique<WindowAssigner>(windowOptions), true /* sendWindowSignals */});
    options.documentSource = group1Stage.get();
    auto group1 = std::make_unique<GroupOperator>(_context.get(), std::move(options));

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
    SortOperator::Options sortOptions{WindowAwareOperator::Options{.sendWindowSignals = true}};
    sortOptions.documentSource = sortStage.get();
    auto sort = std::make_unique<SortOperator>(_context.get(), std::move(sortOptions));

    GroupOperator::Options group2Options(WindowAwareOperator::Options{.sendWindowSignals = true});
    auto group2Stage = createGroupStage(fromjson(group2Spec));
    group2Options.documentSource = group2Stage.get();
    ASSERT(group2Stage);
    auto group2 = std::make_unique<GroupOperator>(_context.get(), std::move(group2Options));

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
        ASSERT_EQ(windowStartTime, doc.streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
        ASSERT_EQ(windowStartTime + toMillis(windowOptions.sizeUnit, windowOptions.size),
                  doc.streamMeta.getWindow()->getEnd()->toMillisSinceEpoch());

        ASSERT(doc.doc["results"].isArray());
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
        ASSERT_EQ(windowStartTime, msg.controlMsg->windowCloseSignal->windowStartTime);
    }
    auto msg = results.front();
    results.pop_front();
    ASSERT(msg.controlMsg);
    ASSERT(msg.controlMsg->watermarkMsg);
    ASSERT_EQ(lastWindowEndTime - 1, msg.controlMsg->watermarkMsg->eventTimeWatermarkMs);
}

/**
 * This is a "dummy" window aware operator. It's used to test the WindowAwareOperator base class
 * checkpointing. The dummy window aware operator just stores all the docs in the window in a
 * vector.
 */
class DummyWindowOperator : public WindowAwareOperator {
public:
    DummyWindowOperator(Context* context, WindowAwareOperator::Options options)
        : WindowAwareOperator(context), _options(std::move(options)) {}

protected:
    struct DummyWindow : public WindowAwareOperator::Window {
        DummyWindow(WindowAwareOperator::Window base) : Window(std::move(base)) {}
        std::vector<Document> docs;
    };

    std::string doGetName() const override {
        return "DummyOperator";
    }

private:
    DummyWindow* getDummyWindow(Window* window) {
        auto dummyWindow = dynamic_cast<DummyWindow*>(window);
        invariant(dummyWindow);
        return dummyWindow;
    }

    void doProcessDocs(Window* window, std::vector<StreamDocument> streamDocs) override {
        for (auto& doc : streamDocs) {
            getDummyWindow(window)->docs.push_back(std::move(doc.doc));
        }
    }

    std::unique_ptr<Window> doMakeWindow(Window baseState) override {
        return std::make_unique<DummyWindow>(std::move(baseState));
    }

    void doCloseWindow(Window* window) override {
        StreamDataMsg dataMsg;
        for (auto& doc : getDummyWindow(window)->docs) {
            StreamDocument streamDoc{std::move(doc)};
            streamDoc.streamMeta = window->streamMetaTemplate;
            dataMsg.docs.push_back(std::move(streamDoc));
        }
        sendDataMsg(0, std::move(dataMsg));
    }

    void doUpdateStats(Window* window) override {
        int64_t memorySize{0};
        for (auto& doc : getDummyWindow(window)->docs) {
            memorySize += doc.computeSize();
        }
        window->stats.memoryUsageBytes = memorySize;
    }

    void doSaveWindowState(CheckpointStorage::WriterHandle* writer, Window* window) override {
        for (const auto& doc : getDummyWindow(window)->docs) {
            MutableDocument record;
            record[WindowOperatorCheckpointRecord::kTestOnlyDataFieldName] = Value{doc};
            _context->checkpointStorage->appendRecord(writer, record.freeze());
        }
    }

    void doRestoreWindowState(Window* window, Document record) override {
        ASSERT(!record[WindowOperatorCheckpointRecord::kTestOnlyDataFieldName].missing());
        getDummyWindow(window)->docs.push_back(
            record[WindowOperatorCheckpointRecord::kTestOnlyDataFieldName].getDocument());
    }

    const WindowAwareOperator::Options& getOptions() const override {
        return _options;
    }

    Options _options;
};

// Create a DummyWindowAware operator, then open two windows with data.
// Write a checkpoint, verify correct results are output. Then restore
// from the checkpoint in another operator instance, and verify we get the same results.
TEST_F(WindowAwareOperatorTest, Checkpoint_MultipleWindows_DummyOperator) {
    int windowSize = 1;
    int windowSizeMs = windowSize * 1000;
    OperatorId operatorId = 2;
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};
    DummyWindowOperator dummy(
        _context.get(),
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)});
    dummy.setOperatorId(operatorId);

    // Add a InMemorySinkOperator after the GroupOperator.
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    dummy.addOutput(&sink, 0);
    sink.start();
    dummy.start();

    auto window1 = timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0).toMillisSinceEpoch();
    auto window2 =
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize, 0).toMillisSinceEpoch();
    // This will open two windows.
    dummy.onDataMsg(0,
                    StreamDataMsg{.docs = std::vector<StreamDocument>{
                                      StreamDocument{Document{BSON("a" << 1)}, window1},
                                      StreamDocument{Document{BSON("b" << 2)}, window2},
                                      StreamDocument{Document{BSON("c" << 3)}, window1},
                                      StreamDocument{Document{BSON("d" << 4)}, window2},
                                      StreamDocument{Document{BSON("e" << 5)}, window2},
                                  }});

    // Send a checkpoint control message through the DAG.
    auto checkpointId = _context->checkpointStorage->startCheckpoint();
    dummy.onControlMsg(0,
                       StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpointId}});

    // Close both the windows and get the results.
    WatermarkControlMsg watermarkMsg{
        .watermarkStatus = WatermarkStatus::kActive,
        .eventTimeWatermarkMs =
            timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize * 2, 0).toMillisSinceEpoch(),
    };
    dummy.onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});

    auto results = queueToVector(sink.getMessages());
    // 1 checkpoint message, 1 batch of docs for window1, 1 for for window3, then the watermark
    // message.
    ASSERT_EQ(4, results.size());
    // Verify the checkpoint message.
    ASSERT(results[0].controlMsg);
    ASSERT_EQ(checkpointId, results[0].controlMsg->checkpointMsg->id);
    // Verify the window1 results.
    ASSERT(results[1].dataMsg);
    ASSERT_EQ(2, results[1].dataMsg->docs.size());
    ASSERT_EQ(window1,
              results[1].dataMsg->docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(1, results[1].dataMsg->docs[0].doc["a"].getLong());
    ASSERT_EQ(3, results[1].dataMsg->docs[1].doc["c"].getLong());
    // Verify the window2 results.
    ASSERT(results[2].dataMsg);
    ASSERT_EQ(3, results[2].dataMsg->docs.size());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(2, results[2].dataMsg->docs[0].doc["b"].getInt());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[1].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(4, results[2].dataMsg->docs[1].doc["d"].getInt());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[1].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(5, results[2].dataMsg->docs[2].doc["e"].getInt());
    // Verify the watermark
    ASSERT(results[3].controlMsg);
    ASSERT(results[3].controlMsg->watermarkMsg);
    ASSERT_EQ(window2 + windowSizeMs - 1,
              results[3].controlMsg->watermarkMsg->eventTimeWatermarkMs);
    ASSERT_EQ(WatermarkStatus::kActive, results[3].controlMsg->watermarkMsg->watermarkStatus);

    // Now, restore the checkpoint data into a new operator.
    _context->restoreCheckpointId = checkpointId;
    DummyWindowOperator restoredDummy(
        _context.get(),
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)});
    restoredDummy.setOperatorId(operatorId);
    InMemorySinkOperator restoredSink(_context.get(), /*numInputs*/ 1);
    restoredDummy.addOutput(&restoredSink, 0);
    restoredSink.start();
    restoredDummy.start();
    // Send the watermark through and get the results.
    restoredDummy.onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});
    auto resultsAfterRestore = queueToVector(restoredSink.getMessages());

    // Verify we get the same results as above, except for the first checkpoint commit message
    // result.
    ASSERT_EQ(results.size() - 1, resultsAfterRestore.size());
    for (size_t i = 0; i < resultsAfterRestore.size(); ++i) {
        ASSERT_EQ(results[i + 1], resultsAfterRestore[i]);
    }
}

TEST_F(WindowAwareOperatorTest, SortExecutorTest) {
    boost::optional<mongo::SortExecutor<mongo::Document>> processor;
    auto expCtx = ExpressionContextBuilder{}.ns(NamespaceString::kEmpty).build();
    auto sortPattern = SortPattern(fromjson("{val: 1}"), expCtx);
    processor.emplace(SortExecutor<Document>(
        sortPattern, 100, std::numeric_limits<uint64_t>::max(), "", false, true));

    std::vector<std::string> vals = {"-a", "axd", "hey", ",n0", "hi", "$$", "aa"};
    for (auto& val : vals) {
        auto key = mongo::Value(val);
        auto doc = fromjson(fmt::format("{{val: \"{}\"}}", val));
        processor->add(key, Document(doc));
    }

    processor->pauseLoading();
    int n = 0;
    while (processor->hasNext()) {
        auto [key, doc] = processor->getNext();
        ASSERT_EQ(key.coerceToString(), vals[n]);
        n++;
    }
    processor->resumeLoading();

    processor->loadingDone();
    std::vector<BSONObj> expectedOutputDocs;
    std::sort(vals.begin(), vals.end());
    expectedOutputDocs.reserve(vals.size());
    for (auto& val : vals) {
        expectedOutputDocs.emplace_back(fromjson(fmt::format("{{val: \"{}\"}}", val)));
    }
    n = 0;
    while (processor->hasNext()) {
        auto [key, doc] = processor->getNext();
        ASSERT_BSONOBJ_EQ(doc.toBson(), expectedOutputDocs[n]);
        n++;
    }
    ASSERT_EQ(n, vals.size());
}

TEST_F(WindowAwareOperatorTest, Checkpoint_MultipleWindows_SortOperator) {
    int windowSize = 1;
    int windowSizeMs = windowSize * 1000;
    OperatorId operatorId = 2;
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};
    const std::string sortSpec = R"(
    {
        $sort: {
            customerId: 1
        }
    })";
    boost::intrusive_ptr<DocumentSourceSort> sortStage = dynamic_cast<DocumentSourceSort*>(
        DocumentSourceSort::createFromBson(fromjson(sortSpec).firstElement(), _context->expCtx)
            .get());
    ASSERT_TRUE(sortStage);
    SortOperator::Options sortOptions{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    sortOptions.documentSource = sortStage.get();
    auto sort = std::make_unique<SortOperator>(_context.get(), std::move(sortOptions));
    sort->setOperatorId(operatorId);

    // Add a InMemorySinkOperator after the SortOperator.
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    sort->addOutput(&sink, 0);
    sink.start();
    sort->start();

    auto window1 = timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0).toMillisSinceEpoch();
    auto window2 =
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize, 0).toMillisSinceEpoch();
    // This will open two windows.
    // documents are sent out of order intentionally (customerId field)
    sort->onDataMsg(
        0,
        StreamDataMsg{
            .docs = std::vector<StreamDocument>{
                StreamDocument{Document{fromjson(fmt::format("{{customerId: {}, val: 1}}", 3))},
                               window1},
                StreamDocument{Document{fromjson(fmt::format("{{customerId: {}, val: 2}}", 2))},
                               window2},
                StreamDocument{Document{fromjson(fmt::format("{{customerId: {}, val: 3}}", 1))},
                               window1},
                StreamDocument{Document{fromjson(fmt::format("{{customerId: {}, val: 4}}", 5))},
                               window2},
                StreamDocument{Document{fromjson(fmt::format("{{customerId: {}, val: 5}}", 4))},
                               window2},
            }});

    // Send a checkpoint control message through the DAG.
    auto checkpointId = _context->checkpointStorage->startCheckpoint();
    sort->onControlMsg(0,
                       StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpointId}});

    auto statsBeforeCheckpoint = sort->getStats();
    // Close both the windows and get the results.
    WatermarkControlMsg watermarkMsg{
        .watermarkStatus = WatermarkStatus::kActive,
        .eventTimeWatermarkMs =
            timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize * 2, 0).toMillisSinceEpoch(),
    };
    sort->onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});

    auto results = queueToVector(sink.getMessages());
    // 1 checkpoint message, 1 batch of docs for window1, 1 for for window3, then the watermark
    // message.
    ASSERT_EQ(4, results.size());
    // Verify the checkpoint message.
    ASSERT(results[0].controlMsg);
    ASSERT_EQ(checkpointId, results[0].controlMsg->checkpointMsg->id);

    // Verify the window1 results.
    ASSERT(results[1].dataMsg);
    ASSERT_EQ(2, results[1].dataMsg->docs.size());
    ASSERT_EQ(window1,
              results[1].dataMsg->docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(1, results[1].dataMsg->docs[0].doc["customerId"].getLong());
    ASSERT_EQ(3, results[1].dataMsg->docs[1].doc["customerId"].getLong());
    // Verify the window2 results.
    ASSERT(results[2].dataMsg);
    ASSERT_EQ(3, results[2].dataMsg->docs.size());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(2, results[2].dataMsg->docs[0].doc["customerId"].getInt());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[1].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(4, results[2].dataMsg->docs[1].doc["customerId"].getInt());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[1].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(5, results[2].dataMsg->docs[2].doc["customerId"].getInt());

    // Verify the watermark
    ASSERT(results[3].controlMsg);
    ASSERT(results[3].controlMsg->watermarkMsg);
    ASSERT_EQ(window2 + windowSizeMs - 1,
              results[3].controlMsg->watermarkMsg->eventTimeWatermarkMs);
    ASSERT_EQ(WatermarkStatus::kActive, results[3].controlMsg->watermarkMsg->watermarkStatus);

    // Now, restore the checkpoint data into a new operator.
    _context->restoreCheckpointId = checkpointId;
    SortOperator::Options sortOptionsRestored{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    sortOptionsRestored.documentSource = sortStage.get();
    auto restoredSort =
        std::make_unique<SortOperator>(_context.get(), std::move(sortOptionsRestored));
    restoredSort->setOperatorId(operatorId);
    InMemorySinkOperator restoredSink(_context.get(), 1);
    restoredSort->addOutput(&restoredSink, 0);
    restoredSink.start();
    restoredSort->start();
    auto statsAfterCheckpoint = restoredSort->getStats();
    ASSERT_EQ(statsAfterCheckpoint.memoryUsageBytes, statsBeforeCheckpoint.memoryUsageBytes);

    // Send the watermark through and get the results.
    restoredSort->onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});
    auto resultsAfterRestore = queueToVector(restoredSink.getMessages());

    // Verify we get the same results as above, except for the first checkpoint commit message
    // result.
    ASSERT_EQ(results.size() - 1, resultsAfterRestore.size());
    for (size_t i = 0; i < resultsAfterRestore.size(); ++i) {
        // TODO(SERVER-94796): We need to remove the minEventTimestampMs and maxEventTimestampMs
        // fields from dataMsg.docs in `results` in order for the assertion to pass because the
        // checkpoint doesn't yet save these fields.
        if (results[i + 1].dataMsg) {
            auto defaultMax = std::numeric_limits<int64_t>::max();
            for (auto& doc : results[i + 1].dataMsg->docs) {
                doc.minEventTimestampMs = defaultMax;
                doc.maxEventTimestampMs = -1;
            }
        }

        ASSERT_EQ(results[i + 1], resultsAfterRestore[i]);
    }
}

TEST_F(WindowAwareOperatorTest, Checkpoint_MultipleWindows_LimitOperator) {
    static constexpr int windowSize = 1;
    static constexpr OperatorId limitOperatorId = 2;

    auto makeLimitOptions = [&]() {
        WindowAssigner::Options windowOptions{.size = windowSize,
                                              .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                              .slide = windowSize,
                                              .slideUnit = mongo::StreamTimeUnitEnum::Second};
        LimitOperator::Options options(WindowAwareOperator::Options{
            .windowAssigner = std::make_unique<WindowAssigner>(windowOptions)});
        options.limit = 100;
        return options;
    };

    auto limit = std::make_unique<LimitOperator>(_context.get(), makeLimitOptions());
    limit->setOperatorId(limitOperatorId);

    // Add a InMemorySinkOperator after the LimitOperator.
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    limit->addOutput(&sink, 0);
    sink.start();
    limit->start();

    // Have two windows open simultaneously. First window gets one document, second window gets
    // two documents.
    auto window1 = timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0).toMillisSinceEpoch();
    auto window2 =
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize, 0).toMillisSinceEpoch();
    limit->onDataMsg(
        0,
        StreamDataMsg{.docs = std::vector<StreamDocument>{
                          StreamDocument{Document{fromjson(fmt::format("{{id: {}}}", 1))}, window1},
                          StreamDocument{Document{fromjson(fmt::format("{{id: {}}}", 2))}, window2},
                          StreamDocument{Document{fromjson(fmt::format("{{id: {}}}", 3))}, window2},
                      }});

    // Start the checkpoint process which should checkpoint the state of the two open windows.
    auto checkpointId = _context->checkpointStorage->startCheckpoint();
    limit->onControlMsg(
        0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpointId}});

    // Restore from the previous checkpoint and verify that the restored limit window state is
    // correct.
    _context->restoreCheckpointId = checkpointId;
    auto restoredLimit = std::make_unique<LimitOperator>(_context.get(), makeLimitOptions());
    restoredLimit->setOperatorId(limitOperatorId);
    restoredLimit->addOutput(&sink, 0);
    restoredLimit->start();
    checkLimitWindow(restoredLimit.get(), window1, /* expectedNumSent */ 1);
    checkLimitWindow(restoredLimit.get(), window2, /* expectedNumSent */ 2);
}

TEST_F(WindowAwareOperatorTest, Checkpoint_MultipleWindows_GroupOperator) {
    int windowSize = 5;
    OperatorId operatorId = 6;
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};
    const std::string groupSpec = R"(
    {
        $group: {
            _id: "$a",
            avg: { $avg: "$b" },
            sum: { $sum: "$b" }
        }
    })";
    boost::intrusive_ptr<DocumentSourceGroup> groupStage = createGroupStage(fromjson(groupSpec));
    GroupOperator::Options groupOptions{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    groupOptions.documentSource = groupStage.get();
    auto groupOperator = std::make_unique<GroupOperator>(_context.get(), std::move(groupOptions));
    groupOperator->setOperatorId(operatorId);

    // Add a InMemorySinkOperator after the GroupOperator.
    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    groupOperator->addOutput(&sink, 0);
    sink.start();
    groupOperator->start();

    auto window1 = timeZone.createFromDateParts(2023, 12, 1, 0, 0, 0, 0).toMillisSinceEpoch();
    auto window2 =
        timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize, 0).toMillisSinceEpoch();
    // This will open two windows.
    groupOperator->onDataMsg(
        0,
        StreamDataMsg{
            .docs = std::vector<StreamDocument>{
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 1, 2))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 2, 3))}, window2},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 3, 4))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 4, 5))}, window2},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 1, 6))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 2, 4))}, window2},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 3, 5))}, window1},
            }});

    // Send a checkpoint control message through the DAG.
    auto checkpointId = _context->checkpointStorage->startCheckpoint();
    groupOperator->onControlMsg(
        0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{.id = checkpointId}});

    auto statsBeforeCheckpoint = groupOperator->getStats();
    ASSERT_GT(statsBeforeCheckpoint.memoryUsageBytes, 0);
    // Add some more Documents
    groupOperator->onDataMsg(
        0,
        StreamDataMsg{
            .docs = std::vector<StreamDocument>{
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 1, 7))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 3, 5))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 3, 3))}, window1},
            }});

    // Close both the windows and get the results.
    WatermarkControlMsg watermarkMsg{
        .watermarkStatus = WatermarkStatus::kActive,
        .eventTimeWatermarkMs =
            timeZone.createFromDateParts(2023, 12, 1, 0, 0, windowSize * 4, 0).toMillisSinceEpoch(),
    };
    groupOperator->onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});

    auto results = queueToVector(sink.getMessages());
    // 1 checkpoint message, 1 batch of docs for window1, 1 for for window3, then the watermark
    // message.
    ASSERT_EQ(4, results.size());
    // Verify the checkpoint message.
    ASSERT(results[0].controlMsg);
    ASSERT_EQ(checkpointId, results[0].controlMsg->checkpointMsg->id);

    // Verify the window1 results.
    ASSERT(results[1].dataMsg);
    ASSERT_EQ(2, results[1].dataMsg->docs.size());
    ASSERT_EQ(window1,
              results[1].dataMsg->docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(window1,
              results[1].dataMsg->docs[1].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());

    if (results[1].dataMsg->docs[0].doc["_id"].getInt() == 1) {
        ASSERT_EQ(3, results[1].dataMsg->docs[1].doc["_id"].getInt());
        ASSERT_EQ(5, results[1].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(15, results[1].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(4.25, results[1].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(17, results[1].dataMsg->docs[1].doc["sum"].getDouble());
    } else {
        ASSERT_EQ(3, results[1].dataMsg->docs[0].doc["_id"].getInt());
        ASSERT_EQ(5, results[1].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(15, results[1].dataMsg->docs[1].doc["sum"].getDouble());
        ASSERT_EQ(4.25, results[1].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(17, results[1].dataMsg->docs[0].doc["sum"].getDouble());
    }
    // Verify the window2 results.
    ASSERT(results[2].dataMsg);
    ASSERT_EQ(2, results[2].dataMsg->docs.size());

    ASSERT_EQ(window2,
              results[2].dataMsg->docs[0].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());
    ASSERT_EQ(window2,
              results[2].dataMsg->docs[1].streamMeta.getWindow()->getStart()->toMillisSinceEpoch());

    if (results[2].dataMsg->docs[0].doc["_id"].getInt() == 2) {
        ASSERT_EQ(4, results[2].dataMsg->docs[1].doc["_id"].getInt());
        ASSERT_EQ(3.5, results[2].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(7, results[2].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(5, results[2].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(5, results[2].dataMsg->docs[1].doc["sum"].getDouble());
    } else {
        ASSERT_EQ(4, results[2].dataMsg->docs[0].doc["_id"].getInt());
        ASSERT_EQ(5, results[2].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(5, results[2].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(3.5, results[2].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(7, results[2].dataMsg->docs[1].doc["sum"].getDouble());
    }
    // Verify the watermark
    ASSERT(results[3].controlMsg);
    ASSERT(results[3].controlMsg->watermarkMsg);

    // Restore the checkpoint data into a new operator.
    _context->restoreCheckpointId = checkpointId;
    GroupOperator::Options groupOptionsRestored{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    groupOptionsRestored.documentSource = groupStage.get();
    auto restoredGroup =
        std::make_unique<GroupOperator>(_context.get(), std::move(groupOptionsRestored));
    restoredGroup->setOperatorId(operatorId);
    InMemorySinkOperator restoredSink(_context.get(), 1);
    restoredGroup->addOutput(&restoredSink, 0);
    restoredSink.start();
    restoredGroup->start();
    auto statsAfterCheckpoint = restoredGroup->getStats();
    ASSERT_EQ(statsBeforeCheckpoint.memoryUsageBytes, statsAfterCheckpoint.memoryUsageBytes);
    // Send the watermark through and get the results.
    restoredGroup->onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});
    auto resultsAfterRestore = queueToVector(restoredSink.getMessages());

    // Verify we get the same results as above, except for the first checkpoint commit message
    // result.
    ASSERT_EQ(results.size() - 1, resultsAfterRestore.size());
    ASSERT_EQ(window1,
              resultsAfterRestore[0]
                  .dataMsg->docs[0]
                  .streamMeta.getWindow()
                  ->getStart()
                  ->toMillisSinceEpoch());
    ASSERT_EQ(window1,
              resultsAfterRestore[0]
                  .dataMsg->docs[1]
                  .streamMeta.getWindow()
                  ->getStart()
                  ->toMillisSinceEpoch());
    if (resultsAfterRestore[0].dataMsg->docs[0].doc["_id"].getInt() == 1) {
        ASSERT_EQ(3, resultsAfterRestore[0].dataMsg->docs[1].doc["_id"].getInt());
        ASSERT_EQ(4, resultsAfterRestore[0].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(8, resultsAfterRestore[0].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(4.5, resultsAfterRestore[0].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(9, resultsAfterRestore[0].dataMsg->docs[1].doc["sum"].getDouble());
    } else {
        ASSERT_EQ(3, resultsAfterRestore[0].dataMsg->docs[0].doc["_id"].getInt());
        ASSERT_EQ(4, resultsAfterRestore[0].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(8, resultsAfterRestore[0].dataMsg->docs[1].doc["sum"].getDouble());
        ASSERT_EQ(4.5, resultsAfterRestore[0].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(9, resultsAfterRestore[0].dataMsg->docs[0].doc["sum"].getDouble());
    }

    ASSERT_EQ(window2,
              resultsAfterRestore[1]
                  .dataMsg->docs[0]
                  .streamMeta.getWindow()
                  ->getStart()
                  ->toMillisSinceEpoch());
    ASSERT_EQ(window2,
              resultsAfterRestore[1]
                  .dataMsg->docs[1]
                  .streamMeta.getWindow()
                  ->getStart()
                  ->toMillisSinceEpoch());
    if (resultsAfterRestore[1].dataMsg->docs[0].doc["_id"].getInt() == 2) {
        ASSERT_EQ(4, resultsAfterRestore[1].dataMsg->docs[1].doc["_id"].getInt());
        ASSERT_EQ(3.5, resultsAfterRestore[1].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(7, resultsAfterRestore[1].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(5, resultsAfterRestore[1].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(5, resultsAfterRestore[1].dataMsg->docs[1].doc["sum"].getDouble());
    } else {
        ASSERT_EQ(4, resultsAfterRestore[1].dataMsg->docs[0].doc["_id"].getInt());
        ASSERT_EQ(5, resultsAfterRestore[1].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(5, resultsAfterRestore[1].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(3.5, resultsAfterRestore[1].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(7, resultsAfterRestore[1].dataMsg->docs[1].doc["sum"].getDouble());
    }

    // Verify the watermark
    ASSERT(resultsAfterRestore[2].controlMsg);
    ASSERT(resultsAfterRestore[2].controlMsg->watermarkMsg);

    GroupOperator::Options groupOptionsRestored2{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    groupOptionsRestored2.documentSource = groupStage.get();
    auto restoredGroup2 =
        std::make_unique<GroupOperator>(_context.get(), std::move(groupOptionsRestored2));
    restoredGroup2->setOperatorId(operatorId);
    InMemorySinkOperator restoredSink2(_context.get(), 1);
    restoredGroup2->addOutput(&restoredSink2, 0);
    restoredSink2.start();
    restoredGroup2->start();

    restoredGroup2->onDataMsg(
        0,
        StreamDataMsg{
            .docs = std::vector<StreamDocument>{
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 1, 7))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 3, 5))}, window1},
                StreamDocument{Document{fromjson(fmt::format("{{a: {}, b: {}}}", 3, 3))}, window1},
            }});

    // Send the watermark through and get the results.
    restoredGroup2->onControlMsg(0, StreamControlMsg{.watermarkMsg = watermarkMsg});
    auto resultsAfterRestore2 = queueToVector(restoredSink2.getMessages());
    ASSERT_EQ(results.size() - 1, resultsAfterRestore2.size());
    ASSERT_EQ(window1,
              resultsAfterRestore2[0]
                  .dataMsg->docs[0]
                  .streamMeta.getWindow()
                  ->getStart()
                  ->toMillisSinceEpoch());
    ASSERT_EQ(window1,
              resultsAfterRestore2[0]
                  .dataMsg->docs[1]
                  .streamMeta.getWindow()
                  ->getStart()
                  ->toMillisSinceEpoch());
    if (resultsAfterRestore2[0].dataMsg->docs[0].doc["_id"].getInt() == 1) {
        ASSERT_EQ(3, resultsAfterRestore2[0].dataMsg->docs[1].doc["_id"].getInt());
        ASSERT_EQ(5, resultsAfterRestore2[0].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(15, resultsAfterRestore2[0].dataMsg->docs[0].doc["sum"].getDouble());
        ASSERT_EQ(4.25, resultsAfterRestore2[0].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(17, resultsAfterRestore2[0].dataMsg->docs[1].doc["sum"].getDouble());
    } else {
        ASSERT_EQ(3, resultsAfterRestore2[0].dataMsg->docs[0].doc["_id"].getInt());
        ASSERT_EQ(5, resultsAfterRestore2[0].dataMsg->docs[1].doc["avg"].getDouble());
        ASSERT_EQ(15, resultsAfterRestore2[0].dataMsg->docs[1].doc["sum"].getDouble());
        ASSERT_EQ(4.25, resultsAfterRestore2[0].dataMsg->docs[0].doc["avg"].getDouble());
        ASSERT_EQ(17, resultsAfterRestore2[0].dataMsg->docs[0].doc["sum"].getDouble());
    }
}

TEST_F(WindowAwareOperatorTest, MemoryTracking_GroupOperator) {
    const std::string groupSpec = R"(
    {
        $group: {
            _id: "$id",
            values: { $push: "$$ROOT" }
        }
    })";

    int windowSize{1};
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};
    auto groupStage = createGroupStage(fromjson(groupSpec));
    GroupOperator::Options groupOptions{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    groupOptions.documentSource = groupStage.get();
    auto groupOperator = std::make_unique<GroupOperator>(_context.get(), std::move(groupOptions));
    groupOperator->setOperatorId(1);

    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    groupOperator->addOutput(&sink, 0);
    sink.start();
    groupOperator->start();

    int windowMs1 = windowSize * 1000;
    int windowMs2 = windowMs1 + 1000;

    // Insert 1k documents in the same window.
    StreamDataMsg dataMsg;
    size_t numDocs{1'000};
    for (size_t i = 0; i < numDocs; ++i) {
        dataMsg.docs.push_back(
            StreamDocument(Document(fromjson(fmt::format("{{id: {}}}", 1))), windowMs1));
    }

    groupOperator->onDataMsg(0, dataMsg);

    OperatorStats stats = groupOperator->getStats();
    assertStateSize(269088, stats.memoryUsageBytes);

    // Insert another document in the same window.
    dataMsg.docs.clear();
    dataMsg.docs.push_back(
        StreamDocument(Document(fromjson(fmt::format("{{id: {}}}", 1))), windowMs1));
    groupOperator->onDataMsg(0, dataMsg);
    stats = groupOperator->getStats();
    assertStateSize(269357, stats.memoryUsageBytes);

    // Insert another document but in a new window.
    dataMsg.docs.clear();
    dataMsg.docs.push_back(
        StreamDocument(Document(fromjson(fmt::format("{{id: {}}}", 2))), windowMs2));
    groupOperator->onDataMsg(0, dataMsg);
    stats = groupOperator->getStats();
    assertStateSize(269714, stats.memoryUsageBytes);

    // Close the first window.
    groupOperator->onControlMsg(0,
                                StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                                     .eventTimeWatermarkMs = windowMs2,
                                                 }});
    stats = groupOperator->getStats();
    assertStateSize(357, stats.memoryUsageBytes);
}

TEST_F(WindowAwareOperatorTest, MemoryTracking_SortOperator) {
    const std::string sortSpec = R"(
    {
        $sort: { _id: 1 }
    })";

    int windowSize{1};
    WindowAssigner::Options windowOptions{.size = windowSize,
                                          .sizeUnit = mongo::StreamTimeUnitEnum::Second,
                                          .slide = windowSize,
                                          .slideUnit = mongo::StreamTimeUnitEnum::Second};
    boost::intrusive_ptr<DocumentSourceSort> sortStage = dynamic_cast<DocumentSourceSort*>(
        DocumentSourceSort::createFromBson(fromjson(sortSpec).firstElement(), _context->expCtx)
            .get());
    ASSERT_TRUE(sortStage);
    SortOperator::Options sortOptions{
        WindowAwareOperator::Options{.windowAssigner =
                                         std::make_unique<WindowAssigner>(windowOptions)},
    };
    sortOptions.documentSource = sortStage.get();
    auto sortOperator = std::make_unique<SortOperator>(_context.get(), std::move(sortOptions));
    sortOperator->setOperatorId(1);

    InMemorySinkOperator sink(_context.get(), /*numInputs*/ 1);
    sortOperator->addOutput(&sink, 0);
    sink.start();
    sortOperator->start();

    int windowMs1 = windowSize * 1000;
    int windowMs2 = windowMs1 + 1000;

    // Insert 1k documents in the same window.
    StreamDataMsg dataMsg;
    size_t numDocs{1'000};
    for (size_t i = 0; i < numDocs; ++i) {
        dataMsg.docs.push_back(
            StreamDocument(Document(fromjson(fmt::format("{{id: {}}}", i))), windowMs1));
    }

    sortOperator->onDataMsg(0, dataMsg);
    OperatorStats stats = sortOperator->getStats();
    assertStateSize(133000, stats.memoryUsageBytes);

    // Insert another document in the same window.
    dataMsg.docs.clear();
    dataMsg.docs.push_back(
        StreamDocument(Document(fromjson(fmt::format("{{id: {}}}", numDocs))), windowMs1));
    sortOperator->onDataMsg(0, dataMsg);
    stats = sortOperator->getStats();
    assertStateSize(133133, stats.memoryUsageBytes);

    // Insert another document but in a new window.
    dataMsg.docs.clear();
    dataMsg.docs.push_back(
        StreamDocument(Document(fromjson(fmt::format("{{id: {}}}", 1))), windowMs2));
    sortOperator->onDataMsg(0, dataMsg);
    stats = sortOperator->getStats();
    assertStateSize(133266, stats.memoryUsageBytes);

    // Close the first window.
    sortOperator->onControlMsg(0,
                               StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                                    .eventTimeWatermarkMs = windowMs2,
                                                }});
    stats = sortOperator->getStats();
    assertStateSize(133, stats.memoryUsageBytes);
}

}  // namespace streams
