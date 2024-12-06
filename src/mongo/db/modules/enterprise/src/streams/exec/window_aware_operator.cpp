/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/window_aware_operator.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/idl/idl_parser.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace {

constexpr StringData kMissedWindowsFieldName = "missedWindowStartTimes"_sd;

}

void WindowAwareOperator::doStart() {
    if (_context->restoreCheckpointId) {
        restoreState(*_context->restoreCheckpointId);
    }
    LOGV2_INFO(9531601,
               "WindowAwareOperator started",
               "context"_attr = _context,
               "name"_attr = getName(),
               "minWindowStartTime"_attr = Date_t::fromMillisSinceEpoch(_minWindowStartTime));
}

void WindowAwareOperator::doOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    if (getOptions().isSessionWindow) {
        onDataMsgSessionWindow(inputIdx, dataMsg, controlMsg);
        return;
    }
    if (getOptions().windowAssigner) {
        assignWindowsAndProcessDataMsg(std::move(dataMsg));
        updateMinMaxOpenWindowStats();
    } else {
        auto start = dataMsg.docs.front().streamMeta.getWindow()->getStart();
        auto end = dataMsg.docs.front().streamMeta.getWindow()->getEnd();
        // Process all the docs in the data message.
        processDocsInWindow(start->toMillisSinceEpoch(),
                            end->toMillisSinceEpoch(),
                            std::move(dataMsg.docs),
                            false /* projectStreamMeta */);
    }

    if (controlMsg) {
        onControlMsg(/*inputIdx*/ 0, std::move(*controlMsg));
    }
}

void WindowAwareOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    invariant(!controlMsg.eofSignal, "Unexpected eofSignal control message.");
    invariant(!(bool(controlMsg.checkpointMsg) && bool(controlMsg.windowCloseSignal)),
              "checkpointMsg and windowCloseSingal should not both be set.");
    invariant(!(bool(controlMsg.watermarkMsg) && bool(controlMsg.windowCloseSignal)),
              "watermarkMsg and windowCloseSingal should not both be set.");
    const auto& options = getOptions();

    if (options.isSessionWindow) {
        onControlMsgSessionWindow(inputIdx, controlMsg);
        return;
    }

    invariant(!controlMsg.windowMergeSignal, "Unexpected window merge control message.");

    if (controlMsg.checkpointMsg) {
        saveState(controlMsg.checkpointMsg->id);
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.watermarkMsg && options.windowAssigner) {
        // If this is the windowAssigner (the first stateful stage in the window's inner
        // pipeline), then use source watermark to find out what windows should be closed.
        // Close all the windows that should be closed by this watermark.
        processWatermarkMsg(std::move(controlMsg));
        updateMinMaxOpenWindowStats();
    } else if (controlMsg.watermarkMsg && !options.windowAssigner) {
        // Send the watermark msg along.
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.windowCloseSignal) {
        invariant(!options.windowAssigner,
                  "Unexpected windowCloseSignal when windowAssigner is set.");
        auto windowIt = _windows.find(controlMsg.windowCloseSignal->windowStartTime);
        if (windowIt != _windows.end()) {
            invariant(windowIt == _windows.begin(),
                      "Expected the minimum window to be closed first.");
            closeWindow(windowIt->second.get());
            _windows.erase(windowIt);
        }
        if (options.sendWindowSignals) {
            sendControlMsg(0, std::move(controlMsg));
        }
    }
}

void WindowAwareOperator::updateMinMaxOpenWindowStats() {
    if (_windows.empty()) {
        _stats.minOpenWindowStartTime = boost::none;
        _stats.maxOpenWindowStartTime = boost::none;
    } else {
        _stats.minOpenWindowStartTime = Date_t::fromMillisSinceEpoch(_windows.begin()->first);
        _stats.maxOpenWindowStartTime = Date_t::fromMillisSinceEpoch(_windows.rbegin()->first);
    }
}

// Assigns the docs in the input to windows and processes each.
void WindowAwareOperator::assignWindowsAndProcessDataMsg(StreamDataMsg dataMsg) {
    const auto& options = getOptions();
    invariant(options.windowAssigner);
    invariant(!dataMsg.docs.empty());

    // Organize docs according to the window(s) the doc belongs to.
    // First, sort the documents by timestamp.
    std::sort(
        dataMsg.docs.begin(), dataMsg.docs.end(), [](const auto& lhs, const auto& rhs) -> bool {
            return lhs.minEventTimestampMs < rhs.minEventTimestampMs;
        });

    int64_t nextWindowStartDocIdx{0};
    int64_t endTs = dataMsg.docs.back().minEventTimestampMs;
    int64_t nextWindowStartTs =
        options.windowAssigner->toOldestWindowStartTime(dataMsg.docs.front().minEventTimestampMs);

    // DLQ docs that are too late to fit in any open window.
    if (nextWindowStartTs < _minWindowStartTime) {
        nextWindowStartTs = _minWindowStartTime;
        while (nextWindowStartDocIdx < (int64_t)dataMsg.docs.size()) {
            const auto& doc = dataMsg.docs[nextWindowStartDocIdx];
            int64_t docTime = doc.minEventTimestampMs;
            int64_t oldestWindowStartTime =
                options.windowAssigner->toOldestWindowStartTime(docTime);
            // If some documents are late, the nextWindowStartTs might need to adjusted.
            // This is needed for the case where a late doc is followed by a doc 1+ hop ahead
            // of the _minWindowStartTime.
            nextWindowStartTs = std::max(_minWindowStartTime, oldestWindowStartTime);
            if (docTime < nextWindowStartTs) {
                sendLateDocDlqMessage(doc, oldestWindowStartTime);
                ++nextWindowStartDocIdx;
            } else {
                // This docs and following docs in the sorted batch might not be late.
                break;
            }
        }
    }

    // DLQ any docs that fit into some open windows, but missed some older closed windows.
    for (size_t i = nextWindowStartDocIdx; i < dataMsg.docs.size(); ++i) {
        int64_t minEligibleStartTime =
            options.windowAssigner->toOldestWindowStartTime(dataMsg.docs[i].minEventTimestampMs);
        if (minEligibleStartTime >= _minWindowStartTime) {
            // This doc and following docs in the sorted batch are not late.
            break;
        }
        sendLateDocDlqMessage(dataMsg.docs[i], minEligibleStartTime);
    }

    while (nextWindowStartTs <= endTs) {
        int64_t windowStart = nextWindowStartTs;
        int64_t windowEnd = options.windowAssigner->getWindowEndTime(windowStart);
        nextWindowStartTs = options.windowAssigner->getNextWindowStartTime(nextWindowStartTs);

        std::vector<StreamDocument> docsInThisWindow;
        docsInThisWindow.reserve(dataMsg.docs.size());
        bool nextWindowStartTsSet{false};
        for (size_t i = nextWindowStartDocIdx; i < dataMsg.docs.size(); ++i) {
            auto& doc = dataMsg.docs[i];
            int64_t docTs = doc.minEventTimestampMs;
            invariant(docTs >= windowStart);
            if (!nextWindowStartTsSet && docTs >= nextWindowStartTs) {
                // Fast forward to the next window if the next window is after the initially
                // expected next window (current window + slide).
                int64_t oldestWindowTs = options.windowAssigner->toOldestWindowStartTime(docTs);
                nextWindowStartTs = std::max(nextWindowStartTs, oldestWindowTs);
                nextWindowStartDocIdx = i;
                nextWindowStartTsSet = true;
            }

            if (options.windowAssigner->windowContains(windowStart, windowEnd, docTs)) {
                docsInThisWindow.push_back(doc);
            } else {
                // Done collecting documents for the current window.
                break;
            }
        }

        processDocsInWindow(
            windowStart,
            windowEnd,
            std::move(docsInThisWindow),
            _context->shouldProjectStreamMetaPriorToSinkStage() /* projectStreamMeta */);
    }
}

OperatorStats WindowAwareOperator::doGetStats() {
    // Add together all the memory usage and DLQ-ed docs of all the open windows.
    int64_t memoryUsageBytes{0};

    if (getOptions().isSessionWindow) {
        for (const auto& [partition, partitionWindows] : _sessionWindows) {
            for (const auto& window : partitionWindows) {
                memoryUsageBytes += window->stats.memoryUsageBytes;
            }
            memoryUsageBytes += partition.getApproximateSize();
        }
    } else {
        for (auto& [startTime, window] : _windows) {
            memoryUsageBytes += window->stats.memoryUsageBytes;
        }
    }
    _stats.memoryUsageBytes = memoryUsageBytes;
    return _stats;
}

// Process documents for a particular window.
void WindowAwareOperator::processDocsInWindow(int64_t windowStartTime,
                                              int64_t windowEndTime,
                                              std::vector<StreamDocument> streamDocs,
                                              bool projectStreamMeta) {
    invariant(!streamDocs.empty());
    const auto& firstStreamMeta = streamDocs.front().streamMeta;
    auto [window, isNew] = addOrGetWindow(
        windowStartTime,
        windowEndTime,
        firstStreamMeta.getSource() ? firstStreamMeta.getSource()->getType() : boost::none);
    if (!window->status.isOK()) {
        return;
    }
    if (getOptions().windowAssigner && _context->checkpointStorage && isNew) {
        window->replayCheckpointId = _context->checkpointStorage->onWindowOpen();
    }

    if (projectStreamMeta) {
        for (auto& streamDoc : streamDocs) {
            auto newStreamMeta = updateStreamMeta(
                streamDoc.doc.getField(*_context->streamMetaFieldName), window->streamMetaTemplate);
            MutableDocument mutableDoc(std::move(streamDoc.doc));
            mutableDoc.setField(*_context->streamMetaFieldName, Value(std::move(newStreamMeta)));
            streamDoc.doc = mutableDoc.freeze();
        }
    }

    try {
        doProcessDocs(window, std::move(streamDocs));
    } catch (const DBException& e) {
        // If an exception escapes the doProcessDocs, we need to DLQ the whole window.
        window->status = e.toStatus();
    }
}

// Add a new window or get an existing window.
std::pair<WindowAwareOperator::Window*, bool> WindowAwareOperator::addOrGetWindow(
    int64_t windowStartTime,
    int64_t windowEndTime,
    boost::optional<StreamMetaSourceTypeEnum> sourceType) {
    if (!_windows.contains(windowStartTime)) {
        // Add a new window.
        StreamMetaSource streamMetaSource;
        streamMetaSource.setType(sourceType);
        StreamMetaWindow streamMetaWindow;
        streamMetaWindow.setStart(Date_t::fromMillisSinceEpoch(windowStartTime));
        streamMetaWindow.setEnd(Date_t::fromMillisSinceEpoch(windowEndTime));
        StreamMeta streamMetaTemplate;
        streamMetaTemplate.setSource(std::move(streamMetaSource));
        streamMetaTemplate.setWindow(std::move(streamMetaWindow));
        auto window = makeWindow(std::move(streamMetaTemplate));
        auto result = _windows.emplace(windowStartTime, std::move(window));
        invariant(result.second);
        return {result.first->second.get(), true};
    }

    return {_windows[windowStartTime].get(), false};
}

// Called when a window is closed. Sends the window output to the next operator.
void WindowAwareOperator::closeWindow(Window* window) {
    if (!window->status.isOK()) {
        // If the window has error-ed, send a message to the DLQ.
        // Note: we might want to send this DLQ message earlier, instead
        // of waiting for the window to close.
        auto numDlqBytes =
            _context->dlq->addMessage(toDeadLetterQueueMsg(_context->streamMetaFieldName,
                                                           window->streamMetaTemplate,
                                                           getName(),
                                                           std::move(window->status.reason())));
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        return;
    }
    doCloseWindow(window);
}

std::unique_ptr<WindowAwareOperator::Window> WindowAwareOperator::makeWindow(
    StreamMeta streamMetaTemplate) {
    return doMakeWindow(WindowAwareOperator::Window{std::move(streamMetaTemplate)});
}

void WindowAwareOperator::saveState(CheckpointId checkpointId) {
    tassert(8279702, "Expected the new checkpoint storage to be set.", _context->checkpointStorage);
    auto writer = _context->checkpointStorage->createStateWriter(checkpointId, _operatorId);

    // First, write the minimum window start time.
    WindowOperatorCheckpointRecord minWindowStartTimeRecord;
    LOGV2_INFO(9531606,
               "Saving window state",
               "minWindowStartTime"_attr = _minWindowStartTime,
               "replayMinWindowStartTime"_attr = _replayMinWindowStartTime);
    minWindowStartTimeRecord.setMinWindowStartTime(_minWindowStartTime);
    minWindowStartTimeRecord.setReplayMinWindowStartTime(_replayMinWindowStartTime);

    _context->checkpointStorage->appendRecord(writer.get(),
                                              Document{minWindowStartTimeRecord.toBSON()});

    _context->checkpointStorage->addMinWindowStartTime(_minWindowStartTime);

    // Write the state of all the open windows.
    for (auto& [windowStartTime, window] : _windows) {
        // Write the window start record.
        WindowOperatorStartRecord windowStart(
            windowStartTime,
            window->streamMetaTemplate.getWindow()->getEnd()->toMillisSinceEpoch());
        if (window->streamMetaTemplate.getSource() &&
            window->streamMetaTemplate.getSource()->getType()) {
            windowStart.setSourceType(window->streamMetaTemplate.getSource()->getType());
        }
        windowStart.setReplayCheckpointId(window->replayCheckpointId);
        WindowOperatorCheckpointRecord startRecord;
        startRecord.setWindowStart(std::move(windowStart));
        _context->checkpointStorage->appendRecord(writer.get(),
                                                  Document{std::move(startRecord).toBSON()});

        // Write the data for this window.
        doSaveWindowState(writer.get(), window.get());

        // Write the window end record.
        WindowOperatorEndRecord windowEnd;
        windowEnd.setWindowEndMarker(windowStartTime);
        WindowOperatorCheckpointRecord endRecord;
        endRecord.setWindowEnd(std::move(windowEnd));
        _context->checkpointStorage->appendRecord(writer.get(),
                                                  Document{std::move(endRecord).toBSON()});
    }
}

void WindowAwareOperator::restoreState(CheckpointId checkpointId) {
    tassert(8279703, "Expected the new checkpoint storage to be set.", _context->checkpointStorage);

    if (_context->restoredCheckpointInfo && _context->restoredCheckpointInfo->minWindowStartTime) {
        // We enter this block when restoring after a modify operation.
        _minWindowStartTime = *_context->restoredCheckpointInfo->minWindowStartTime;
        _replayMinWindowStartTime = _minWindowStartTime;
        return;
    }

    auto reader = _context->checkpointStorage->createStateReader(checkpointId, _operatorId);
    IDLParserContext parserContext("WindowAwareOperatorCheckpointRestore");

    // Restore the minumum window start time.
    boost::optional<Document> record = _context->checkpointStorage->getNextRecord(reader.get());
    if (!record) {
        // In case of a modify request, there is no need to recover the window state.
        // The document are replayed from the $source with the modified pipeline.
        return;
    }
    tassert(8318300, "Expected record", record);
    auto minWindowStartTime =
        record->getField(WindowOperatorCheckpointRecord::kMinWindowStartTimeFieldName);
    tassert(8318301,
            "Expected minWindowStartTime record",
            minWindowStartTime.getType() == BSONType::NumberLong);
    _minWindowStartTime = minWindowStartTime.getLong();

    auto replayMinWindowStartTime =
        record->getField(WindowOperatorCheckpointRecord::kReplayMinWindowStartTimeFieldName);
    if (!replayMinWindowStartTime.missing()) {
        tassert(8318305,
                "Expected replayMinWindowStartTimeFieldName to be long",
                replayMinWindowStartTime.getType() == BSONType::NumberLong);
        _replayMinWindowStartTime = replayMinWindowStartTime.getLong();
    }

    // Restore all the open windows.
    while (boost::optional<Document> record =
               _context->checkpointStorage->getNextRecord(reader.get())) {
        // Parse and add the new window.
        auto windowStart = record->getField(WindowOperatorCheckpointRecord::kWindowStartFieldName);
        tassert(8289701, "Expected window start record", windowStart.getType() == BSONType::Object);
        auto windowStartDoc = windowStart.getDocument();

        // Parse the startTime, endTime, and sourceType from the record.
        auto startTime = windowStartDoc[WindowOperatorStartRecord::kStartTimeMsFieldName];
        tassert(8289702,
                "Expected startTime field of type long",
                startTime.getType() == BSONType::NumberLong);
        auto endTime = windowStartDoc[WindowOperatorStartRecord::kEndTimeMsFieldName];
        tassert(8289703,
                "Expected endTime field of type long",
                endTime.getType() == BSONType::NumberLong);
        boost::optional<StreamMetaSourceTypeEnum> sourceTypeEnum;
        auto sourceType = windowStartDoc[WindowOperatorStartRecord::kSourceTypeFieldName];
        if (!sourceType.missing()) {
            tassert(8289710,
                    "Expected sourceType field of type string",
                    sourceType.getType() == BSONType::String);
            sourceTypeEnum = StreamMetaSourceType_parse(parserContext, sourceType.getString());
        }

        auto [window, isNew] =
            addOrGetWindow(startTime.getLong(), endTime.getLong(), sourceTypeEnum);
        tassert(8279700, "Window should not already exist", isNew);

        auto replayCheckpointId =
            windowStartDoc[WindowOperatorStartRecord::kReplayCheckpointIdFieldName];
        if (!replayCheckpointId.missing()) {
            tassert(8289711,
                    "Expected replayCheckpointId field of type long",
                    replayCheckpointId.getType() == BSONType::NumberLong);
            window->replayCheckpointId = replayCheckpointId.getLong();
            _context->checkpointStorage->onWindowRestore(*window->replayCheckpointId);
        }

        // Restore all this window's state.
        auto nextRecord = _context->checkpointStorage->getNextRecord(reader.get());
        tassert(8279701, "Expected window record.", nextRecord);
        // Keep reading records until we see the windowEnd record.
        while (
            nextRecord->getField(WindowOperatorCheckpointRecord::kWindowEndFieldName).missing()) {
            // Ask the derived class to restore its window state for this record.
            doRestoreWindowState(window, std::move(*nextRecord));
            updateStats(window);
            nextRecord = _context->checkpointStorage->getNextRecord(reader.get());
            tassert(8279706, "Expected window record.", nextRecord);
        }
        auto windowEndRecord =
            nextRecord->getField(WindowOperatorCheckpointRecord::kWindowEndFieldName);
        tassert(
            8279707, "Expected window end record.", windowEndRecord.getType() == BSONType::Object);
        auto windowEndMarker = windowEndRecord.getDocument().getField(
            WindowOperatorEndRecord::kWindowEndMarkerFieldName);
        tassert(8279708,
                "Expected windowEndMarker field of type long.",
                windowEndMarker.getType() == BSONType::NumberLong);
        tassert(8279709,
                "Unexpected window end marker.",
                windowEndMarker.getLong() == startTime.getLong());
    }
}

void WindowAwareOperator::processWatermarkMsg(StreamControlMsg controlMsg) {
    tassert(8318505, "Expected a watermarkMsg", controlMsg.watermarkMsg);
    const auto& watermark = *controlMsg.watermarkMsg;
    const auto& options = getOptions();
    tassert(8347600, "Expected to be the window assigner", options.windowAssigner);
    const auto& assigner = *options.windowAssigner;

    bool isInputActive = watermark.watermarkStatus == WatermarkStatus::kActive;
    int64_t inputWatermarkTime{0};
    if (isInputActive) {
        _idleStartTime = boost::none;
        inputWatermarkTime =
            watermark.eventTimeWatermarkMs - options.windowAssigner->getAllowedLateness();
        if (inputWatermarkTime > _maxReceivedWatermarkMs) {
            _maxReceivedWatermarkMs = inputWatermarkTime;
        }
    } else {
        tassert(8318506,
                "Expected a watermarkStatus of kIdle",
                watermark.watermarkStatus == WatermarkStatus::kIdle);
        if (!assigner.hasIdleTimeout()) {
            // User has not set an idle timeout, so we don't do anything for idle messages.
            return;
        }

        auto now = Date_t::now().toMillisSinceEpoch();
        if (!_idleStartTime) {
            // Start the idle time counter and return.
            _idleStartTime = now;
        }

        auto duration = now - *_idleStartTime;
        if (!assigner.hasIdleTimeoutElapsed(duration)) {
            // The idle timeout hasn't occured, return.
            return;
        }

        if (_windows.empty()) {
            // There are no open windows so we do nothing with idleness, return.
            return;
        }

        // The idle timeout has occured.
        boost::optional<int64_t> maxWindowEndToClose;
        for (auto windowIt = _windows.rbegin(); windowIt != _windows.rend(); ++windowIt) {
            // We close all windows with a "remaining time" less than the idle duration.
            // "remaining time" is the window end minus the last observed event time watermark.
            auto windowEndTime = assigner.getWindowEndTime(windowIt->first);
            auto remainingTime = windowEndTime - _maxReceivedWatermarkMs;
            if (duration >= remainingTime) {
                // We want to close this window, and all windows before it.
                maxWindowEndToClose = windowEndTime;
                break;
            }
        }
        if (!maxWindowEndToClose) {
            // Idle duration is not large enough to close any open windows.
            return;
        }
        inputWatermarkTime = *maxWindowEndToClose;
    }

    auto windowIt = _windows.begin();
    while (windowIt != _windows.end()) {
        int64_t windowStartTime = windowIt->first;
        if (options.windowAssigner->shouldCloseWindow(windowStartTime, inputWatermarkTime)) {
            // Send the results for this window.
            closeWindow(windowIt->second.get());
            if (windowIt->second->replayCheckpointId) {
                tassert(9531600,
                        "Found replayCheckpointId, expected to be the window assigner",
                        getOptions().windowAssigner);
                _context->checkpointStorage->onWindowClose(*(windowIt->second->replayCheckpointId));
            }
            windowIt = _windows.erase(windowIt);
            if (options.sendWindowSignals) {
                // Send a windowCloseSignal message downstream.
                sendControlMsg(0,
                               StreamControlMsg{.windowCloseSignal = WindowCloseMsg{
                                                    .windowStartTime = windowStartTime}});
            }
        } else {
            // The windows map is ordered, so we can stop iterating now.
            break;
        }
    }

    // Here we advance the _minWindowStartTime based on the watermark and windows we've closed.
    // Any windows that start before the _minWindowStartTime are "already closed".
    // They will not be re-output.
    // Get the minimum allowed window start time from the max closed window time.
    auto minWindowStartTime = assigner.toOldestWindowStartTime(inputWatermarkTime);
    if (minWindowStartTime > _minWindowStartTime) {
        // Don't allow _minWindowStartTime to be decreased.
        // This prevents the scenario where the _minWindowStartTime is initialized
        // during checkpoint restore, and then the $source sends a watermark that would
        // decrease _minWindowStartTime.
        _minWindowStartTime = minWindowStartTime;
    }

    // The window's output watermark is 1 ms less than the minimum allowed window start time.
    int64_t outputWatermark = _minWindowStartTime - 1;
    if (outputWatermark > _maxSentWatermarkMs) {
        // Send the update output watermark from this window.
        sendControlMsg(
            0 /* outputIdx */,
            StreamControlMsg{WatermarkControlMsg{.watermarkStatus = WatermarkStatus::kActive,
                                                 .eventTimeWatermarkMs = outputWatermark}});
        _maxSentWatermarkMs = outputWatermark;
    }
}

void WindowAwareOperator::sendLateDocDlqMessage(const StreamDocument& doc,
                                                int64_t minEligibleStartTime) {
    if (_replayMinWindowStartTime && minEligibleStartTime < *_replayMinWindowStartTime) {
        // After a pipeline modify, we might replay docs that have already been in previously
        // closed windows. We don't want to DLQ those docs.
        return;
    }

    auto windowAssigner = getOptions().windowAssigner.get();
    tassert(8292300, "Expected to be the window assigner.", windowAssigner);

    std::vector<int64_t> missedWindows;
    int64_t windowStartTs = minEligibleStartTime;
    while (windowStartTs < _minWindowStartTime && windowStartTs <= doc.minEventTimestampMs) {
        missedWindows.push_back(windowStartTs);
        windowStartTs = windowAssigner->getNextWindowStartTime(windowStartTs);
    }
    if (missedWindows.size() > 0) {
        // create Dlq document
        auto bsonObjBuilder = toDeadLetterQueueMsg(_context->streamMetaFieldName,
                                                   doc,
                                                   getName(),
                                                   std::string{"Input document arrived late."});
        bsonObjBuilder.append(kMissedWindowsFieldName, missedWindows);

        // write Dlq message
        int64_t numDlqBytes = _context->dlq->addMessage(std::move(bsonObjBuilder));
        // update Dlq stats
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
    }
}

// Below method are for the $sessionWindow implementation.
void WindowAwareOperator::onDataMsgSessionWindow(int32_t inputIdx,
                                                 StreamDataMsg dataMsg,
                                                 boost::optional<StreamControlMsg> controlMsg) {
    if (getOptions().windowAssigner) {
        assignSessionWindowsAndProcessDataMsg(std::move(dataMsg));
    } else {
        invariant(!dataMsg.docs.empty());
        const auto& firstStreamWindow = dataMsg.docs.front().streamMeta.getWindow();
        invariant(firstStreamWindow);
        const auto& partition = *(firstStreamWindow->getPartition());
        auto minTS = *(firstStreamWindow->getStart());
        auto maxTS = *(firstStreamWindow->getEnd());
        processDocsInSessionWindow(partition,
                                   std::move(dataMsg.docs),
                                   minTS.toMillisSinceEpoch(),
                                   maxTS.toMillisSinceEpoch(),
                                   false);
    }
    if (controlMsg) {
        onControlMsg(/*inputIdx*/ 0, std::move(*controlMsg));
    }
}

void WindowAwareOperator::onControlMsgSessionWindow(int32_t inputIdx, StreamControlMsg controlMsg) {
    const auto& options = getOptions();

    if (controlMsg.checkpointMsg) {
        // TODO(SERVER-91882): Implement this
        MONGO_UNREACHABLE;
    } else if (controlMsg.watermarkMsg && options.windowAssigner) {
        // If this is the windowAssigner (the first stateful stage in the window's inner
        // pipeline), then use source watermark to find out what windows should be closed.
        // Close all the windows that should be closed by this watermark.
        processSessionWindowWatermarkMsg(std::move(controlMsg));
    } else if (controlMsg.watermarkMsg && !options.windowAssigner) {
        // Send the watermark msg along.
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.windowCloseSignal) {
        processSessionWindowCloseMsg(std::move(controlMsg));
    } else if (controlMsg.windowMergeSignal) {
        processSessionWindowMergeMsg(std::move(controlMsg));
    }
}

void WindowAwareOperator::assignSessionWindowsAndProcessDataMsg(StreamDataMsg dataMsg) {
    // TODO(SERVER-92473): Send late arriving date to the DLQ
    struct MiniWindow {
        std::vector<StreamDocument> docsInWindow;
        int64_t minTS = std::numeric_limits<int64_t>::max();
        int64_t maxTS = std::numeric_limits<int64_t>::min();
    };

    std::sort(
        dataMsg.docs.begin(), dataMsg.docs.end(), [](const auto& lhs, const auto& rhs) -> bool {
            return lhs.minEventTimestampMs < rhs.minEventTimestampMs;
        });

    const auto& options = getOptions();
    auto assigner = dynamic_cast<SessionWindowAssigner*>(options.windowAssigner.get());
    const auto& expr = assigner->getPartitionBy();
    mongo::ValueUnorderedMap<MiniWindow> miniWindows =
        mongo::ValueComparator::kInstance.makeUnorderedValueMap<MiniWindow>();
    for (auto& doc : dataMsg.docs) {
        // TODO(SERVER-92473): Handle exceptions here and send offending doc to DLQ.
        mongo::Value partition = expr->evaluate(doc.doc, &expr->getExpressionContext()->variables);
        auto partitionIt = miniWindows.find(partition);
        if (partitionIt == miniWindows.end()) {
            miniWindows[partition] = MiniWindow{};
        }
        auto& myMiniWindow = miniWindows[partition];

        if (myMiniWindow.docsInWindow.empty() ||
            assigner->shouldMergeSessionWindows(doc.minEventTimestampMs,
                                                doc.minEventTimestampMs,
                                                myMiniWindow.minTS,
                                                myMiniWindow.maxTS)) {
            myMiniWindow.minTS = std::min(myMiniWindow.minTS, doc.minEventTimestampMs);
            myMiniWindow.maxTS = std::max(myMiniWindow.maxTS, doc.minEventTimestampMs);
            myMiniWindow.docsInWindow.push_back(std::move(doc));
        } else {
            processDocsInSessionWindow(partition,
                                       std::move(myMiniWindow.docsInWindow),
                                       myMiniWindow.minTS,
                                       myMiniWindow.maxTS,
                                       _context->shouldProjectStreamMetaPriorToSinkStage());
            myMiniWindow = MiniWindow{
                std::vector<StreamDocument>{}, doc.minEventTimestampMs, doc.minEventTimestampMs};
            myMiniWindow.docsInWindow.push_back(std::move(doc));
        }
    }
    for (auto& pair : miniWindows) {
        invariant(!pair.second.docsInWindow.empty());
        processDocsInSessionWindow(std::move(pair.first),
                                   std::move(pair.second.docsInWindow),
                                   pair.second.minTS,
                                   pair.second.maxTS,
                                   _context->shouldProjectStreamMetaPriorToSinkStage());
    }
}

void WindowAwareOperator::processDocsInSessionWindow(mongo::Value const& partition,
                                                     std::vector<StreamDocument> streamDocs,
                                                     int64_t minTS,
                                                     int64_t maxTS,
                                                     bool projectStreamMeta) {
    invariant(!streamDocs.empty());
    const auto& firstStreamMeta = streamDocs.front().streamMeta;
    auto window = addOrGetSessionWindow(
        partition,
        minTS,
        maxTS,
        firstStreamMeta.getWindow() ? firstStreamMeta.getWindow()->getWindowID() : boost::none,
        firstStreamMeta.getSource() ? firstStreamMeta.getSource()->getType() : boost::none);
    invariant(window);  // cannot be a null ptr

    if (!window->status.isOK()) {
        return;
    }

    if (projectStreamMeta) {
        for (auto& streamDoc : streamDocs) {
            auto newStreamMeta = updateStreamMeta(
                streamDoc.doc.getField(*_context->streamMetaFieldName), window->streamMetaTemplate);
            MutableDocument mutableDoc(std::move(streamDoc.doc));
            mutableDoc.setField(*_context->streamMetaFieldName, Value(std::move(newStreamMeta)));
            streamDoc.doc = mutableDoc.freeze();
        }
    }

    try {
        doProcessDocs(window, std::move(streamDocs));
    } catch (const DBException& e) {
        window->status = e.toStatus();
    }
}

WindowAwareOperator::Window* WindowAwareOperator::mergeSessionWindows(
    boost::container::small_vector<std::unique_ptr<Window>, 1>& partitionWindows,
    int64_t newStartTimestampMs,
    int64_t newEndTimestampMs,
    boost::optional<mongo::StreamMetaSourceTypeEnum> sourceType) {
    const auto& options = getOptions();
    invariant(options.windowAssigner);

    if (partitionWindows.empty()) {
        // If no open windows for this session, nothing to merge.
        return nullptr;
    }

    std::vector<int64_t> mergedWindows;

    Window* destinationWindow{nullptr};

    auto it = partitionWindows.begin();
    while (it != partitionWindows.end()) {
        auto window = it->get();
        auto currWindowID = window->getWindowID();
        int64_t start = window->getStartMs();
        int64_t end = window->getEndMs();

        if (dynamic_cast<SessionWindowAssigner*>(options.windowAssigner.get())
                ->shouldMergeSessionWindows(start, end, newStartTimestampMs, newEndTimestampMs)) {
            // If this window overlaps within gap distance of the input interval, update window
            // bounds.
            newStartTimestampMs = std::min(start, newStartTimestampMs);
            newEndTimestampMs = std::max(end, newEndTimestampMs);

            mergedWindows.push_back(currWindowID);
            if (!destinationWindow) {
                // Take the first window that can merge as the "destination" window.
                destinationWindow = window;
                ++it;
                continue;
            }

            destinationWindow->merge(window);
            updateStats(destinationWindow);

            // find and delete the window being merged into the destination window from auxillary
            // data structures.
            auto staleIt =
                std::find(_sessionWindowsVector.begin(), _sessionWindowsVector.end(), window);
            invariant(staleIt != _sessionWindowsVector.end());
            _sessionWindowsVector.erase(staleIt);
            it = partitionWindows.erase(it);
        } else {
            ++it;
        }
    }

    if (!destinationWindow) {
        // No windows were merged.
        return nullptr;
    }

    if (options.sendWindowSignals && mergedWindows.size() > 1) {
        // At least 1 window was merged with this interval, so send a merge signal downstream.
        sendControlMsg(0,
                       StreamControlMsg{.windowMergeSignal =
                                            SessionWindowMergeMsg{destinationWindow->getPartition(),
                                                                  newStartTimestampMs,
                                                                  newEndTimestampMs,
                                                                  std::move(mergedWindows)}});
    }

    // Update the min/max timestamp of the destination window.
    destinationWindow->setStartMs(newStartTimestampMs);
    destinationWindow->setEndMs(newEndTimestampMs);
    return destinationWindow;
}

WindowAwareOperator::Window* WindowAwareOperator::addOrGetSessionWindow(
    mongo::Value const& partition,
    int64_t minTS,
    int64_t maxTS,
    boost::optional<int32_t> windowID,
    boost::optional<mongo::StreamMetaSourceTypeEnum> sourceType) {

    const auto& options = getOptions();
    auto& partitionWindows = _sessionWindows[partition];  // we want the partition to exist anyways

    if (options.windowAssigner) {
        invariant(!windowID);
        auto destinationWindow = mergeSessionWindows(partitionWindows, minTS, maxTS, sourceType);

        if (destinationWindow) {
            return destinationWindow;
        }

        windowID = _nextSessionWindowId;
        ++_nextSessionWindowId;
    } else {
        invariant(windowID);
        auto windowIt = std::find_if(partitionWindows.begin(),
                                     partitionWindows.end(),
                                     [&](const std::unique_ptr<Window>& window) {
                                         return window->getWindowID() == *windowID;
                                     });
        if (windowIt != partitionWindows.end()) {
            return windowIt->get();
        }
    }

    // The window does not already exist, so create a new one.
    invariant(windowID);

    StreamMetaSource streamMetaSource;
    streamMetaSource.setType(sourceType);

    StreamMetaWindow streamMetaWindow;
    streamMetaWindow.setPartition(partition);
    streamMetaWindow.setWindowID(*windowID);
    streamMetaWindow.setStart(Date_t::fromMillisSinceEpoch(minTS));
    streamMetaWindow.setEnd(Date_t::fromMillisSinceEpoch(maxTS));

    StreamMeta streamMetaTemplate;
    streamMetaTemplate.setSource(std::move(streamMetaSource));
    streamMetaTemplate.setWindow(std::move(streamMetaWindow));

    auto window = makeWindow(std::move(streamMetaTemplate));

    if (options.windowAssigner) {
        // Store new window in auxillary data structure used for closing windows.
        _sessionWindowsVector.push_back(window.get());
    }

    auto ret = window.get();
    partitionWindows.push_back(std::move(window));
    return ret;
}

void WindowAwareOperator::processSessionWindowWatermarkMsg(StreamControlMsg controlMsg) {
    tassert(9188000, "Expected a watermarkMsg", controlMsg.watermarkMsg);
    const auto& watermark = *controlMsg.watermarkMsg;
    const auto& options = getOptions();
    tassert(9188001, "Expected to be the window assignger", options.windowAssigner);

    bool isInputActive = watermark.watermarkStatus == WatermarkStatus::kActive;
    int64_t inputWatermarkTime{0};
    if (isInputActive) {
        inputWatermarkTime =
            watermark.eventTimeWatermarkMs - options.windowAssigner->getAllowedLateness();
        if (inputWatermarkTime > _maxReceivedWatermarkMs) {
            _maxReceivedWatermarkMs = inputWatermarkTime;
        }
    } else {
        tassert(9188002,
                "Expected a watermarkStatus of kIdle",
                watermark.watermarkStatus == WatermarkStatus::kIdle);
        return;
    }

    auto assigner = dynamic_cast<SessionWindowAssigner*>(options.windowAssigner.get());
    auto sendWindowSignals = options.sendWindowSignals;

    auto newEnd = std::remove_if(
        _sessionWindowsVector.begin(),
        _sessionWindowsVector.end(),
        [this, inputWatermarkTime, sendWindowSignals, &assigner](Window* window) {
            if (assigner->shouldCloseWindow(window->getEndMs(), inputWatermarkTime)) {
                // close window
                closeWindow(window);

                auto windowID = window->getWindowID();
                const auto& partition = window->getPartition();

                if (sendWindowSignals) {
                    sendControlMsg(
                        0,
                        StreamControlMsg{
                            .windowCloseSignal = WindowCloseMsg{
                                partition, window->getStartMs(), window->getEndMs(), windowID}});
                }

                // Remove window from open session window map.
                auto partitionWindowsIt = _sessionWindows.find(partition);
                invariant(partitionWindowsIt != _sessionWindows.end());
                auto& partitionWindows = partitionWindowsIt->second;
                auto windowIt = std::find_if(partitionWindows.begin(),
                                             partitionWindows.end(),
                                             [&](const std::unique_ptr<Window>& window) {
                                                 return window->getWindowID() == windowID;
                                             });
                invariant(windowIt != partitionWindows.end());
                partitionWindows.erase(windowIt);
                if (partitionWindows.empty()) {
                    _sessionWindows.erase(partitionWindowsIt);
                }
                return true;
            }
            return false;
        });

    // Remove closed windows from auxillary data structure.
    _sessionWindowsVector.erase(newEnd, _sessionWindowsVector.end());
}

void WindowAwareOperator::processSessionWindowCloseMsg(StreamControlMsg controlMsg) {
    const auto& options = getOptions();
    invariant(!options.windowAssigner, "Unexpected windowCloseSignal when windowAssigner is set.");

    auto partitionWindowsIt = _sessionWindows.find(controlMsg.windowCloseSignal->partition);
    if (partitionWindowsIt == _sessionWindows.end()) {
        // The target window does not exist in this operator.
        return;
    }
    auto& partitionWindows = partitionWindowsIt->second;

    auto windowIt =
        std::find_if(partitionWindows.begin(),
                     partitionWindows.end(),
                     [&controlMsg](const std::unique_ptr<Window>& window) {
                         return window->getWindowID() == controlMsg.windowCloseSignal->windowId;
                     });

    if (windowIt == partitionWindows.end()) {
        // The target window does not exist in this operator.
        /* This can happen when there is a downstream match operator that filters out all of the
        documents in a window, so operators downstream of the match never get a dataMsg with that
        windowId and never create a corresponding window in their open windows map. */
        return;
    }

    windowIt->get()->setStartMs(controlMsg.windowCloseSignal->windowStartTime);
    windowIt->get()->setEndMs(controlMsg.windowCloseSignal->windowEndTime);
    closeWindow(windowIt->get());
    partitionWindows.erase(windowIt);

    if (partitionWindows.empty()) {
        _sessionWindows.erase(partitionWindowsIt);
    }

    if (options.sendWindowSignals) {
        sendControlMsg(0, std::move(controlMsg));
    }
}

void WindowAwareOperator::processSessionWindowMergeMsg(StreamControlMsg controlMsg) {
    const auto& options = getOptions();
    invariant(!options.windowAssigner, "Unexpected windowMergeSignal when windowAssigner is set.");

    const auto& partition = controlMsg.windowMergeSignal->partition;
    auto minTS = controlMsg.windowMergeSignal->minTimestampMs;
    auto maxTS = controlMsg.windowMergeSignal->maxTimestampMs;
    auto windowsToMerge = controlMsg.windowMergeSignal->windowsToMerge;

    invariant(windowsToMerge.size() >= 1);
    auto destinationWindowId = windowsToMerge[0];

    Window* destinationWindow{nullptr};

    auto partitionWindowsIt = _sessionWindows.find(partition);
    if (partitionWindowsIt == _sessionWindows.end()) {
        // The entire partition has been filtered out, so nothing to merge.
        /*
        Example: the pipeline below uses eager computation, so windows will flow through
        operators without being closed.
        {["$sessionWindow"]:
        {
            pipeline: [
                {
                    $match: {
                        b: 2
                    },
                    $group: {
                        _id: null,
                        allIds: {$push: "$id"}
                    }
                }
            ]
            gap: {size: NumberInt(1), unit: "hour"},
            partitionBy: "$a"
        }}
        DataMsg 1 docs: [{a: 1, id: 1, b: 2, ts: 0}]
        DataMsg 2 docs: [{a: 1, id: 2, b: 1, ts: 0}]

        Both docs have the same timestamp and the same partition, so their session windows
        should merge. However, eager computation will filter out the first doc's window in
        the match stage, so the group operator won't store anything for partition a = 1.
        */
        return;
    }

    auto& partitionWindows = partitionWindowsIt->second;

    for (auto windowId : windowsToMerge) {
        auto windowIt = std::find_if(partitionWindows.begin(),
                                     partitionWindows.end(),
                                     [windowId](const std::unique_ptr<Window>& window) {
                                         return window->getWindowID() == windowId;
                                     });

        // This window got filtered out.
        if (windowIt == partitionWindows.end()) {
            continue;
        }

        if (!destinationWindow) {
            // Use this window as the destination window.
            destinationWindow = windowIt->get();
            // Set it's windowID the upstream assigner specified.
            destinationWindow->streamMetaTemplate.getWindow()->setWindowID(destinationWindowId);
            destinationWindow->setStartMs(minTS);
            destinationWindow->setEndMs(maxTS);
        } else {
            // Merge this window into the destination window.
            auto victimWindow = windowIt->get();
            destinationWindow->merge(victimWindow);
            updateStats(destinationWindow);
            partitionWindowsIt->second.erase(windowIt);
        }
    }

    if (options.sendWindowSignals) {
        sendControlMsg(0, std::move(controlMsg));
    }
}

}  // namespace streams
