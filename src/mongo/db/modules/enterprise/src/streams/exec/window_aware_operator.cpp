/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/window_aware_operator.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/idl/idl_parser.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/exec_internal_gen.h"

namespace streams {

using namespace mongo;

void WindowAwareOperator::doStart() {
    if (_context->restoreCheckpointId) {
        restoreState(*_context->restoreCheckpointId);
    }
}

void WindowAwareOperator::doOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    if (getOptions().windowAssigner) {
        assignWindowsAndProcessDataMsg(std::move(dataMsg));
    } else {
        invariant(!dataMsg.docs.empty());
        // Validate all the docs in the dataMsg have the same windowStart and
        // windowEnd
        auto start = dataMsg.docs.front().streamMeta.getWindowStart();
        auto end = dataMsg.docs.front().streamMeta.getWindowEnd();
        invariant(start);
        invariant(end);
        for (auto& doc : dataMsg.docs) {
            invariant(doc.streamMeta.getWindowStart() == start);
            invariant(doc.streamMeta.getWindowEnd() == end);
        }
        // Process all the docs in the data message.
        processDocsInWindow(start->toMillisSinceEpoch(),
                            end->toMillisSinceEpoch(),
                            std::move(dataMsg.docs),
                            false /* projectStreamMeta */);
    }

    if (controlMsg) {
        onControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void WindowAwareOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    invariant(!controlMsg.eofSignal, "Unexpected eofSignal control message.");
    invariant(!(bool(controlMsg.checkpointMsg) && bool(controlMsg.windowCloseSignal)),
              "checkpointMsg and windowCloseSingal should not both be set.");
    invariant(!(bool(controlMsg.watermarkMsg) && bool(controlMsg.windowCloseSignal)),
              "watermarkMsg and windowCloseSingal should not both be set.");
    const auto& options = getOptions();

    if (controlMsg.checkpointMsg) {
        saveState(controlMsg.checkpointMsg->id);
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.watermarkMsg && options.windowAssigner) {
        // If this is the windowAssigner (the first stateful stage in the window's inner
        // pipeline), then use source watermark to find out what windows should be closed.
        // Close all the windows that should be closed by this watermark.
        processWatermarkMsg(std::move(controlMsg));
    } else if (controlMsg.watermarkMsg && !options.windowAssigner) {
        // Send the watermark msg along.
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.windowCloseSignal) {
        invariant(!options.windowAssigner,
                  "Unexpected windowCloseSignal when windowAssigner is set.");
        auto windowIt = _windows.find(*controlMsg.windowCloseSignal);
        if (windowIt != _windows.end()) {
            invariant(windowIt == _windows.begin(),
                      "Expected the minimum window to be closed first.");
            closeWindow(windowIt->second.get());
            _windows.erase(windowIt);
        }
        if (options.sendWindowCloseSignal) {
            sendControlMsg(0, std::move(controlMsg));
        }
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
    if (nextWindowStartTs < _minWindowStartTime) {
        // If the min window start time is after the min timestamp in this document batch, then
        // skip all documents with a timestamp before the min window start time.
        nextWindowStartTs = _minWindowStartTime;
        while (nextWindowStartDocIdx < (int64_t)dataMsg.docs.size()) {
            const auto& doc = dataMsg.docs[nextWindowStartDocIdx];
            int64_t docTime = doc.minEventTimestampMs;
            if (docTime < nextWindowStartTs) {
                sendLateDocDlqMessage(doc,
                                      options.windowAssigner->toOldestWindowStartTime(docTime));
                ++nextWindowStartDocIdx;
            } else {
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
    for (auto& [startTime, window] : _windows) {
        memoryUsageBytes += window->stats.memoryUsageBytes;
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
    auto window = addOrGetWindow(
        windowStartTime, windowEndTime, streamDocs.front().streamMeta.getSourceType());
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
        // If an exception escapes the doProcessDocs, we need to DLQ the whole window.
        window->status = e.toStatus();
    }
}

// Add a new window or get an existing window.
WindowAwareOperator::Window* WindowAwareOperator::addOrGetWindow(
    int64_t windowStartTime,
    int64_t windowEndTime,
    boost::optional<StreamMetaSourceTypeEnum> sourceType) {
    if (!_windows.contains(windowStartTime)) {
        // Add a new window.
        StreamMeta streamMetaTemplate;
        streamMetaTemplate.setSourceType(sourceType);
        streamMetaTemplate.setWindowStart(Date_t::fromMillisSinceEpoch(windowStartTime));
        streamMetaTemplate.setWindowEnd(Date_t::fromMillisSinceEpoch(windowEndTime));
        auto window = makeWindow(std::move(streamMetaTemplate));
        auto result = _windows.emplace(windowStartTime, std::move(window));
        invariant(result.second);
        return result.first->second.get();
    }

    return _windows[windowStartTime].get();
}

// Called when a window is closed. Sends the window output to the next operator.
void WindowAwareOperator::closeWindow(Window* window) {
    if (!window->status.isOK()) {
        // If the window has error-ed, send a message to the DLQ.
        // Note: we might want to send this DLQ message earlier, instead
        // of waiting for the window to close.
        auto numDlqBytes =
            _context->dlq->addMessage(toDeadLetterQueueMsg(_context->streamMetaFieldName,
                                                           std::move(window->streamMetaTemplate),
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
    minWindowStartTimeRecord.setMinWindowStartTime(_minWindowStartTime);
    _context->checkpointStorage->appendRecord(writer.get(),
                                              Document{minWindowStartTimeRecord.toBSON()});

    // Write the state of all the open windows.
    for (auto& [windowStartTime, window] : _windows) {
        // Write the window start record.
        WindowOperatorStartRecord windowStart(
            windowStartTime, window->streamMetaTemplate.getWindowEnd()->toMillisSinceEpoch());
        if (window->streamMetaTemplate.getSourceType()) {
            windowStart.setSourceType(window->streamMetaTemplate.getSourceType());
        }
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
    auto reader = _context->checkpointStorage->createStateReader(checkpointId, _operatorId);
    IDLParserContext parserContext("WindowAwareOperatorCheckpointRestore");

    // Restore the minumum window start time.
    boost::optional<Document> record = _context->checkpointStorage->getNextRecord(reader.get());
    tassert(8318300, "Expected record", record);
    auto minWindowStartTime =
        record->getField(WindowOperatorCheckpointRecord::kMinWindowStartTimeFieldName);
    tassert(8318301,
            "Expected minWindowStartTime record",
            minWindowStartTime.getType() == BSONType::NumberLong);
    _minWindowStartTime = minWindowStartTime.getLong();

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

        tassert(
            8279700, "Window should not already exist", !_windows.contains(startTime.getLong()));
        auto window = addOrGetWindow(startTime.getLong(), endTime.getLong(), sourceTypeEnum);

        // Restore all this window's state.
        auto nextRecord = _context->checkpointStorage->getNextRecord(reader.get());
        tassert(8279701, "Expected window record.", nextRecord);
        // Keep reading records until we see the windowEnd record.
        while (
            nextRecord->getField(WindowOperatorCheckpointRecord::kWindowEndFieldName).missing()) {
            // Ask the derived class to restore its window state for this record.
            doRestoreWindowState(window, std::move(*nextRecord));
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
        updateStats(window);
    }
}

void WindowAwareOperator::processWatermarkMsg(StreamControlMsg controlMsg) {
    tassert(8318505, "Expected a watermarkMsg", controlMsg.watermarkMsg);
    const auto& watermark = *controlMsg.watermarkMsg;
    const auto& options = getOptions();
    tassert(8347600, "Expected to be the window assignger", options.windowAssigner);
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
            windowIt = _windows.erase(windowIt);
            if (options.sendWindowCloseSignal) {
                // Send a windowCloseSignal message downstream.
                sendControlMsg(0, StreamControlMsg{.windowCloseSignal = windowStartTime});
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
        BSONObjBuilder bsonObjBuilder;
        bsonObjBuilder.append("doc", doc.doc.toBson());
        bsonObjBuilder.append("error", "Input document arrived late.");
        bsonObjBuilder.append("missedWindowStartTimes", missedWindows);

        // write Dlq message
        int64_t numDlqBytes = _context->dlq->addMessage(bsonObjBuilder.obj());
        // update Dlq stats
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
    }
}

}  // namespace streams
