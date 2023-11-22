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
        // Validate all the docs in the dataMsg have the same windowStartTimestamp and
        // windowEndTimestamp
        auto start = dataMsg.docs.front().streamMeta.getWindowStartTimestamp();
        auto end = dataMsg.docs.front().streamMeta.getWindowEndTimestamp();
        invariant(start);
        invariant(end);
        for (auto& doc : dataMsg.docs) {
            invariant(doc.streamMeta.getWindowStartTimestamp() == start);
            invariant(doc.streamMeta.getWindowEndTimestamp() == end);
        }
        // Process all the docs in the data message.
        processDocsInWindow(
            start->toMillisSinceEpoch(), end->toMillisSinceEpoch(), std::move(dataMsg.docs));
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
        int64_t inputWatermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs -
            options.windowAssigner->getAllowedLateness();
        // If this is the windowAssigner (the first stateful stage in the window's inner
        // pipeline), then use source watermark to find out what windows should be closed.
        // Close all the windows that should be closed by this watermark.
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

        auto minWindowStartTime =
            getOptions().windowAssigner->toOldestWindowStartTime(inputWatermarkTime);
        if (minWindowStartTime > _minWindowStartTime) {
            // Don't allow _minWindowStartTime to be decreased.
            // This prevents the scenario where the _minWindowStartTime is initialized
            // during checkpoint restore, and then the $source sends a watermark that would
            // decrease _minWindowStartTime.
            _minWindowStartTime = minWindowStartTime;
        }

        int64_t outputWatermark = _minWindowStartTime - 1;
        if (outputWatermark > _maxSentWatermarkMs) {
            // Send the update output watermark from this window.
            sendControlMsg(
                0 /* outputIdx */,
                StreamControlMsg{WatermarkControlMsg{.watermarkStatus = WatermarkStatus::kActive,
                                                     .eventTimeWatermarkMs = outputWatermark}});
            _maxSentWatermarkMs = outputWatermark;
        }
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

        processDocsInWindow(windowStart, windowEnd, std::move(docsInThisWindow));
    }
}

OperatorStats WindowAwareOperator::doGetStats() {
    // Add together all the memory usage and DLQ-ed docs of all the open windows.
    int64_t memoryUsageBytes{0};
    for (auto& [startTime, window] : _windows) {
        updateStats(window.get());
        memoryUsageBytes += window->stats.memoryUsageBytes;
    }
    _stats.memoryUsageBytes = memoryUsageBytes;
    return _stats;
}

// Process documents for a particular window.
void WindowAwareOperator::processDocsInWindow(int64_t windowStartTime,
                                              int64_t windowEndTime,
                                              std::vector<StreamDocument> streamDocs) {
    invariant(!streamDocs.empty());
    auto window = addOrGetWindow(
        windowStartTime, windowEndTime, streamDocs.front().streamMeta.getSourceType());
    if (!window->status.isOK()) {
        return;
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
        streamMetaTemplate.setWindowStartTimestamp(Date_t::fromMillisSinceEpoch(windowStartTime));
        streamMetaTemplate.setWindowEndTimestamp(Date_t::fromMillisSinceEpoch(windowEndTime));
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
        _context->dlq->addMessage(streams::toDeadLetterQueueMsg(
            std::move(window->streamMetaTemplate), std::move(window->status.reason())));
        incOperatorStats({.numDlqDocs = 1});
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
    for (auto& [windowStartTime, window] : _windows) {
        // Write the window start record.
        WindowOperatorStartRecord windowStart(
            windowStartTime,
            window->streamMetaTemplate.getWindowEndTimestamp()->toMillisSinceEpoch());
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
    while (auto record = _context->checkpointStorage->getNextRecord(reader.get())) {
        // Parse and add the new window.
        auto data = WindowOperatorCheckpointRecord::parse(parserContext, record->toBson());
        auto windowStartDoc = data.getWindowStart();
        tassert(8279705, "Expected a window start record.", windowStartDoc);
        int64_t startTime = windowStartDoc->getStartTimeMs();
        tassert(8279700, "Window should not already exist", !_windows.contains(startTime));
        auto window = addOrGetWindow(windowStartDoc->getStartTimeMs(),
                                     windowStartDoc->getEndTimeMs(),
                                     windowStartDoc->getSourceType());

        // Restore all this window's state.
        auto nextRecord = _context->checkpointStorage->getNextRecord(reader.get());
        tassert(8279701, "Expected window record.", nextRecord);
        // Keep reading records until we see the windowEnd record.
        auto windowRecord =
            WindowOperatorCheckpointRecord::parse(parserContext, nextRecord->toBson());
        while (!windowRecord.getWindowEnd()) {
            // Ask the derived class to restore its window state for this record.
            doRestoreWindowState(window, std::move(*nextRecord));
            nextRecord = _context->checkpointStorage->getNextRecord(reader.get());
            tassert(8279706, "Expected window record.", nextRecord);
            windowRecord =
                WindowOperatorCheckpointRecord::parse(parserContext, nextRecord->toBson());
        }
        tassert(8279707,
                "Unexpected window start time in the window end record.",
                windowRecord.getWindowEnd()->getWindowEndMarker() == startTime);
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
        _context->dlq->addMessage(bsonObjBuilder.obj());
        // update Dlq stats
        incOperatorStats({.numDlqDocs = 1});
    }
}

}  // namespace streams
