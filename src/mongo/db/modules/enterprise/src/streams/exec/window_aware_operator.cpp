/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/window_aware_operator.h"
#include "streams/exec/exec_internal_gen.h"

namespace streams {

using namespace mongo;

void WindowAwareOperator::doOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    if (_options.windowAssigner) {
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

    if (controlMsg.checkpointMsg) {
        // TODO(SERVER-82797): Implement checkpoint save and restore.
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.watermarkMsg && _options.windowAssigner) {
        int64_t inputWatermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs;
        // If this is the windowAssigner (the first stateful stage in the window's inner
        // pipeline), then use source watermark to find out what windows should be closed.
        // Close all the windows that should be closed by this watermark.
        auto windowIt = _windows.begin();
        while (windowIt != _windows.end()) {
            int64_t windowStartTime = windowIt->first;
            if (_options.windowAssigner->shouldCloseWindow(windowStartTime, inputWatermarkTime)) {
                // Send the results for this window.
                closeWindow(windowIt->second.get());
                windowIt = _windows.erase(windowIt);
                if (_options.sendWindowCloseSignal) {
                    // Send a windowCloseSignal message downstream.
                    sendControlMsg(0, StreamControlMsg{.windowCloseSignal = windowStartTime});
                }
            } else {
                // The windows map is ordered, so we can stop iterating now.
                break;
            }
        }

        // Compute the output watermark.
        int64_t minWindowTime =
            _windows.empty() ? std::numeric_limits<int64_t>::max() : _windows.begin()->first;
        int64_t outputWatermarkTime = std::min(minWindowTime, inputWatermarkTime);
        if (outputWatermarkTime > _maxSentWatermarkMs) {
            sendControlMsg(0,
                           StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                                .eventTimeWatermarkMs = outputWatermarkTime}});
            _maxSentWatermarkMs = outputWatermarkTime;
        }
    } else if (controlMsg.watermarkMsg && !_options.windowAssigner) {
        // Send the watermark msg along.
        sendControlMsg(0, std::move(controlMsg));
    } else if (controlMsg.windowCloseSignal) {
        invariant(!_options.windowAssigner,
                  "Unexpected windowCloseSignal when windowAssigner is set.");
        auto windowIt = _windows.find(*controlMsg.windowCloseSignal);
        if (windowIt != _windows.end()) {
            invariant(windowIt == _windows.begin(),
                      "Expected the minimum window to be closed first.");
            closeWindow(windowIt->second.get());
            _windows.erase(windowIt);
        }
        if (_options.sendWindowCloseSignal) {
            sendControlMsg(0, std::move(controlMsg));
        }
    }
}

// Assigns the docs in the input to windows and processes each.
void WindowAwareOperator::assignWindowsAndProcessDataMsg(StreamDataMsg dataMsg) {
    invariant(_options.windowAssigner);
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
        _options.windowAssigner->toOldestWindowStartTime(dataMsg.docs.front().minEventTimestampMs);
    while (nextWindowStartTs <= endTs) {
        int64_t windowStart = nextWindowStartTs;
        int64_t windowEnd = _options.windowAssigner->getWindowEndTime(windowStart);
        nextWindowStartTs = _options.windowAssigner->getNextWindowStartTime(nextWindowStartTs);

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
                int64_t oldestWindowTs = _options.windowAssigner->toOldestWindowStartTime(docTs);
                nextWindowStartTs = std::max(nextWindowStartTs, oldestWindowTs);
                nextWindowStartDocIdx = i;
                nextWindowStartTsSet = true;
            }

            if (_options.windowAssigner->windowContains(windowStart, windowEnd, docTs)) {
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
        doProcessDocs(window, streamDocs);
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

}  // namespace streams
