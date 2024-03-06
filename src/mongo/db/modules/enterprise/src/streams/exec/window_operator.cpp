/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/window_operator.h"

#include <boost/none.hpp>
#include <chrono>
#include <exception>
#include <limits>

#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/collect_operator.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/executor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/util.h"
#include "streams/exec/window_operator.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

namespace {
int64_t calculateOffsetMs(StreamTimeUnitEnum offsetUnit, int offsetFromUtc) {
    return offsetFromUtc >= 0 ? toMillis(offsetUnit, offsetFromUtc)
                              : -toMillis(offsetUnit, -offsetFromUtc);
}
}  // namespace

WindowOperator::WindowOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _windowSizeMs(toMillis(options.sizeUnit, options.size)),
      _windowSlideMs(toMillis(options.slideUnit, options.slide)),
      _windowOffsetMs(calculateOffsetMs(options.offsetUnit, options.offsetFromUtc)) {
    if (_options.idleTimeoutUnit) {
        tassert(8318500,
                "Expected idleTimeoutSize to be set if idleTimeoutUnit is set.",
                _options.idleTimeoutSize);
        _idleTimeoutMs = toMillis(*_options.idleTimeoutUnit, *_options.idleTimeoutSize);
    }
    dassert(_options.size > 0);
    dassert(_options.slide > 0);
    dassert(_windowSizeMs > 0);
    dassert(_windowSlideMs > 0);
}

void WindowOperator::doStart() {
    // Checkpointing is enabled if checkpointStorage is set.
    if (_context->restoreCheckpointId) {
        initFromCheckpoint();
    }
}

bool WindowOperator::windowContains(int64_t start, int64_t end, int64_t timestamp) {
    return timestamp >= start && timestamp < end;
}

bool WindowOperator::shouldCloseWindow(int64_t windowEnd, int64_t watermarkTime) {
    return watermarkTime >= windowEnd;
}

std::map<int64_t, WindowOperator::OpenWindow>::iterator WindowOperator::addWindow(int64_t start,
                                                                                  int64_t end) {
    Planner::Options plannerOptions;
    plannerOptions.planMainPipeline = false;
    if (_options.minMaxOperatorIds) {
        plannerOptions.minOperatorId = _options.minMaxOperatorIds->first;
    }
    auto planner = std::make_unique<Planner>(_context, std::move(plannerOptions));
    auto operatorDag = planner->plan(_options.pipeline);
    auto pipeline = operatorDag->movePipeline();
    auto operators = operatorDag->moveOperators();
    if (_options.minMaxOperatorIds) {
        invariant(operators.back()->getOperatorId() == _options.minMaxOperatorIds->second);
    }

    WindowPipeline::Options options;
    options.startMs = start;
    options.endMs = end;
    options.pipeline = std::move(pipeline);
    options.operators = std::move(operators);
    WindowPipeline windowPipeline(_context, std::move(options));
    // The priorCheckpointId is the max received checkpointId (the last element in
    // _unsentCheckpointIds), or, it's the _maxSentCheckpointId, which is set during restore.
    CheckpointId priorCheckpointId = _maxSentCheckpointId;
    if (!_unsentCheckpointIds.empty()) {
        priorCheckpointId = _unsentCheckpointIds.back();
    }
    auto result =
        _openWindows.emplace(start, OpenWindow{std::move(windowPipeline), priorCheckpointId});
    return std::move(result.first);
}

void WindowOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    // Unset the idle start time.
    _idleStartTime = boost::none;

    // Sort documents by timestamp and then partition the input data message by window start
    // timestamp that the documents belong to so that we only send a single batch for each
    // unique window start timestamp.
    std::sort(
        dataMsg.docs.begin(), dataMsg.docs.end(), [](const auto& lhs, const auto& rhs) -> bool {
            return lhs.minEventTimestampMs < rhs.minEventTimestampMs;
        });

    int64_t nextWindowStartDocIdx{0};
    int64_t endTs = dataMsg.docs.back().minEventTimestampMs;
    int64_t nextWindowStartTs = toOldestWindowStartTime(dataMsg.docs.front().minEventTimestampMs);
    if (nextWindowStartTs < _minWindowStartTime) {
        // If the min window start time is after the min timestamp in this document batch, then
        // skip all documents with a timestamp before the min window start time.
        nextWindowStartTs = _minWindowStartTime;
        while (nextWindowStartDocIdx < (int64_t)dataMsg.docs.size()) {
            if (dataMsg.docs[nextWindowStartDocIdx].minEventTimestampMs < nextWindowStartTs) {
                ++nextWindowStartDocIdx;
            } else {
                break;
            }
        }
    }

    while (nextWindowStartTs <= endTs) {
        int64_t windowStart = nextWindowStartTs;
        int64_t windowEnd = windowStart + _windowSizeMs;
        nextWindowStartTs += _windowSlideMs;

        StreamDataMsg dataMsgPartition;
        bool nextWindowStartTsSet{false};
        for (size_t i = nextWindowStartDocIdx; i < dataMsg.docs.size(); ++i) {
            auto& doc = dataMsg.docs[i];
            int64_t docTs = doc.minEventTimestampMs;
            dassert(docTs >= windowStart);
            if (!nextWindowStartTsSet && docTs >= nextWindowStartTs) {
                // Fast forward to the next window if the next window is after the initially
                // expected next window (current window + slide).
                int64_t oldestWindowTs = toOldestWindowStartTime(docTs);
                nextWindowStartTs = std::max(nextWindowStartTs, oldestWindowTs);
                nextWindowStartDocIdx = i;
                nextWindowStartTsSet = true;
            }

            if (windowContains(windowStart, windowEnd, docTs)) {
                dataMsgPartition.docs.push_back(doc);
            } else {
                // Done collecting documents for the current window.
                break;
            }
        }

        dassert(!dataMsgPartition.docs.empty());
        auto it = _openWindows.find(windowStart);
        if (it == _openWindows.end()) {
            it = addWindow(windowStart, windowEnd);
        }

        auto& pipeline = it->second.pipeline;
        pipeline.process(std::move(dataMsgPartition));
        // Flush any output data msgs that are immediately available in the case
        // where this window pipeline doesn't have any blocking operators.
        auto dataMsgs = pipeline.getNextOutputDataMsgs();
        while (!dataMsgs.empty()) {
            sendDataMsg(/*outputIdx*/ 0, std::move(dataMsgs.front()));
            dataMsgs.pop();
        }
    }
    if (controlMsg) {
        doOnControlMsg(inputIdx, *controlMsg);
    }
    _numOpenWindowsGauge->set(_openWindows.size());
}

/**
 * Like flink, we align our tumbling window boundaries to the epoch.
 * Suppose the pipeline has a 1 hour window and we see our first event at
 * 11:05:32.000 on the first day.
 * For this event we will open a window for: [11:00:00.000, 12:00:00.000).
 * Here are a few examples for tumbling windows.
 *
 * Support docTime is 555 and the window size is 100 (slide is also 100).
 * 555 - 100 + 100 - (555 % 100) = 555 - (555 % 100) = 500
 *
 * Support docTime is 999 and the window size is 25 (slide is also 25).
 * 999 - 25 + 25 - (999 % 25) = 999 - (999 % 25) = 999 - 24 = 975
 *
 * For tumblingWindows, _windowSize == _windowSlide.
 * Thus, docTime - _windowSize + _windowSlide - (docTime % _windowSlide)
 * reduces to:
 * docTime - _windowSize + _windowSize - (docTime % _windowSize)
 * docTime - (docTime % _windowSize)
 *
 * The windowStartTime should be adjusted by _windowOffsetMs for tumblingWindows.
 *
 * For example, a tumblingWindow with _windowSize = _windowSlide = 100, _windowOffsetMs = -10,
 * possible windows are (0, 100-10), (100-10, 200-10), (200-10, 300-10), ...
 *
 * And for docTime = 501,
 * the windowStartTime should be 501 - 100 + 100 - ((501 + 10) % 100) = 501 - 11 = 490.
 * For docTime = 589,
 * the windowStartTime should be 589 - 100 + 100 - ((589 + 10) % 100) = 589 - 99 = 490.
 */
int64_t WindowOperator::toOldestWindowStartTime(int64_t docTime) {
    auto windowStartTimeMs = docTime - _windowSizeMs + _windowSlideMs;
    auto remainderMs = (docTime - _windowOffsetMs) % _windowSlideMs;
    windowStartTimeMs -= remainderMs;
    return std::max(windowStartTimeMs, int64_t{0});
}


void WindowOperator::registerMetrics(MetricManager* metricManager) {
    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    _numOpenWindowsGauge = metricManager->registerGauge(
        "num_open_windows", "Number of windows that are currently open", std::move(labels));
}

void WindowOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    invariant(bool(controlMsg.watermarkMsg) != bool(controlMsg.checkpointMsg));
    bool closedWindows{false};
    if (controlMsg.watermarkMsg) {
        closedWindows = processWatermarkMsg(std::move(controlMsg));
    } else if (controlMsg.checkpointMsg) {
        invariant(isCheckpointingEnabled());
        invariant(_unsentCheckpointIds.empty() ||
                  controlMsg.checkpointMsg->id > _unsentCheckpointIds.back());
        // After this, any new windows opened will have this as their "prior checkpoint".
        _unsentCheckpointIds.push_back(controlMsg.checkpointMsg->id);
    }

    if (isCheckpointingEnabled()) {
        boost::optional<CheckpointId> checkpointIdToSend;
        if (_openWindows.empty()) {
            if (!_unsentCheckpointIds.empty()) {
                // If there are no open windows, we can send along the max unsent checkpoint.
                checkpointIdToSend = _unsentCheckpointIds.back();
            }
        } else if (closedWindows) {
            // Find the minimum prior checkpointId, which may have changed because
            // we closed a window.
            // The minimum prior checkpointId is far enough back in the $source to replay
            // all open windows. So it's safe to sent and commit.
            boost::optional<CheckpointId> minPriorCheckpointId;
            for (const auto& [startTime, openWindow] : _openWindows) {
                invariant(openWindow.priorCheckpointId > 0);
                if (!minPriorCheckpointId || openWindow.priorCheckpointId < *minPriorCheckpointId) {
                    minPriorCheckpointId = openWindow.priorCheckpointId;
                }
            }
            checkpointIdToSend = minPriorCheckpointId;
        }

        // We check to checkpointIdToSend > _mostRecentSentCheckpoint to avoid sending
        // the same checkpointId twice.
        if (checkpointIdToSend && *checkpointIdToSend > _maxSentCheckpointId) {
            sendCheckpointMsg(*checkpointIdToSend);
        }
    }
}

void WindowOperator::initFromCheckpoint() {
    invariant(_context->restoreCheckpointId);
    boost::optional<BSONObj> bson;
    if (_context->oldCheckpointStorage) {
        bson = _context->oldCheckpointStorage->readState(
            *_context->restoreCheckpointId, _operatorId, 0 /* chunkNumber */);
    } else {
        invariant(_context->checkpointStorage);
        auto reader = _context->checkpointStorage->createStateReader(*_context->restoreCheckpointId,
                                                                     _operatorId);
        bson = _context->checkpointStorage->getNextRecord(reader.get());
    }
    CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId, _operatorId, "expected state", bson);
    auto state = WindowOperatorStateFastMode::parseOwned(IDLParserContext{"WindowOperator"},
                                                         std::move(*bson));
    _minWindowStartTime = state.getMinimumWindowStartTime();
    invariant(_minWindowStartTime % _windowSlideMs == 0);
    _maxSentCheckpointId = *_context->restoreCheckpointId;

    LOGV2_INFO(74700,
               "WindowOperator restored",
               "minWindowStartTime"_attr =
                   Date_t::fromMillisSinceEpoch(_minWindowStartTime).toString(),
               "context"_attr = _context,
               "checkpointId"_attr = *_context->restoreCheckpointId);
}

boost::optional<int64_t> WindowOperator::getEndTimeToClose(
    const WatermarkControlMsg& watermarkMsg) {
    if (watermarkMsg.watermarkStatus == WatermarkStatus::kActive) {
        // Unset the idle start time.
        _idleStartTime = boost::none;
        return watermarkMsg.eventTimeWatermarkMs;
    } else {
        tassert(8318501,
                "Expected a watermarkStatus of kIdle",
                watermarkMsg.watermarkStatus == WatermarkStatus::kIdle);
        if (!_idleTimeoutMs) {
            // User has not set an idle timeout, so we don't do anything for idle messages.
            return boost::none;
        }

        auto now = Date_t::now().toMillisSinceEpoch();
        if (!_idleStartTime) {
            // Start the idle time counter.
            _idleStartTime = now;
            return boost::none;
        }

        bool timeoutElapsed = now >= *_idleStartTime + *_idleTimeoutMs;
        if (!timeoutElapsed) {
            return boost::none;
        }

        // The idle timeout has occured.
        // Find the maximum open window that should be closed by the wall time, if any.
        auto window = _openWindows.rbegin();
        while (window != _openWindows.rend()) {
            int64_t endTime = window->first + _windowSizeMs;
            if (endTime <= now) {
                // Close windows with this endTime or less.
                return endTime;
            }
            ++window;
        }

        // We didn't find a window that should be closed, so we do nothing with this
        // idle timeout.
        return boost::none;
    }
}

bool WindowOperator::processWatermarkMsg(StreamControlMsg controlMsg) {
    tassert(8318502, "Expected a watermarkMsg", controlMsg.watermarkMsg);

    bool closedWindows = false;
    auto closeTime = getEndTimeToClose(*controlMsg.watermarkMsg);
    if (!closeTime) {
        return false;
    }

    // TODO(SERVER-76722): If we want to use an unordered_map for the container, we need
    // to add some extra logic here to close windows in order. We can choose a starting
    // point in time and iterate using options.slide, like in doOnDataMessage. The starting
    // point in time here could be min(EarliestOpenWindowStart, watermarkTime aligned to its
    // closest End boundary).
    for (auto it = _openWindows.begin(); it != _openWindows.end();) {
        auto& windowPipeline = it->second.pipeline;

        if (shouldCloseWindow(windowPipeline.getEnd(), *closeTime)) {
            while (!windowPipeline.isEof() && !windowPipeline.getError()) {
                auto dataMsgs = windowPipeline.getNextOutputDataMsgs(/*eof*/ true);
                while (!dataMsgs.empty()) {
                    sendDataMsg(/*outputIdx*/ 0, std::move(dataMsgs.front()));
                    dataMsgs.pop();
                }
            }

            if (windowPipeline.getError()) {
                _context->dlq->addMessage(windowPipeline.getDeadLetterQueueMsg());
                incOperatorStats({.numDlqDocs = 1});
            }

            auto stats = windowPipeline.close();
            incOperatorStats({.numDlqDocs = stats.numDlqDocs});
            _openWindows.erase(it++);
            closedWindows = true;
        } else {
            break;
        }
    }

    // Update _minWindowStartTime time based on the watermark.
    // After processing this watermark, we've closed all windows with an endTime <= closeTime.
    // So the _minWindowStartTime is the oldest possible window
    // where endTime > closeTime (because any window before has been closed).
    // This is the same thing as the oldest window that contains the closeTime,
    // i.e. windowStartTime <= closeTime < windowEndTime
    // So we can re-use the toOldestWindowStartTime function, which finds this window
    // for a timestamp.
    auto minWindowStartTime = toOldestWindowStartTime(*closeTime);
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

    return closedWindows;
}

void WindowOperator::sendCheckpointMsg(CheckpointId maxCheckpointIdToSend) {
    invariant(!_unsentCheckpointIds.empty() &&
              maxCheckpointIdToSend <= _unsentCheckpointIds.back());
    // Send all the checkpoint IDs up through checkpointIdToSend.
    while (!_unsentCheckpointIds.empty() && _unsentCheckpointIds.front() <= maxCheckpointIdToSend) {
        CheckpointId checkpointId = _unsentCheckpointIds.front();
        _unsentCheckpointIds.pop_front();

        if (_context->oldCheckpointStorage) {
            _context->oldCheckpointStorage->addState(
                checkpointId,
                _operatorId,
                WindowOperatorStateFastMode{_minWindowStartTime}.toBSON(),
                0 /* chunkNumber */);
        } else {
            invariant(_context->checkpointStorage);
            auto writer = _context->checkpointStorage->createStateWriter(
                *_context->restoreCheckpointId, _operatorId);
            _context->checkpointStorage->appendRecord(
                writer.get(), WindowOperatorStateFastMode{_minWindowStartTime}.toBSON());
        }

        sendControlMsg(0, StreamControlMsg{.checkpointMsg = CheckpointControlMsg{checkpointId}});
        LOGV2_INFO(74701,
                   "WindowOperator sent checkpoint message",
                   "minWindowStartTime"_attr =
                       Date_t::fromMillisSinceEpoch(_minWindowStartTime).toString(),
                   "context"_attr = _context,
                   "checkpointId"_attr = checkpointId);
        _maxSentCheckpointId = checkpointId;
    }
}

OperatorStats WindowOperator::doGetStats() {
    // Relevant closed windows stats are absorbed into _stats when a window closes

    OperatorStats stats{_stats};
    stats.memoryUsageBytes = 0;
    for (const auto& [_, window] : _openWindows) {
        const auto& wStats = window.pipeline.getStats();
        stats.numDlqDocs += wStats.numDlqDocs;
        stats.memoryUsageBytes += wStats.memoryUsageBytes;
    }

    _stats.memoryUsageBytes = stats.memoryUsageBytes;
    return stats;
}

bool WindowOperator::isCheckpointingEnabled() {
    // If checkpointStorage is not nullptr, checkpointing is enabled.
    return bool(_context->oldCheckpointStorage);
}

}  // namespace streams
