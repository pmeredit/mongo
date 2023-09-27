/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/window_operator.h"

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
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
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
    dassert(_options.size > 0);
    dassert(_options.slide > 0);
    dassert(_windowSizeMs > 0);
    dassert(_windowSlideMs > 0);

    Parser::Options parserOptions;
    parserOptions.planMainPipeline = false;
    _parser = std::make_unique<Parser>(_context, std::move(parserOptions));
    auto operatorDag = _parser->fromBson(_options.pipeline);
    _innerPipelineTemplate =
        Pipeline::create(std::move(*operatorDag).movePipeline(), _context->expCtx);

    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    _numOpenWindowsGauge = _context->metricManager->registerGauge(
        "num_open_windows", "Number of windows that are currently open", std::move(labels));
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
    auto pipeline = _innerPipelineTemplate->clone();
    auto operators = _parser->fromPipeline(*pipeline, /* minOperatorId */ _operatorId + 1);

    // Add a CollectOperator at the end of the operator chain to collect the documents
    // emitted at the end of the pipeline.
    auto collectOperator = std::make_unique<CollectOperator>(_context, /*numInputs*/ 1);
    // TODO(SERVER-78481): We may need to increment by getNumInnerOperators here when $facet is
    OperatorId collectOperatorId =
        getOperatorId() + _innerPipelineTemplate->getSources().size() + 1;
    collectOperator->setOperatorId(collectOperatorId);
    invariant(collectOperator->getNumInnerOperators() == 0);
    if (!operators.empty()) {
        operators.back()->addOutput(collectOperator.get(), 0);
    }
    operators.push_back(std::move(collectOperator));

    WindowPipeline::Options options;
    options.startMs = start;
    options.endMs = end;
    options.pipeline = std::move(pipeline->getSources());
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
    for (auto& doc : dataMsg.docs) {
        auto docTime = doc.minEventTimestampMs;
        if (docTime < _minWindowStartTime) {
            // Ignore any documents before the _minimumWindowStartTime.
            continue;
        }
        // Create and/or look up windows from the oldest window until we exceed 'docTime'.
        for (auto start = toOldestWindowStartTime(docTime); start <= docTime;
             start += _windowSlideMs) {
            if (start < _minWindowStartTime) {
                // Ignore any windows before the _minimumWindowStartTime.
                continue;
            }

            auto end = start + _windowSizeMs;
            dassert(windowContains(start, end, docTime));
            auto window = _openWindows.find(start);
            if (window == _openWindows.end()) {
                window = addWindow(start, end);
            }
            auto& openWindow = window->second;
            try {
                // TODO: Avoid copying the doc.
                StreamDataMsg dataMsg{{doc}};
                openWindow.pipeline.process(std::move(dataMsg));
            } catch (const DBException& e) {
                openWindow.pipeline.setError(str::stream()
                                             << "Failed to process input document in " << getName()
                                             << " with error: " << e.what());
            }
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

int32_t WindowOperator::getNumInnerOperators() const {
    // The size of the inner pipeline, plus 1 for the CollectOperator.
    return _innerPipelineTemplate->getSources().size() + 1;
}

void WindowOperator::initFromCheckpoint() {
    invariant(_context->restoreCheckpointId);
    auto bson = _context->checkpointStorage->readState(
        *_context->restoreCheckpointId, _operatorId, 0 /* chunkNumber */);
    CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId, _operatorId, "expected state", bson);
    auto state = WindowOperatorStateFastMode::parseOwned(IDLParserContext{"WindowOperator"},
                                                         std::move(*bson));
    _minWindowStartTime = state.getMinimumWindowStartTime();
    _maxSentCheckpointId = *_context->restoreCheckpointId;

    LOGV2_INFO(74700,
               "WindowOperator restored",
               "minWindowStartTime"_attr =
                   Date_t::fromMillisSinceEpoch(_minWindowStartTime).toString(),
               "context"_attr = _context,
               "checkpointId"_attr = *_context->restoreCheckpointId);
}

bool WindowOperator::processWatermarkMsg(StreamControlMsg controlMsg) {
    bool closedWindows = false;
    // TODO(SERVER-76722): If we want to use an unordered_map for the container, we need
    // to add some extra logic here to close windows in order. We can choose a starting
    // point in time and iterate using options.slide, like in doOnDataMessage. The starting
    // point in time here could be min(EarliestOpenWindowStart, watermarkTime aligned to its
    // closest End boundary).
    auto watermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs;
    for (auto it = _openWindows.begin(); it != _openWindows.end();) {
        auto& windowPipeline = it->second.pipeline;

        if (shouldCloseWindow(windowPipeline.getEnd(), watermarkTime)) {
            std::queue<StreamDataMsg> results;
            try {
                results = windowPipeline.close();
            } catch (const DBException& e) {
                windowPipeline.setError(str::stream()
                                        << "Failed to process an input document for this window in "
                                        << getName() << " with error: " << e.what());
            }

            if (windowPipeline.getError()) {
                _context->dlq->addMessage(windowPipeline.getDeadLetterQueueMsg());
            } else {
                while (!results.empty()) {
                    // The sink needs to flush these documents before checkpointIds
                    // sent afterwards are committed.
                    sendDataMsg(0, std::move(results.front()));
                    results.pop();
                }
            }
            _openWindows.erase(it++);
            closedWindows = true;
        } else {
            break;
        }
    }

    // Update _minWindowStartTime time based on the watermark.
    // After processing this watermark, we've closed all windows with an
    // endTime <= watermarkTime.
    // So the _minWindowStartTime is the oldest possible window
    // where endTime > watermarkTime (because any window before has been closed).
    // This is the same thing as the oldest window that contains the watermarkTime,
    // i.e. windowStartTime <= watermarkTime < windowEndTime
    // So we can re-use the toOldestWindowStartTime function, which finds this window
    // for a timestamp.
    auto minWindowStartTimeAfterThisWatermark = toOldestWindowStartTime(watermarkTime);
    if (minWindowStartTimeAfterThisWatermark > _minWindowStartTime) {
        // Don't allow _minWindowStartTime to be decreased.
        // This prevents the scenario where the _minWindowStartTime is initialized
        // during checkpoint restore, and then the $source sends a watermark that would
        // decrease _minWindowStartTime.
        _minWindowStartTime = minWindowStartTimeAfterThisWatermark;
    }

    sendControlMsg(/* outputIdx */ 0, std::move(controlMsg));
    return closedWindows;
}

void WindowOperator::sendCheckpointMsg(CheckpointId maxCheckpointIdToSend) {
    invariant(!_unsentCheckpointIds.empty() &&
              maxCheckpointIdToSend <= _unsentCheckpointIds.back());
    // Send all the checkpoint IDs up through checkpointIdToSend.
    while (!_unsentCheckpointIds.empty() && _unsentCheckpointIds.front() <= maxCheckpointIdToSend) {
        CheckpointId checkpointId = _unsentCheckpointIds.front();
        _unsentCheckpointIds.pop_front();
        _context->checkpointStorage->addState(
            checkpointId,
            _operatorId,
            WindowOperatorStateFastMode{_minWindowStartTime}.toBSON(),
            0 /* chunkNumber */);
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
    int64_t memoryUsage{0};
    for (const auto& [_, window] : _openWindows) {
        memoryUsage += window.pipeline.getStats().memoryUsageBytes;
    }

    _stats.memoryUsageBytes = memoryUsage;
    return _stats;
}

bool WindowOperator::isCheckpointingEnabled() {
    // If checkpointStorage is not nullptr, checkpointing is enabled.
    return bool(_context->checkpointStorage);
}

}  // namespace streams
