/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>

#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/message.h"
#include "streams/exec/time_util.h"
#include "streams/exec/window_operator.h"

using namespace mongo;

namespace streams {

WindowOperator::WindowOperator(Options options)
    : Operator(1, 1),
      _options(options),
      _windowSizeMs(toMillis(options.sizeUnit, options.size)),
      _windowSlideMs(toMillis(options.slideUnit, options.slide)) {
    dassert(_options.size > 0);
    dassert(_options.slide > 0);
    dassert(_windowSizeMs > 0);
    dassert(_windowSlideMs > 0);
    // Only tumbling windows currently supported.
    dassert(isTumblingWindow());
    _innerPipelineTemplate = Pipeline::parse(_options.pipeline, _options.expCtx);
    _innerPipelineTemplate->optimizePipeline();
}

bool WindowOperator::windowContains(int64_t start, int64_t end, int64_t timestamp) {
    return timestamp >= start && timestamp < end;
}

bool WindowOperator::shouldCloseWindow(int64_t windowEnd, int64_t watermarkTime) {
    return watermarkTime >= windowEnd;
}

std::map<int64_t, WindowPipeline>::iterator WindowOperator::addWindow(int64_t start, int64_t end) {
    auto pipeline = _innerPipelineTemplate->clone();
    WindowPipeline windowPipeline(start, end, std::move(pipeline), _options.expCtx);
    auto result = _openWindows.emplace(std::make_pair(start, std::move(windowPipeline)));
    dassert(result.second);
    return std::move(result.first);
}

void WindowOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    // TODO(STREAMS-220)-PrivatePreview: Modify this implementation for $hoppingWindow.
    for (auto& doc : dataMsg.docs) {
        auto docTime = doc.minEventTimestampMs;
        auto start = toOldestWindowStartTime(docTime);
        auto end = start + _windowSizeMs;
        dassert(windowContains(start, end, docTime));
        auto window = _openWindows.find(start);
        if (window == _openWindows.end()) {
            window = addWindow(start, end);
        }
        window->second.process(std::move(doc));
    }

    if (controlMsg) {
        doOnControlMsg(inputIdx, *controlMsg);
    }
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
 */
int64_t WindowOperator::toOldestWindowStartTime(int64_t docTime) {
    return docTime - _windowSizeMs + _windowSlideMs - (docTime % _windowSlideMs);
}

void WindowOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.watermarkMsg) {
        // TODO(SERVER-75956): If we want to use an unordered_map for the container, we need
        // to add some extra logic here to close windows in order. We can choose a starting point in
        // time and iterate using options.slide, like in doOnDataMessage.
        // The starting point in time here could be
        // min(EarliestOpenWindowStart, watermarkTime aligned to its closest End boundary).
        auto watermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs;
        for (auto it = _openWindows.begin(); it != _openWindows.end();) {
            if (shouldCloseWindow(it->second.getEnd(), watermarkTime)) {
                auto results = it->second.close();
                while (!results.empty()) {
                    sendDataMsg(0, std::move(results.front()));
                    results.pop();
                }
                _openWindows.erase(it++);
            } else {
                break;
            }
        }
    }

    sendControlMsg(0, std::move(controlMsg));
}

}  // namespace streams
