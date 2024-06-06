/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/window_assigner.h"

namespace streams {

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
int64_t WindowAssigner::toOldestWindowStartTime(int64_t docTime) const {
    auto windowStartTimeMs = docTime - _windowSizeMs + _windowSlideMs;
    auto remainderMs = (docTime - _windowOffsetMs) % _windowSlideMs;
    windowStartTimeMs -= remainderMs;
    return std::max(windowStartTimeMs, int64_t{0});
}

bool WindowAssigner::shouldCloseWindow(int64_t windowStartTime,
                                       int64_t inputWatermarkMinusLateness) const {
    int64_t windowEnd = getWindowEndTime(windowStartTime);
    return inputWatermarkMinusLateness >= windowEnd;
}

int64_t WindowAssigner::calculateOffsetMs(mongo::StreamTimeUnitEnum offsetUnit,
                                          int offsetFromUtc) const {
    return offsetFromUtc >= 0 ? toMillis(offsetUnit, offsetFromUtc)
                              : -toMillis(offsetUnit, -offsetFromUtc);
}

int64_t WindowAssigner::getWindowEndTime(int64_t windowStartTime) const {
    return windowStartTime + _windowSizeMs;
}

bool WindowAssigner::windowContains(int64_t start, int64_t end, int64_t timestamp) const {
    return timestamp >= start && timestamp < end;
}

int64_t WindowAssigner::getNextWindowStartTime(int64_t startTime) const {
    return startTime + _windowSlideMs;
}

}  // namespace streams
