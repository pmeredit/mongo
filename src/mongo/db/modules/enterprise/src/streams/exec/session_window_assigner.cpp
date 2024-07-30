/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/session_window_assigner.h"

namespace streams {

bool SessionWindowAssigner::shouldCloseWindow(int64_t windowEndTime,
                                              int64_t inputWatermarkMinusLateness) const {
    return inputWatermarkMinusLateness >= windowEndTime + _windowGapSizeMs;
}

bool SessionWindowAssigner::shouldMergeSessionWindows(int64_t start1,
                                                      int64_t end1,
                                                      int64_t start2,
                                                      int64_t end2) const {

    auto window1 = std::make_pair(start1, end1);
    auto window2 = std::make_pair(start2, end2);
    if (window1.first > window2.first) {
        std::swap(window1, window2);
    }
    auto gap = window2.first - window1.second;
    return gap < _windowGapSizeMs;
}

}  // namespace streams
