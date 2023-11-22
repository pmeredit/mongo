#pragma once

#include "streams/exec/util.h"

namespace streams {

/**
 * WindowAssigner is a stateless class used by WindowAwareOperator to assign timestamps to event
 * time windows.
 */
class WindowAssigner {
public:
    struct Options {
        int size;
        mongo::StreamTimeUnitEnum sizeUnit;
        int slide;
        mongo::StreamTimeUnitEnum slideUnit;
        int offsetFromUtc{0};
        mongo::StreamTimeUnitEnum offsetUnit{mongo::StreamTimeUnitEnum::Millisecond};
        int64_t allowedLatenessMs{0};
    };

    WindowAssigner(Options options)
        : _options(std::move(options)),
          _windowSizeMs(toMillis(options.sizeUnit, options.size)),
          _windowSlideMs(toMillis(options.slideUnit, options.slide)),
          _windowOffsetMs(calculateOffsetMs(options.offsetUnit, options.offsetFromUtc)) {}

    // Returns the start time of the oldest window that this docTime is in.
    int64_t toOldestWindowStartTime(int64_t docTime) const;

    // Returns true if a window should be closed at this watermark time.
    bool shouldCloseWindow(int64_t windowStartTime, int64_t inputWatermarkMinusLateness) const;

    // Gets the window end time from a window start time.
    int64_t getWindowEndTime(int64_t windowStartTime) const;

    // Returns true if the window contains the timestamp.
    bool windowContains(int64_t startTime, int64_t endtime, int64_t timestamp) const;

    // Returns the window slide in millis.
    int64_t getNextWindowStartTime(int64_t startTime) const;

    // Returns the allowed lateness in millis.
    int64_t getAllowedLateness() {
        return _options.allowedLatenessMs;
    }

private:
    // Calculates the offset from the user supplied spec.
    int64_t calculateOffsetMs(mongo::StreamTimeUnitEnum offsetUnit, int offsetFromUtc) const;

    const Options _options;
    const int64_t _windowSizeMs;
    const int64_t _windowSlideMs;
    const int64_t _windowOffsetMs;
};

}  // namespace streams
