/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/stages_gen.h"
#include "streams/exec/util.h"

namespace streams {

/**
 * WindowAssigner is a stateless class used by WindowAwareOperator to assign timestamps to event
 * time windows.
 */
class WindowAssigner {
public:
    struct Options {
        int size{-1};
        mongo::StreamTimeUnitEnum sizeUnit{mongo::StreamTimeUnitEnum::Millisecond};
        int slide{-1};
        mongo::StreamTimeUnitEnum slideUnit{mongo::StreamTimeUnitEnum::Millisecond};
        int offsetFromUtc{0};
        mongo::StreamTimeUnitEnum offsetUnit{mongo::StreamTimeUnitEnum::Millisecond};
        int64_t allowedLatenessMs{0};
        boost::optional<int64_t> idleTimeoutMs;
        mongo::WindowBoundaryEnum boundary{mongo::WindowBoundaryEnum::eventTime};
    };

    WindowAssigner(Options options)
        : _options(std::move(options)),
          _windowSizeMs(toMillis(_options.sizeUnit, _options.size)),
          _windowSlideMs(toMillis(_options.slideUnit, _options.slide)),
          _windowOffsetMs(calculateOffsetMs(_options.offsetUnit, _options.offsetFromUtc)) {}

    virtual ~WindowAssigner() = default;

    // Returns the start time of the oldest window that this docTime is in.
    int64_t toOldestWindowStartTime(int64_t docTime) const;

    // Returns true if a window should be closed at this watermark time.
    virtual bool shouldCloseWindow(int64_t windowStartTime,
                                   int64_t inputWatermarkMinusLateness) const;

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

    // Returns true if the idle timeout is set.
    bool hasIdleTimeout() const {
        return bool(_options.idleTimeoutMs);
    }

    // Returns true if the idle timeout has elapsed.
    bool hasIdleTimeoutElapsed(int64_t idleDurationMs) const {
        tassert(8347601, "Expected idleTimeout to be set", _options.idleTimeoutMs);
        return idleDurationMs >= *_options.idleTimeoutMs;
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
