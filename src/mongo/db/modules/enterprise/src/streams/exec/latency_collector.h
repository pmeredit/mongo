/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/util/time_support.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/rate_limiter.h"

namespace streams {

struct Context;

// LatencyCollector collects and emits end to end latency information for a stream processor.
// TODO(SERVER-100112): Integrate with this class in $merge and timeseries $emit sinks.
class LatencyCollector {
public:
    // Millisecond timestamps of various interesting points in an StreamDocument's lifetime.
    struct LatencyInfo {
        // Wall time of the event according to the source. For example, wallTime in changestream
        // events.
        mongo::Milliseconds sourceTime{0};
        // Time the processor read the event.
        mongo::Milliseconds readTime{0};
        // Time the processor started writing the event.
        mongo::Milliseconds writeTime{0};
        // Time the event was fully committed to the sink.
        mongo::Milliseconds commitTime{0};

        static LatencyInfo makeBeforeWrite(const StreamDocument& doc) {
            return LatencyInfo{.sourceTime = Milliseconds{doc.sourceTimestampMs},
                               .readTime = Milliseconds{doc.minProcessingTimeMs},
                               .writeTime = Milliseconds{static_cast<int64_t>(curTimeMillis64())}};
        }
    };

    struct LatencyDeltas {
        // readTime - sourceTime.
        mongo::Milliseconds readDelta{0};
        // writeTime - readTime.
        mongo::Milliseconds writeDelta{0};
        // commitTime - writeTime.
        mongo::Milliseconds commitDelta{0};
        // commitTime - sourceTime.
        mongo::Milliseconds overallDelta{0};

        static LatencyDeltas fromLatencyInfo(const LatencyInfo& info) {
            return LatencyDeltas{.readDelta = info.readTime - info.sourceTime,
                                 .writeDelta = info.writeTime - info.readTime,
                                 .commitDelta = info.commitTime - info.writeTime,
                                 .overallDelta = info.commitTime - info.sourceTime};
        }
    };

    // Used to track maximum latency deltas and the sourceTime associated with each.
    struct MaxLatencyDeltas {
        Milliseconds maxReadDelta{-1};
        Milliseconds maxReadDeltaSourceTime{-1};

        Milliseconds maxWriteDelta{-1};
        Milliseconds maxWriteDeltaSourceTime{-1};

        Milliseconds maxCommitDelta{-1};
        Milliseconds maxCommitDeltaSourceTime{-1};

        Milliseconds maxOverallDelta{-1};
        Milliseconds maxOverallDeltaSourceTime{-1};

        bool operator==(const MaxLatencyDeltas& other) const = default;
    };

    LatencyCollector(LoggingContext loggingContext) : _loggingContext(std::move(loggingContext)) {}

    ~LatencyCollector();

    // Add a latency measurement.
    void add(LatencyInfo info);

    // Get the max latency observed.
    MaxLatencyDeltas getMax() const {
        return _max;
    }

private:
    friend class LatencyCollectorTest;

    LoggingContext _loggingContext;
    // timer used for log rate limiting
    Timer _timer{};
    // No more than 1 log every 30 seconds
    RateLimiter _logRateLimiter{1.0 / 30.0, 1, &_timer};
    // True when the max has been updated and we want to log to splunk.
    bool _needsLog{false};
    // Max latency deltas observed so far.
    MaxLatencyDeltas _max;
};

};  // namespace streams
