/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/latency_collector.h"
#include "mongo/logv2/log.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

mongo::BSONObj toBSON(const LatencyCollector::MaxLatencyDeltas& delta) {
    return BSON("maxReadDelta" << delta.maxReadDelta.count() << "maxReadDeltaSourceTime"
                               << delta.maxReadDeltaSourceTime.count() << "maxWriteDelta"
                               << delta.maxWriteDelta.count() << "maxWriteDeltaSourceTime"
                               << delta.maxWriteDeltaSourceTime.count() << "maxCommitDelta"
                               << delta.maxCommitDelta.count() << "maxCommitDeltaSourceTime"
                               << delta.maxCommitDeltaSourceTime.count() << "maxOverallDelta"
                               << delta.maxOverallDelta.count() << "maxOverallDeltaSourceTime"
                               << delta.maxOverallDeltaSourceTime.count());
}

LatencyCollector::~LatencyCollector() {
    LOGV2_INFO(9961301,
               "Maximum latency deltas",
               "context"_attr = _loggingContext,
               "maxDeltas"_attr = _max);
}

void LatencyCollector::add(LatencyInfo info) {
    auto diff = LatencyDeltas::fromLatencyInfo(info);

    if (diff.readDelta > _max.maxReadDelta) {
        _max.maxReadDelta = diff.readDelta;
        _max.maxReadDeltaSourceTime = info.sourceTime;
        _needsLog = true;
    }
    if (diff.writeDelta > _max.maxWriteDelta) {
        _max.maxWriteDelta = diff.writeDelta;
        _max.maxWriteDeltaSourceTime = info.sourceTime;
        _needsLog = true;
    }
    if (diff.commitDelta > _max.maxCommitDelta) {
        _max.maxCommitDelta = diff.commitDelta;
        _max.maxCommitDeltaSourceTime = info.sourceTime;
        _needsLog = true;
    }
    if (diff.overallDelta > _max.maxOverallDelta) {
        _max.maxOverallDelta = diff.overallDelta;
        _max.maxOverallDeltaSourceTime = info.sourceTime;
        _needsLog = true;
    }

    // TODO(SERVER-100112): Emit metrics to victoriametrics as well.
    if (_needsLog) {
        auto delay = _logRateLimiter.consume(1);
        if (delay == Microseconds(0)) {
            _needsLog = false;
            LOGV2_INFO(9961300,
                       "Maximum latency deltas",
                       "context"_attr = _loggingContext,
                       "maxDeltas"_attr = _max);
        }
    }
}

}  // namespace streams
