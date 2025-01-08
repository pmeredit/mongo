/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/delayed_watermark_generator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

DelayedWatermarkGenerator::DelayedWatermarkGenerator(
    int32_t inputIdx,
    WatermarkCombiner* combiner,
    boost::optional<WatermarkControlMsg> initialWatermark)
    : WatermarkGenerator(inputIdx, initialWatermark, combiner) {
    if (initialWatermark) {
        // -1 indicates that the watermark timestamp has not be set yet.
        invariant(initialWatermark->watermarkTimestampMs >= -1);
        // The reverse of the logic in doOnEvent (_maxEventTimestampMs - _allowedLatenessMs - 1).
        _maxEventTimestampMs = initialWatermark->watermarkTimestampMs + 1;
    }
}

void DelayedWatermarkGenerator::doOnEvent(int64_t eventTimestampMs) {
    dassert(_watermarkMsg.watermarkStatus == WatermarkStatus::kActive);

    _maxEventTimestampMs = std::max(_maxEventTimestampMs, eventTimestampMs);
    _watermarkMsg.watermarkTimestampMs = _maxEventTimestampMs - 1;
    if (_watermarkMsg.watermarkTimestampMs < -1) {
        _watermarkMsg.watermarkTimestampMs = -1;
    }
}

void DelayedWatermarkGenerator::doSetIdle() {
    _watermarkMsg.watermarkStatus = WatermarkStatus::kIdle;
}

void DelayedWatermarkGenerator::doSetActive() {
    _watermarkMsg.watermarkStatus = WatermarkStatus::kActive;
}

}  // namespace streams
