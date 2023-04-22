/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/delayed_watermark_generator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

DelayedWatermarkGenerator::DelayedWatermarkGenerator(int32_t inputIdx,
                                                     WatermarkCombiner* combiner,
                                                     int64_t allowedLatenessMs)
    : WatermarkGenerator(inputIdx, combiner), _allowedLatenessMs(allowedLatenessMs) {}

void DelayedWatermarkGenerator::doOnEvent(int64_t eventTimestampMs) {
    dassert(_watermarkMsg.watermarkStatus == WatermarkStatus::kActive);

    _maxEventTimestampMs = std::max(_maxEventTimestampMs, eventTimestampMs);
    _watermarkMsg.eventTimeWatermarkMs = _maxEventTimestampMs - _allowedLatenessMs - 1;
    if (_watermarkMsg.eventTimeWatermarkMs < 0) {
        _watermarkMsg.eventTimeWatermarkMs = 0;
    }
}

void DelayedWatermarkGenerator::doSetIdle() {
    _watermarkMsg.watermarkStatus = WatermarkStatus::kIdle;
}

void DelayedWatermarkGenerator::doSetActive() {
    _watermarkMsg.watermarkStatus = WatermarkStatus::kActive;
}

}  // namespace streams
