/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "streams/exec/watermark_generator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/watermark_combiner.h"

namespace streams {

using namespace mongo;

WatermarkGenerator::WatermarkGenerator(int32_t inputIdx, WatermarkCombiner* combiner)
    : _inputIdx(inputIdx), _combiner(combiner) {}

void WatermarkGenerator::onEvent(int64_t eventTimeMs) {
    doOnEvent(eventTimeMs);
    if (_combiner) {
        _combiner->onWatermarkMsg(_inputIdx, _watermarkMsg);
    }
}

void WatermarkGenerator::setIdle() {
    doSetIdle();
    if (_combiner) {
        _combiner->onWatermarkMsg(_inputIdx, _watermarkMsg);
    }
}

void WatermarkGenerator::setActive() {
    doSetActive();
    if (_combiner) {
        _combiner->onWatermarkMsg(_inputIdx, _watermarkMsg);
    }
}

}  // namespace streams
