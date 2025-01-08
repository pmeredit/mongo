/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/watermark_generator.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/watermark_combiner.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

WatermarkGenerator::WatermarkGenerator(int32_t inputIdx,
                                       boost::optional<WatermarkControlMsg> initialWatermark,
                                       WatermarkCombiner* combiner)
    : _inputIdx(inputIdx), _combiner(combiner) {
    if (initialWatermark) {
        // -1 indicates that the watermark timestamp has not be set yet.
        invariant(initialWatermark->watermarkTimestampMs >= -1);
        _watermarkMsg = *initialWatermark;
        if (_combiner) {
            _combiner->onWatermarkMsg(_inputIdx, _watermarkMsg);
        }
    }
}

void WatermarkGenerator::onEvent(int64_t eventTimestampMs) {
    doOnEvent(eventTimestampMs);
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
