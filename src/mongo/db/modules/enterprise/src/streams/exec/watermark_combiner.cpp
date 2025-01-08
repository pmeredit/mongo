/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

#include "streams/exec/watermark_combiner.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace streams {

using namespace mongo;

WatermarkCombiner::WatermarkCombiner(int32_t numInputs) : _numInputs(numInputs) {
    _watermarkMsgs.resize(_numInputs);
}

void WatermarkCombiner::onWatermarkMsg(int32_t inputIdx, const WatermarkControlMsg& watermarkMsg) {
    dassert(inputIdx < _numInputs);

    auto& wMsg = _watermarkMsgs[inputIdx];
    if (wMsg.watermarkStatus != watermarkMsg.watermarkStatus) {
        // WatermarkStatus changed for this input. So, we should recompute the combined watermark.
        wMsg.watermarkStatus = watermarkMsg.watermarkStatus;
        _recomputeCombinedWatermark = true;
    }

    if (wMsg.watermarkTimestampMs < watermarkMsg.watermarkTimestampMs) {
        wMsg.watermarkTimestampMs = watermarkMsg.watermarkTimestampMs;
        if (inputIdx == _minWatermarkInputIdx) {
            // The input that advanced previously had the lowest watermark. So, we should
            // recompute the combined watermark.
            _recomputeCombinedWatermark = true;
        }
    }
}

const WatermarkControlMsg& WatermarkCombiner::getCombinedWatermarkMsg() {
    if (_recomputeCombinedWatermark) {
        computeCombinedWatermark();
        _recomputeCombinedWatermark = false;
    }
    return _combinedWatermarkMsg;
}

void WatermarkCombiner::computeCombinedWatermark() {
    int64_t minWatermarkTimestampMs = std::numeric_limits<int64_t>::max();
    // Tracks the lowest watermark timestamp across all active inputs.
    int newMinWatermarkInputIdx{0};
    bool allIdle{true};
    for (size_t i = 0; i < _watermarkMsgs.size(); ++i) {
        volatile auto& wMsg = _watermarkMsgs[i];
        if (wMsg.watermarkStatus == WatermarkStatus::kIdle) {
            // Exclude idle inputs when computing the watermark timestamp.
            continue;
        }

        allIdle = false;
        if (minWatermarkTimestampMs > wMsg.watermarkTimestampMs) {
            minWatermarkTimestampMs = wMsg.watermarkTimestampMs;
            newMinWatermarkInputIdx = i;
        }
    }

    if (allIdle) {
        _combinedWatermarkMsg.watermarkStatus = WatermarkStatus::kIdle;
    } else {
        _combinedWatermarkMsg.watermarkStatus = WatermarkStatus::kActive;
        _minWatermarkInputIdx = newMinWatermarkInputIdx;
        if (_combinedWatermarkMsg.watermarkTimestampMs <= minWatermarkTimestampMs) {
            _combinedWatermarkMsg.watermarkTimestampMs = minWatermarkTimestampMs;
        }
    }
}

}  // namespace streams
