/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/stdx/unordered_map.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * This class can be used by an operator to combine watermarks received from all its inputs to
 * generate the watermark for the operator itself.
 * Note that this class is not thread-safe.
 */
class WatermarkCombiner {
public:
    WatermarkCombiner(int32_t numInputs);

    /**
     * This is called when a WatermarkControlMsg is received by this operator on
     * its input link inputIdx.
     * inputIdx is always 0 for a single input operator.
     */
    void onWatermarkMsg(int32_t inputIdx, const WatermarkControlMsg& watermarkMsg);

    /**
     * Returns the result of combining WatermarkControlMsgs received from all inputs.
     */
    const WatermarkControlMsg& getCombinedWatermarkMsg();

protected:
    // Combines the latest WatermarkControlMsgs received from all inputs and stores the result in
    // _combinedWatermarkMsg.
    void computeCombinedWatermark();

    int32_t _numInputs{0};
    // Tracks the latest WatermarkControlMsg received from each input.
    std::vector<WatermarkControlMsg> _watermarkMsgs;
    // Tracks the index of an active input in _watermarkMsgs that has the lowest
    // WatermarkControlMsg.watermarkTimestampMs.
    int _minWatermarkInputIdx{0};
    // Tracks the result of combining WatermarkControlMsgs received from all inputs.
    WatermarkControlMsg _combinedWatermarkMsg;
    // Whether _combinedWatermarkMsg is stale and needs to be recomputed.
    bool _recomputeCombinedWatermark{true};
};

}  // namespace streams
