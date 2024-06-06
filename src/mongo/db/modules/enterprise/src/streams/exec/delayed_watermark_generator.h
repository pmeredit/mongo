/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "streams/exec/message.h"
#include "streams/exec/watermark_generator.h"

namespace streams {

class WatermarkCombiner;

/**
 * A watermark generator that allows events to arrive out of order and advances the watermark
 * after the specified delay.
 */
class DelayedWatermarkGenerator : public WatermarkGenerator {
public:
    /**
     * Refer to watermark_generator.h for comments on inputIdx and combiner.
     * allowedLatenessMs specifies the delay allowed in advancing the watermark.
     */
    DelayedWatermarkGenerator(int32_t inputIdx,
                              WatermarkCombiner* combiner,
                              boost::optional<WatermarkControlMsg> initialWatermark = boost::none);

private:
    friend class DelayedWatermarkGeneratorTest;
    friend class PlannerTest;

    void doOnEvent(int64_t eventTimestampMs) override;
    void doSetIdle() override;
    void doSetActive() override;

    // Tracks the maximum event timestamp seen so far.
    int64_t _maxEventTimestampMs{-1};
};

}  // namespace streams
