/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "streams/exec/message.h"

namespace streams {

class WatermarkCombiner;

/**
 * This class is the abstract base class of all watermark generators.
 * A watermark generator is used to compute the watermark for a shard/partition based on the event
 * time of the events received on that shard/partition.
 */
class WatermarkGenerator {
public:
    /**
     * inputIdx identifies the shard/partition that this watermark generator corresponds to.
     * If a WatermarkCombiner instance is provided, it will be sent the watermark of this
     * shard/partition whenever onEvent() is called.
     */
    WatermarkGenerator(int32_t inputIdx,
                       boost::optional<WatermarkControlMsg> initialWatermark,
                       WatermarkCombiner* combiner);

    virtual ~WatermarkGenerator() = default;

    /**
     * Returns the current watermark for this shard/partition.
     */
    const WatermarkControlMsg& getWatermarkMsg() const {
        return _watermarkMsg;
    }

    /**
     * Whether the given event timestamp is older than the watermark. Caller should drop late
     * events.
     */
    bool isLate(int64_t eventTimestampMs) const {
        return (eventTimestampMs <= _watermarkMsg.watermarkTimestampMs);
    }

    /**
     * This is called when an event i.e. a document is received by a source operator on this
     * shard/partition.
     */
    void onEvent(int64_t eventTimestampMs);

    /**
     * Mark this shard/partition idle.
     */
    void setIdle();

    /**
     * Mark this shard/partition active.
     */
    void setActive();

protected:
    virtual void doOnEvent(int64_t eventTimestampMs) = 0;
    virtual void doSetIdle() = 0;
    virtual void doSetActive() = 0;

    int32_t _inputIdx{0};
    // Tracks the current watermark for this shard/partition.
    WatermarkControlMsg _watermarkMsg;
    // Tracks the WatermarkCombiner instance that will be sent the watermark of this
    // shard/partition whenever onEvent() is called.
    WatermarkCombiner* _combiner{nullptr};
};

}  // namespace streams
