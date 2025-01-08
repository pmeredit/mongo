/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/unittest/unittest.h"

#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/watermark_combiner.h"

namespace streams {
namespace {

using namespace mongo;

void pushNewEventTime(std::vector<DelayedWatermarkGenerator>& generators,
                      std::initializer_list<size_t> inputs,
                      int64_t eventTimestampMs) {
    for (size_t input : inputs) {
        generators[input].onEvent(eventTimestampMs);
    }
}

void markInputsIdle(std::vector<DelayedWatermarkGenerator>& generators,
                    std::initializer_list<size_t> inputs) {
    for (size_t input : inputs) {
        generators[input].setIdle();
    }
}

void markInputsActive(std::vector<DelayedWatermarkGenerator>& generators,
                      std::initializer_list<size_t> inputs) {
    for (size_t input : inputs) {
        generators[input].setActive();
    }
}

TEST(WatermarkCombinerTest, MultipleInputs) {
    streams::WatermarkCombiner combiner(/*numInputs*/ 4);
    std::vector<DelayedWatermarkGenerator> generators;
    for (int i = 0; i < 4; ++i) {
        generators.push_back(DelayedWatermarkGenerator(
            /*inputIdx*/ i, &combiner));
    }

    // Test that combiner works as expected when all inputs are at the same watermark.
    pushNewEventTime(generators, {0, 1, 2, 3}, /*eventTimestampMs*/ 20'000);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 20'000 - 1);

    // Test that combiner works as expected when some inputs are behind.
    pushNewEventTime(generators, {1, 2, 3}, /*eventTimestampMs*/ 300'002);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 20'000 - 1);

    // Mark the delayed input idle and test that watermark advances now.
    markInputsIdle(generators, {0});
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 300'002 - 1);

    // Advance the watermark for the delayed input and test that watermark stays the same.
    markInputsActive(generators, {0});
    pushNewEventTime(generators, {0}, /*eventTimestampMs*/ 300'005);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 300'002 - 1);

    // Test that watermark remains unchanged when out of order events arrive.
    pushNewEventTime(generators, {0}, /*eventTimestampMs*/ 100'000);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 300'002 - 1);

    // Advance the watermark for some inputs and test that watermark stays the same.
    pushNewEventTime(generators, {0, 1, 2}, /*eventTimestampMs*/ 400'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 300'002 - 1);

    // Advance the watermark for the delayed input and test that watermark advances as well.
    pushNewEventTime(generators, {3}, /*eventTimestampMs*/ 400'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 400'000);

    // Advance the watermark for all inputs and test that watermark advances as well.
    pushNewEventTime(generators, {0, 1, 2, 3}, /*eventTimestampMs*/ 450'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 450'000);

    // Mark all inputs idle and then mark them back active. Then advance the watermark for all
    // inputs and test that watermark advances as well.
    markInputsIdle(generators, {0, 1, 2, 3});
    markInputsActive(generators, {0, 1, 2, 3});
    pushNewEventTime(generators, {0, 1, 2, 3}, /*eventTimestampMs*/ 500'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 500'000);

    // Mark 2 inputs idle, advance the watermak for the other 2 inputs and test that watermark
    // advances as well.
    markInputsIdle(generators, {0, 1});
    pushNewEventTime(generators, {2, 3}, /*eventTimestampMs*/ 700'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 700'000);

    // Mark one of the 2 idle inputs active, slightly advance the watermak for it and test that
    // watermark does not regress.
    markInputsActive(generators, {0});
    pushNewEventTime(generators, {0}, /*eventTimestampMs*/ 600'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 700'000);

    // Mark the remaining idle input active, slightly advance the watermak for it and test that
    // watermark does not regress.
    markInputsActive(generators, {1});
    pushNewEventTime(generators, {1}, /*eventTimestampMs*/ 650'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 700'000);

    // Advance the watermak for the 2 inputs that have been active for a while and test that
    // watermark stays the same.
    pushNewEventTime(generators, {2, 3}, /*eventTimestampMs*/ 800'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 700'000);

    // Advance the watermak for the 2 most delayed inputs and test that watermark advances as well.
    pushNewEventTime(generators, {0}, /*eventTimestampMs*/ 720'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 700'000);
    pushNewEventTime(generators, {1}, /*eventTimestampMs*/ 750'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 720'000);

    // Advance the watermak for the most delayed input once again and test that watermark advances
    // as well.
    pushNewEventTime(generators, {0}, /*eventTimestampMs*/ 800'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 750'000);

    // Advance the watermak for the most delayed input once again and test that watermark advances
    // as well.
    pushNewEventTime(generators, {1}, /*eventTimestampMs*/ 800'001);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 800'000);
}

}  // namespace
}  // namespace streams
