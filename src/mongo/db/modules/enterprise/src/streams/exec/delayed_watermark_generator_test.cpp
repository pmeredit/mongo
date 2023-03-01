/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/unittest/unittest.h"

#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/watermark_combiner.h"

namespace streams {
namespace {

using namespace mongo;

TEST(DelayedWatermarkGenerator, Basic) {
    streams::WatermarkCombiner combiner(/*numInputs*/ 1);
    DelayedWatermarkGenerator generator(/*inputIdx*/ 0, &combiner, /*allowedLatenessMs*/ 300'000);

    // Test that generator honors allowedLatenessMs.
    generator.onEvent(/*eventTimeMs*/ 20'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 0);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 0);

    // Test that generator honors allowedLatenessMs.
    generator.onEvent(/*eventTimeMs*/ 100'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 0);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 0);

    // Test that generator honors allowedLatenessMs.
    generator.onEvent(/*eventTimeMs*/ 300'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 0);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 0);

    // Test that generator sets the idle/active status as expected.
    generator.setIdle();
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kIdle);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 0);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kIdle);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 0);
    generator.setActive();

    // Test that watermark advances after allowedLatenessMs.
    generator.onEvent(/*eventTimeMs*/ 300'002);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 1);

    // Test that watermark remains unchanged when out of order events arrive.
    generator.onEvent(/*eventTimeMs*/ 300'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 1);

    // Test that watermark remains unchanged when out of order events arrive.
    generator.onEvent(/*eventTimeMs*/ 100'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 1);

    // Test that watermark keeps advancing as event timestamp keeps increasing.
    generator.onEvent(/*eventTimeMs*/ 400'001);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 100'000);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 100'000);
}

}  // namespace
}  // namespace streams
