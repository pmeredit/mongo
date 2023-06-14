/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/framework.h"
#include "mongo/unittest/unittest.h"

#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/message.h"
#include "streams/exec/watermark_combiner.h"

namespace streams {

using namespace mongo;

class DelayedWatermarkGeneratorTest : public unittest::Test {
protected:
    int64_t getMaxEventTimestampMs(DelayedWatermarkGenerator* generator) {
        return generator->_maxEventTimestampMs;
    }
};

TEST_F(DelayedWatermarkGeneratorTest, Basic) {
    streams::WatermarkCombiner combiner(/*numInputs*/ 1);
    DelayedWatermarkGenerator generator(/*inputIdx*/ 0, &combiner, /*allowedLatenessMs*/ 300'000);

    // Test that generator honors allowedLatenessMs.
    generator.onEvent(/*eventTimestampMs*/ 20'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 0);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 0);

    // Test that generator honors allowedLatenessMs.
    generator.onEvent(/*eventTimestampMs*/ 100'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 0);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 0);

    // Test that generator honors allowedLatenessMs.
    generator.onEvent(/*eventTimestampMs*/ 300'000);
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
    generator.onEvent(/*eventTimestampMs*/ 300'002);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 1);

    // Test that watermark remains unchanged when out of order events arrive.
    generator.onEvent(/*eventTimestampMs*/ 300'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 1);

    // Test that watermark remains unchanged when out of order events arrive.
    generator.onEvent(/*eventTimestampMs*/ 100'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 1);

    // Test that watermark keeps advancing as event timestamp keeps increasing.
    generator.onEvent(/*eventTimestampMs*/ 400'001);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().eventTimeWatermarkMs, 100'000);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().eventTimeWatermarkMs, 100'000);
}

TEST_F(DelayedWatermarkGeneratorTest, Restore) {
    struct TestSpec {
        int64_t allowedLateness;
        int64_t eventTimestamp1;
        int64_t eventTimestamp2;
        int64_t expectedWatermark1;
        int64_t expectedWatermark2;
        int64_t expectedCombinedWatermark;
        int64_t expectedEventTimestamp1;
        int64_t expectedEventTimestamp2;
        WatermarkStatus status1{WatermarkStatus::kActive};
        WatermarkStatus status2{WatermarkStatus::kActive};
    };
    auto innerTest = [&](TestSpec spec) {
        streams::WatermarkCombiner combiner(/*numInputs*/ 2);
        DelayedWatermarkGenerator generator1(/*inputIdx*/ 0, &combiner, spec.allowedLateness);
        DelayedWatermarkGenerator generator2(/*inputIdx*/ 1, &combiner, spec.allowedLateness);

        // Verify generator1
        generator1.onEvent(spec.eventTimestamp1);
        if (spec.status1 == WatermarkStatus::kIdle) {
            generator1.setIdle();
        }
        auto watermarkMsg1 = generator1.getWatermarkMsg();
        ASSERT_EQUALS(spec.expectedWatermark1, watermarkMsg1.eventTimeWatermarkMs);
        ASSERT_EQUALS(spec.eventTimestamp1, getMaxEventTimestampMs(&generator1));
        ASSERT_EQUALS(spec.status1, watermarkMsg1.watermarkStatus);

        // Verify generator2
        generator2.onEvent(spec.eventTimestamp2);
        if (spec.status2 == WatermarkStatus::kIdle) {
            generator2.setIdle();
        }
        auto watermarkMsg2 = generator2.getWatermarkMsg();
        ASSERT_EQUALS(spec.expectedWatermark2, watermarkMsg2.eventTimeWatermarkMs);
        ASSERT_EQUALS(spec.eventTimestamp2, getMaxEventTimestampMs(&generator2));
        ASSERT_EQUALS(spec.status2, watermarkMsg2.watermarkStatus);

        // Verify the combined watermark
        auto combinedMsg = combiner.getCombinedWatermarkMsg();
        ASSERT_EQUALS(combinedMsg.watermarkStatus, WatermarkStatus::kActive);
        ASSERT_EQUALS(spec.expectedCombinedWatermark, combinedMsg.eventTimeWatermarkMs);

        // Created restored generators and combiner.
        streams::WatermarkCombiner restoredCombiner(/*numInputs*/ 2);
        DelayedWatermarkGenerator restoredGenerator1(
            /*inputIdx*/ 0,
            &restoredCombiner,
            /*allowedLatenessMs*/ spec.allowedLateness,
            watermarkMsg1);
        DelayedWatermarkGenerator restoredGenerator2(
            /*inputIdx*/ 1,
            &restoredCombiner,
            /*allowedLatenessMs*/ spec.allowedLateness,
            watermarkMsg2);

        // Verify the restored generators have the same watermark.
        ASSERT_EQUALS(watermarkMsg1, restoredGenerator1.getWatermarkMsg());
        ASSERT_EQUALS(spec.expectedEventTimestamp1, getMaxEventTimestampMs(&restoredGenerator1));
        ASSERT_EQUALS(watermarkMsg2, restoredGenerator2.getWatermarkMsg());
        ASSERT_EQUALS(spec.expectedEventTimestamp2, getMaxEventTimestampMs(&restoredGenerator2));
        // Verify the restored combiner has the same watermar.
        ASSERT_EQUALS(combinedMsg, restoredCombiner.getCombinedWatermarkMsg());
    };

    innerTest({.allowedLateness = 0,
               .eventTimestamp1 = 0,
               .eventTimestamp2 = 0,
               .expectedWatermark1 = 0,
               .expectedWatermark2 = 0,
               .expectedCombinedWatermark = 0,
               .expectedEventTimestamp1 = 1,
               .expectedEventTimestamp2 = 1});

    innerTest({.allowedLateness = 300'000,
               .eventTimestamp1 = 300'000,
               .eventTimestamp2 = 300'000,
               .expectedWatermark1 = 0,
               .expectedWatermark2 = 0,
               .expectedCombinedWatermark = 0,
               .expectedEventTimestamp1 = 300'001,
               .expectedEventTimestamp2 = 300'001});

    innerTest({.allowedLateness = 300'000,
               .eventTimestamp1 = 300'002,
               .eventTimestamp2 = 300'002,
               .expectedWatermark1 = 1,
               .expectedWatermark2 = 1,
               .expectedCombinedWatermark = 1,
               .expectedEventTimestamp1 = 300'002,
               .expectedEventTimestamp2 = 300'002});

    innerTest({.allowedLateness = 300'000,
               .eventTimestamp1 = 300'200,
               .eventTimestamp2 = 300'200,
               .expectedWatermark1 = 199,
               .expectedWatermark2 = 199,
               .expectedCombinedWatermark = 199,
               .expectedEventTimestamp1 = 300'200,
               .expectedEventTimestamp2 = 300'200});

    innerTest({.allowedLateness = 0,
               .eventTimestamp1 = 1'672'552'800'00,
               .eventTimestamp2 = 1'672'552'800'00,
               .expectedWatermark1 = 1'672'552'800'00 - 1,
               .expectedWatermark2 = 1'672'552'800'00 - 1,
               .expectedCombinedWatermark = 1'672'552'800'00 - 1,
               .expectedEventTimestamp1 = 1'672'552'800'00,
               .expectedEventTimestamp2 = 1'672'552'800'00});

    int64_t allowedLateness = 300'000;
    int64_t timestamp = 1'672'552'800'00;
    innerTest({.allowedLateness = allowedLateness,
               .eventTimestamp1 = timestamp,
               .eventTimestamp2 = timestamp,
               .expectedWatermark1 = timestamp - allowedLateness - 1,
               .expectedWatermark2 = timestamp - allowedLateness - 1,
               .expectedCombinedWatermark = timestamp - allowedLateness - 1,
               .expectedEventTimestamp1 = timestamp,
               .expectedEventTimestamp2 = timestamp});

    // Note: With an eventTimestamp within {allowedLateness} of the epoch, the restored
    // DelayedWatermarkGenerator's private _maxEventTimestampMs may equal what was actually observed
    // in the data. This is fine because the actual computed watermark will just be 0, as it was
    // when the checkpoint was generated.
    innerTest({.allowedLateness = 300'000,
               .eventTimestamp1 = 290'000,
               .eventTimestamp2 = 290'000,
               .expectedWatermark1 = 0,
               .expectedWatermark2 = 0,
               .expectedCombinedWatermark = 0,
               .expectedEventTimestamp1 = 300'001,
               .expectedEventTimestamp2 = 300'001});

    // Verify results when there are two watermark generators with different watermarks.
    int64_t eventTimestamp1 = 1'672'552'800'00;
    int64_t eventTimestamp2 = 1'572'552'800'00;
    int64_t expectedWatermark1 = eventTimestamp1 - 1;
    int64_t expectedWatermark2 = eventTimestamp2 - 1;
    innerTest({.allowedLateness = 0,
               .eventTimestamp1 = eventTimestamp1,
               .eventTimestamp2 = eventTimestamp2,
               .expectedWatermark1 = expectedWatermark1,
               .expectedWatermark2 = expectedWatermark2,
               .expectedCombinedWatermark = std::min(expectedWatermark1, expectedWatermark2),
               .expectedEventTimestamp1 = eventTimestamp1,
               .expectedEventTimestamp2 = eventTimestamp2});

    eventTimestamp1 = 672'552'800'00;
    eventTimestamp2 = 1'572'552'800'00;
    expectedWatermark1 = eventTimestamp1 - 1;
    expectedWatermark2 = eventTimestamp2 - 1;
    innerTest({.allowedLateness = 0,
               .eventTimestamp1 = eventTimestamp1,
               .eventTimestamp2 = eventTimestamp2,
               .expectedWatermark1 = expectedWatermark1,
               .expectedWatermark2 = expectedWatermark2,
               .expectedCombinedWatermark = std::min(expectedWatermark1, expectedWatermark2),
               .expectedEventTimestamp1 = eventTimestamp1,
               .expectedEventTimestamp2 = eventTimestamp2});

    // Verify results when one watermark generator is idle
    eventTimestamp1 = 1'672'552'800'00;
    eventTimestamp2 = 1'572'552'800'00;
    expectedWatermark1 = eventTimestamp1 - 1;
    expectedWatermark2 = eventTimestamp2 - 1;
    innerTest({.allowedLateness = 0,
               .eventTimestamp1 = eventTimestamp1,
               .eventTimestamp2 = eventTimestamp2,
               .expectedWatermark1 = expectedWatermark1,
               .expectedWatermark2 = expectedWatermark2,
               .expectedCombinedWatermark = expectedWatermark1,
               .expectedEventTimestamp1 = eventTimestamp1,
               .expectedEventTimestamp2 = eventTimestamp2,
               .status1 = WatermarkStatus::kActive,
               .status2 = WatermarkStatus::kIdle});
}

}  // namespace streams
