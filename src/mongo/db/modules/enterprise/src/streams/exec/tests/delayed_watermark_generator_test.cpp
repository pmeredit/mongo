/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
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
    DelayedWatermarkGenerator generator(/*inputIdx*/ 0, &combiner);

    // Test that the generator has the watermark message uninitialized before the first message is
    // fed.
    ASSERT_EQUALS(-1, generator.getWatermarkMsg().watermarkTimestampMs);

    // Test that _ts=0 is allowed through when there is no watermark yet.
    ASSERT_FALSE(generator.isLate(0));

    generator.onEvent(/*eventTimestampMs*/ 20'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkTimestampMs, 20'000 - 1);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 20'000 - 1);

    // Test that generator sets the idle/active status as expected.
    generator.setIdle();
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kIdle);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkTimestampMs, 20'000 - 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kIdle);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 20'000 - 1);
    generator.setActive();

    // Test that watermark remains unchanged when out of order events arrive.
    generator.onEvent(/*eventTimestampMs*/ 300'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkTimestampMs, 300'000 - 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 300'000 - 1);

    // Test that watermark remains unchanged when out of order events arrive.
    generator.onEvent(/*eventTimestampMs*/ 100'000);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkTimestampMs, 300'000 - 1);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 300'000 - 1);

    // Test that watermark keeps advancing as event timestamp keeps increasing.
    generator.onEvent(/*eventTimestampMs*/ 400'001);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(generator.getWatermarkMsg().watermarkTimestampMs, 400'000);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkStatus, WatermarkStatus::kActive);
    ASSERT_EQUALS(combiner.getCombinedWatermarkMsg().watermarkTimestampMs, 400'000);
}

TEST_F(DelayedWatermarkGeneratorTest, Restore) {
    struct TestSpec {
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
        DelayedWatermarkGenerator generator1(/*inputIdx*/ 0, &combiner);
        DelayedWatermarkGenerator generator2(/*inputIdx*/ 1, &combiner);

        // Verify generator1
        generator1.onEvent(spec.eventTimestamp1);
        if (spec.status1 == WatermarkStatus::kIdle) {
            generator1.setIdle();
        }
        auto watermarkMsg1 = generator1.getWatermarkMsg();
        ASSERT_EQUALS(spec.expectedWatermark1, watermarkMsg1.watermarkTimestampMs);
        ASSERT_EQUALS(spec.eventTimestamp1, getMaxEventTimestampMs(&generator1));
        ASSERT_EQUALS(spec.status1, watermarkMsg1.watermarkStatus);

        // Verify generator2
        generator2.onEvent(spec.eventTimestamp2);
        if (spec.status2 == WatermarkStatus::kIdle) {
            generator2.setIdle();
        }
        auto watermarkMsg2 = generator2.getWatermarkMsg();
        ASSERT_EQUALS(spec.expectedWatermark2, watermarkMsg2.watermarkTimestampMs);
        ASSERT_EQUALS(spec.eventTimestamp2, getMaxEventTimestampMs(&generator2));
        ASSERT_EQUALS(spec.status2, watermarkMsg2.watermarkStatus);

        // Verify the combined watermark
        auto combinedMsg = combiner.getCombinedWatermarkMsg();
        ASSERT_EQUALS(combinedMsg.watermarkStatus, WatermarkStatus::kActive);
        ASSERT_EQUALS(spec.expectedCombinedWatermark, combinedMsg.watermarkTimestampMs);

        // Created restored generators and combiner.
        streams::WatermarkCombiner restoredCombiner(/*numInputs*/ 2);
        DelayedWatermarkGenerator restoredGenerator1(
            /*inputIdx*/ 0, &restoredCombiner, watermarkMsg1);
        DelayedWatermarkGenerator restoredGenerator2(
            /*inputIdx*/ 1, &restoredCombiner, watermarkMsg2);

        // Verify the restored generators have the same watermark.
        ASSERT_EQUALS(watermarkMsg1, restoredGenerator1.getWatermarkMsg());
        ASSERT_EQUALS(spec.expectedEventTimestamp1, getMaxEventTimestampMs(&restoredGenerator1));
        ASSERT_EQUALS(watermarkMsg2, restoredGenerator2.getWatermarkMsg());
        ASSERT_EQUALS(spec.expectedEventTimestamp2, getMaxEventTimestampMs(&restoredGenerator2));
        // Verify the restored combiner has the same watermar.
        ASSERT_EQUALS(combinedMsg, restoredCombiner.getCombinedWatermarkMsg());
    };

    innerTest({.eventTimestamp1 = 0,
               .eventTimestamp2 = 0,
               .expectedWatermark1 = -1,
               .expectedWatermark2 = -1,
               .expectedCombinedWatermark = -1,
               .expectedEventTimestamp1 = 0,
               .expectedEventTimestamp2 = 0});

    innerTest({.eventTimestamp1 = 300'000,
               .eventTimestamp2 = 300'000,
               .expectedWatermark1 = 300'000 - 1,
               .expectedWatermark2 = 300'000 - 1,
               .expectedCombinedWatermark = 300'000 - 1,
               .expectedEventTimestamp1 = 300'000,
               .expectedEventTimestamp2 = 300'000});

    innerTest({.eventTimestamp1 = 300'002,
               .eventTimestamp2 = 300'002,
               .expectedWatermark1 = 300'002 - 1,
               .expectedWatermark2 = 300'002 - 1,
               .expectedCombinedWatermark = 300'002 - 1,
               .expectedEventTimestamp1 = 300'002,
               .expectedEventTimestamp2 = 300'002});

    innerTest({.eventTimestamp1 = 300'200,
               .eventTimestamp2 = 300'200,
               .expectedWatermark1 = 300'200 - 1,
               .expectedWatermark2 = 300'200 - 1,
               .expectedCombinedWatermark = 300'200 - 1,
               .expectedEventTimestamp1 = 300'200,
               .expectedEventTimestamp2 = 300'200});

    innerTest({.eventTimestamp1 = 1'672'552'800'00,
               .eventTimestamp2 = 1'672'552'800'00,
               .expectedWatermark1 = 1'672'552'800'00 - 1,
               .expectedWatermark2 = 1'672'552'800'00 - 1,
               .expectedCombinedWatermark = 1'672'552'800'00 - 1,
               .expectedEventTimestamp1 = 1'672'552'800'00,
               .expectedEventTimestamp2 = 1'672'552'800'00});

    int64_t timestamp = 1'672'552'800'00;
    innerTest({.eventTimestamp1 = timestamp,
               .eventTimestamp2 = timestamp,
               .expectedWatermark1 = timestamp - 1,
               .expectedWatermark2 = timestamp - 1,
               .expectedCombinedWatermark = timestamp - 1,
               .expectedEventTimestamp1 = timestamp,
               .expectedEventTimestamp2 = timestamp});

    // Note: With an eventTimestamp within {allowedLateness} of the epoch, the restored
    // DelayedWatermarkGenerator's private _maxEventTimestampMs may equal what was actually observed
    // in the data. This is fine because the actual computed watermark will just be 0, as it was
    // when the checkpoint was generated.
    innerTest({.eventTimestamp1 = 290'000,
               .eventTimestamp2 = 290'000,
               .expectedWatermark1 = 290'000 - 1,
               .expectedWatermark2 = 290'000 - 1,
               .expectedCombinedWatermark = 290'000 - 1,
               .expectedEventTimestamp1 = 290'000,
               .expectedEventTimestamp2 = 290'000});

    // Verify results when there are two watermark generators with different watermarks.
    int64_t eventTimestamp1 = 1'672'552'800'00;
    int64_t eventTimestamp2 = 1'572'552'800'00;
    int64_t expectedWatermark1 = eventTimestamp1 - 1;
    int64_t expectedWatermark2 = eventTimestamp2 - 1;
    innerTest({.eventTimestamp1 = eventTimestamp1,
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
    innerTest({.eventTimestamp1 = eventTimestamp1,
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
    innerTest({.eventTimestamp1 = eventTimestamp1,
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
