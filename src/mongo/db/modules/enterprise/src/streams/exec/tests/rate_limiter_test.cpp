/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/tick_source_mock.h"
#include "streams/exec/rate_limiter.h"
#include <random>

namespace streams {

namespace {

TickSourceMock<Microseconds> tickSource{};
Timer timer{&tickSource};

TEST(RateLimiterTest, IsFull) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(5), Seconds(0));
}

TEST(RateLimiterTest, IsEmpty) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    ASSERT_EQ(rateLimiter.consume(), Seconds(1));
}

TEST(RateLimiterTest, InsufficientTimeHasPassed) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    tickSource.advance(Seconds(5));
    ASSERT_EQ(rateLimiter.consume(6), Seconds(1));
}

TEST(RateLimiterTest, IsContinuous) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    tickSource.advance(Milliseconds(516));
    ASSERT_EQ(rateLimiter.consume(1), Milliseconds(484));
}

TEST(RateLimiterTest, HasInsufficientTokens) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(6), Seconds(0));
    ASSERT_EQ(rateLimiter.consume(5), Seconds(1));
}

TEST(RateLimiterTest, SufficientTimeHasPassed) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(6), Seconds(0));
    tickSource.advance(Seconds(2));
    ASSERT_EQ(rateLimiter.consume(5), Seconds(0));
}

TEST(RateLimiterTest, HasSufficientTokens) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(6), Seconds(0));
    ASSERT_EQ(rateLimiter.consume(2), Seconds(0));
}

TEST(RateLimiterTest, HasExactTokens) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(6), Seconds(0));
    ASSERT_EQ(rateLimiter.consume(4), Seconds(0));
}

TEST(RateLimiterTest, ConsumesMoreThanCapacity) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_THROWS_WHAT(
        rateLimiter.consume(11), mongo::DBException, "Tokens requested exceed bucket capacity");
}

TEST(RateLimiterTest, ConsumesInvalidTokens) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_THROWS_WHAT(
        rateLimiter.consume(0), mongo::DBException, "Tokens requested are less than 1");
    ASSERT_THROWS_WHAT(
        rateLimiter.consume(-1), mongo::DBException, "Tokens requested are less than 1");
}

TEST(RateLimiterTest, ConstructsWithInvalidTokenRate) {
    ASSERT_THROWS_WHAT(RateLimiter(0, 10, &timer),
                       mongo::DBException,
                       "tokensRefilledPerSec is not greater than 0");
    ASSERT_THROWS_WHAT(RateLimiter(-1, 10, &timer),
                       mongo::DBException,
                       "tokensRefilledPerSec is not greater than 0");
}

TEST(RateLimiterTest, ConstructsWithInvalidCapacity) {
    ASSERT_THROWS_WHAT(
        RateLimiter(1, 0, &timer), mongo::DBException, "capacity is not greater than 0");
    ASSERT_THROWS_WHAT(
        RateLimiter(1, -1, &timer), mongo::DBException, "capacity is not greater than 0");
}

TEST(RateLimiterTest, SetsInvalidTokenRate) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_THROWS_WHAT(rateLimiter.setTokensRefilledPerSec(0),
                       mongo::DBException,
                       "tokensRefilledPerSec is not greater than 0");
    ASSERT_THROWS_WHAT(rateLimiter.setTokensRefilledPerSec(-1),
                       mongo::DBException,
                       "tokensRefilledPerSec is not greater than 0");
}

TEST(RateLimiterTest, RefillRateIsSlower) {
    RateLimiter rateLimiter{2, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    tickSource.advance(Seconds(3));
    ASSERT_EQ(rateLimiter.consume(5), Seconds(0));

    rateLimiter.setTokensRefilledPerSec(1);
    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    tickSource.advance(Seconds(3));
    ASSERT_EQ(rateLimiter.consume(5), Seconds(2));
}

TEST(RateLimiterTest, RefillRateIsFaster) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    tickSource.advance(Seconds(3));
    ASSERT_EQ(rateLimiter.consume(5), Seconds(2));

    rateLimiter.setTokensRefilledPerSec(2);
    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));
    tickSource.advance(Seconds(3));
    ASSERT_EQ(rateLimiter.consume(5), Seconds(0));
}

TEST(RateLimiterTest, SetsInvalidCapacity) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_THROWS_WHAT(
        rateLimiter.setCapacity(0), mongo::DBException, "capacity is not greater than 0");
    ASSERT_THROWS_WHAT(
        rateLimiter.setCapacity(-1), mongo::DBException, "capacity is not greater than 0");
}

TEST(RateLimiterTest, CapacityIsLowered) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_THROWS_WHAT(
        rateLimiter.consume(11), mongo::DBException, "Tokens requested exceed bucket capacity");

    tickSource.advance(Seconds(1));
    rateLimiter.setCapacity(11);
    ASSERT_EQ(rateLimiter.consume(11), Seconds(0));
}

TEST(RateLimiterTest, CapacityIsRaised) {
    RateLimiter rateLimiter{1, 10, &timer};

    ASSERT_EQ(rateLimiter.consume(10), Seconds(0));

    rateLimiter.setCapacity(9);
    ASSERT_THROWS_WHAT(
        rateLimiter.consume(10), mongo::DBException, "Tokens requested exceed bucket capacity");
}

TEST(RateLimiterTest, SupportsReturningMicrosecondResolutionDelay) {
    RateLimiter rateLimiter{1'000'000, 10'000'000, &timer};

    ASSERT_EQ(rateLimiter.consume(9'999'999), Microseconds(0));
    ASSERT_EQ(rateLimiter.consume(3), Microseconds(2));
}

TEST(RateLimiterTest, SupportsTickingAtMicrosecondResolution) {
    RateLimiter rateLimiter{1'000'000, 10'000'000, &timer};

    ASSERT_EQ(rateLimiter.consume(9'999'999), Microseconds(0));
    tickSource.advance(Microseconds(1));
    ASSERT_EQ(rateLimiter.consume(3), Microseconds(1));
}

TEST(RateLimiterTest, CapacityIsLesserThanTokensPerSecond) {
    RateLimiter rateLimiter{10, 5, &timer};

    // Empty bucket
    ASSERT_EQ(rateLimiter.consume(5), Milliseconds(0));

    // Allow only enough time to pass for 2 tokens to be generated
    tickSource.advance(Milliseconds(200));
    ASSERT_EQ(rateLimiter.consume(3), Milliseconds(100));

    // Allow enough time for an additional token to be generated
    tickSource.advance(Milliseconds(100));
    ASSERT_EQ(rateLimiter.consume(3), Milliseconds(0));
}

TEST(RateLimiterTest, ShouldAcceptAllRequests) {
    RateLimiter rateLimiter{1000, 5000, &timer};

    std::default_random_engine generator;
    std::uniform_int_distribution distribution(1, 200);

    for (int second = 0; second < 100; second++) {
        for (int j = 0; j < 5; j++) {
            // Generate a number of tokens between 1 and 200 to consume
            auto tokensToConsume = distribution(generator);

            ASSERT_EQ(rateLimiter.consume(tokensToConsume), Seconds(0));
            tickSource.advance(Milliseconds(200));
        }
    }
}

}  // namespace

}  // namespace streams
