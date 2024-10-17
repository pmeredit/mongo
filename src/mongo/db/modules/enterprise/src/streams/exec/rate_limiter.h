/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <climits>
#include <cstdint>

#include "mongo/util/duration.h"
#include "mongo/util/timer.h"

namespace streams {

using namespace mongo;

// This is a non-thread-safe implementation of a token bucket rate limiter.
class RateLimiter {
public:
    /**
     * tokensRefilledPerSec specifies how many tokens should be generated per second. capacity
     * specifies the maximum number of tokens the bucket can hold.
     */
    RateLimiter(int64_t tokensRefilledPerSec, int64_t capacity, Timer* timer);

    // Attempts to consume tokens. Returns the necessary non-zero delay to wait if tokens aren't yet
    // available.
    Microseconds consume(int64_t tokens = 1);

    void setTokensRefilledPerSec(int64_t tokensRefilledPerSec);
    void setCapacity(int64_t capacity);

private:
    int64_t _tokensRefilledPerSec;
    int64_t _capacity;

    Timer* _timer;

    Microseconds _lastConsumptionTime;
    int64_t _lastAvailableTokens;
};

}  // namespace streams
