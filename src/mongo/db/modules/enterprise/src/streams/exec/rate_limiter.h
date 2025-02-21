/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <climits>
#include <cstdint>

#include "mongo/platform/rwmutex.h"
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
    RateLimiter(double tokensRefilledPerSec, int64_t capacity, Timer* timer);

    // Constructs a RateLimiter whose full burst period is a second
    RateLimiter(int64_t tokensRefilledPerSec, Timer* timer);

    // Attempts to consume tokens. Returns the necessary non-zero delay to wait if tokens aren't yet
    // available.
    Microseconds consume(int64_t tokens = 1);

    void setTokensRefilledPerSecAndCapacity(double tokensRefilledPerSec, int64_t capacity);
    void setTokensRefilledPerSec(double tokensRefilledPerSec);
    void setCapacity(int64_t capacity);

protected:
    void validate();
    void reset();

private:
    double _tokensRefilledPerSec{0};
    int64_t _capacity{0};

    Timer* _timer{nullptr};

    Microseconds _lastConsumptionTime{0};
    int64_t _lastAvailableTokens{0};
};

}  // namespace streams
