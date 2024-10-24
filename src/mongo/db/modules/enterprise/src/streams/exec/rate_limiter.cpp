/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "rate_limiter.h"

#include "mongo/util/timer.h"
#include <cmath>
#include <cstdint>

#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"

constexpr int kMicrosecondsPerSecond = 1'000'000;

namespace streams {

RateLimiter::RateLimiter(int64_t tokensRefilledPerSec, int64_t capacity, Timer* timer)
    : _tokensRefilledPerSec{tokensRefilledPerSec},
      _capacity{capacity},
      _timer{timer},
      _lastConsumptionTime{timer->elapsed()},
      _lastAvailableTokens{capacity} {
    uassert(ErrorCodes::InternalError,
            "tokensRefilledPerSec is not greater than 0",
            tokensRefilledPerSec > 0);
    uassert(ErrorCodes::InternalError, "capacity is not greater than 0", capacity > 0);
    tassert(ErrorCodes::InternalError, "timer is null", timer != nullptr);
}

RateLimiter::RateLimiter(int64_t tokensRefilledPerSec, Timer* timer) {
    *this = RateLimiter{tokensRefilledPerSec, tokensRefilledPerSec, timer};
}

Microseconds RateLimiter::consume(int64_t tokens) {
    uassert(ErrorCodes::InternalError, "Tokens requested are less than 1", tokens > 0);
    uassert(
        ErrorCodes::InternalError, "Tokens requested exceed bucket capacity", tokens <= _capacity);

    const auto now = _timer->elapsed();
    const auto timeSinceLastConsumption = now - _lastConsumptionTime;
    const auto availableTokens =
        std::min<double>(_capacity,
                         _lastAvailableTokens +
                             1.0 * timeSinceLastConsumption.count() * _tokensRefilledPerSec /
                                 kMicrosecondsPerSecond);

    if (availableTokens < tokens) {
        const int64_t microsecDelay =
            std::ceil((tokens - availableTokens) / _tokensRefilledPerSec * kMicrosecondsPerSecond);
        return Microseconds(microsecDelay);
    }

    // Update the state of the bucket at the last consumption
    _lastConsumptionTime = now;
    _lastAvailableTokens = availableTokens - tokens;

    return Microseconds(0);
}

void RateLimiter::setTokensRefilledPerSec(int64_t tokensRefilledPerSec) {
    *this = RateLimiter{tokensRefilledPerSec, _capacity, _timer};
}

void RateLimiter::setCapacity(int64_t capacity) {
    *this = RateLimiter{_tokensRefilledPerSec, capacity, _timer};
}

}  // namespace streams
