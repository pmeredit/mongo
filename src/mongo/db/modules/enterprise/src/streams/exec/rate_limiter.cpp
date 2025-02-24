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

RateLimiter::RateLimiter(double tokensRefilledPerSec, int64_t capacity, Timer* timer)
    : _tokensRefilledPerSec{tokensRefilledPerSec},
      _capacity{capacity},
      _timer{timer},
      _lastConsumptionTime{timer->elapsed()},
      _lastAvailableTokens{capacity} {
    validate();
}

RateLimiter::RateLimiter(int64_t tokensRefilledPerSec, Timer* timer)
    : RateLimiter{1.0 * tokensRefilledPerSec, tokensRefilledPerSec, timer} {}

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

void RateLimiter::setTokensRefilledPerSecAndCapacity(double tokensRefilledPerSec,
                                                     int64_t capacity) {
    _tokensRefilledPerSec = tokensRefilledPerSec;
    _capacity = capacity;
    reset();
}

void RateLimiter::setTokensRefilledPerSec(double tokensRefilledPerSec) {
    setTokensRefilledPerSecAndCapacity(tokensRefilledPerSec, _capacity);
}

void RateLimiter::setCapacity(int64_t capacity) {
    setTokensRefilledPerSecAndCapacity(_tokensRefilledPerSec, capacity);
}

void RateLimiter::validate() {
    uassert(ErrorCodes::InternalError,
            "tokensRefilledPerSec is not greater than 0",
            _tokensRefilledPerSec > 0);
    uassert(ErrorCodes::InternalError, "capacity is not greater than 0", _capacity > 0);
    tassert(ErrorCodes::InternalError, "timer is null", _timer != nullptr);
}

void RateLimiter::reset() {
    _lastConsumptionTime = _timer->elapsed();
    _lastAvailableTokens = _capacity;
    validate();
}


}  // namespace streams
