/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <chrono>
#include <cmath>

#include "mongo/util/duration.h"

namespace streams {

// Utilty to sleep based on an exponential backoff schedule.
class Sleeper {
public:
    struct Options {
        mongo::Milliseconds minSleep{-1};
        mongo::Milliseconds maxSleep{-1};
    };

    Sleeper(Options options) : _options(std::move(options)) {
        tassert(mongo::ErrorCodes::InternalError,
                "minSleep should be set",
                _options.minSleep >= mongo::Milliseconds(0));
        tassert(mongo::ErrorCodes::InternalError,
                "maxSleep should be set",
                _options.maxSleep >= mongo::Milliseconds(0));
        tassert(mongo::ErrorCodes::InternalError,
                "maxSleep should gte minSleep",
                _options.maxSleep >= _options.minSleep);
    }

    void sleep() {
        mongo::stdx::this_thread::sleep_for(
            mongo::stdx::chrono::milliseconds{getSleepTime().count()});
        ++_count;
    }

    void reset() {
        _count = 0;
    }

    mongo::Milliseconds getSleepTime() {
        return std::min(_options.maxSleep,
                        _options.minSleep * static_cast<int>(std::pow(2, _count)));
    }

private:
    Options _options;
    int32_t _count{0};
};

};  // namespace streams
