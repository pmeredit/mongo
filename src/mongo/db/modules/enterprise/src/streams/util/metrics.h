#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "mongo/platform/atomic_word.h"

namespace streams {

// Base class of all metrics. All metrics are thread-safe.
class Metric {
public:
    virtual ~Metric() = default;
};

// A metric that represents a single monotonically increasing counter.
class Counter : public Metric {
public:
    void increment(int64_t val = 1) {
        _value.fetchAndAddRelaxed(val);
    }

    int64_t value() const {
        return _value.loadRelaxed();
    }

private:
    mongo::AtomicWord<int64_t> _value{0};
};

// A metric that represents a single numerical value that can arbitrarily go up and down.
class Gauge : public Metric {
public:
    void set(double val) {
        _value.storeRelaxed(val);
    }

    double value() const {
        return _value.loadRelaxed();
    }

private:
    mongo::AtomicWord<double> _value{0};
};

// A Gauge whose value is retrieved via a callback function.
class CallbackGauge : public Metric {
public:
    using CallbackFn = std::function<double()>;

    CallbackGauge(CallbackFn fn) : Metric(), _callbackFn(std::move(fn)) {}

    double value() const {
        return _callbackFn();
    }

private:
    CallbackFn _callbackFn;
};

}  // namespace streams
