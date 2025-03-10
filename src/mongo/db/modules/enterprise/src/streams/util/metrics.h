/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <atomic>
#include <boost/container_hash/hash_fwd.hpp>
#include <boost/optional.hpp>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/chrono.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/histogram.h"

namespace streams {

// Base class of all types metrics that can be used to collect metrics. All Collectors are
// thread-safe.
class Collector {
public:
    Collector(std::string name, std::string description)
        : _name{std::move(name)}, _description{std::move(description)} {}

    virtual ~Collector() = default;
    virtual void takeSnapshot() = 0;

    const std::string& getName() const {
        return _name;
    }

    const std::string& getDescription() const {
        return _description;
    }

protected:
    std::string _name;
    std::string _description;
};

// A single sample value a unique set label names and values
class Metric : public Collector {
public:
    using LabelsVec = std::vector<std::pair<std::string, std::string>>;

    Metric(std::string name, std::string description, LabelsVec labels)
        : Collector{std::move(name), std::move(description)}, _labels{std::move(labels)} {}

    const LabelsVec& getLabels() const {
        return _labels;
    }

protected:
    LabelsVec _labels;
};

// A metric that represents a single monotonically increasing counter.
class Counter : public Metric {
public:
    Counter(std::string name, std::string description, LabelsVec labels)
        : Metric{std::move(name), std::move(description), std::move(labels)} {}

    void increment(int64_t val = 1) {
        _value.fetchAndAddRelaxed(val);
    }
    int64_t value() const {
        return _value.loadRelaxed();
    }

    // upper layers are responsible for calling this function
    // so that the callbackFn is called under a mutex.
    void takeSnapshot() override {
        _snapshotValue.store(value());
    }

    int64_t snapshotValue() {
        return _snapshotValue.loadRelaxed();
    }

private:
    mongo::AtomicWord<int64_t> _value{0};
    mongo::AtomicWord<int64_t> _snapshotValue{0};
};

// Collector that bundles with a common name, description and set of label names.
class CounterVec : public Collector {
public:
    using LabelNames = std::vector<std::string>;
    using LabelValues = std::vector<std::string>;

    // Constructs a CounterVec with specified base labels that should be constrained.
    CounterVec(std::string name,
               std::string description,
               Metric::LabelsVec baseLabels,
               LabelNames extraLabelNames)
        : Collector{std::move(name), std::move(description)},
          _baseLabels{std::move(baseLabels)},
          _extraLabelNames{std::move(extraLabelNames)} {}

    // Retrieves a counter with the base labels and specified additional labels.
    std::shared_ptr<Counter> withLabels(LabelValues extraLabelValues);

    std::vector<std::shared_ptr<Counter>> getCounters();

    void takeSnapshot() override;

private:
    std::size_t operator()(const LabelValues& labelValues) const {
        size_t seed{0};
        for (const auto& value : labelValues) {
            boost::hash_combine(seed, value);
        }
        return seed;
    }

    Metric::LabelsVec _baseLabels;
    LabelNames _extraLabelNames;
    mongo::stdx::unordered_map<LabelValues, std::shared_ptr<Counter>> _countersByExtraLabelValues;
    mongo::stdx::mutex _mutex;
};

// A metric that represents a single numerical value that can arbitrarily go up and down.
template <typename T>
class GaugeBase : public Metric {
public:
    GaugeBase(std::string name, std::string description, LabelsVec labels)
        : Metric{std::move(name), std::move(description), std::move(labels)} {}

    void set(T val) {
        _value.storeRelaxed(val);
    }

    template <typename U = T, typename = std::enable_if_t<std::is_same<U, int64_t>::value>>
    void incBy(int64_t val) {
        _value.fetchAndAddRelaxed(val);
    }

    T value() const {
        return _value.loadRelaxed();
    }

    // upper layers are responsible for calling this function
    // so that the callbackFn is called under a mutex.
    void takeSnapshot() override {
        _snapshotValue.store(value());
    }

    T snapshotValue() {
        return _snapshotValue.loadRelaxed();
    }

protected:
    mongo::AtomicWord<T> _value{0};
    mongo::AtomicWord<T> _snapshotValue{0};
};

using Gauge = GaugeBase<double>;
using IntGauge = GaugeBase<int64_t>;

// A Gauge whose value is retrieved via a callback function.
class CallbackGauge : public Metric {
public:
    using CallbackFn = std::function<double()>;

    CallbackGauge(std::string name, std::string description, LabelsVec labels, CallbackFn fn)
        : Metric{std::move(name), std::move(description), std::move(labels)},
          _callbackFn(std::move(fn)) {}

    double value() const {
        return _callbackFn();
    }

    // upper layers are responsible for calling this function
    // so that the callbackFn is called under a mutex.
    void takeSnapshot() override {
        _snapshotValue.store(value());
    }

    double snapshotValue() {
        return _snapshotValue.loadRelaxed();
    }

private:
    CallbackFn _callbackFn;
    mongo::AtomicWord<double> _snapshotValue{0};
};

// The comparator is `std::less_equal` rather than `std::less` because these metrics are
// exported to prometheus which expects the upper bound of the bucket to be inclusive.
using BaseHistogram = mongo::Histogram<int64_t, std::less_equal<int64_t>>;

// Histogram metric with user-defined buckets.
class Histogram : public Metric, public BaseHistogram {
public:
    struct Bucket {
        // Inclusive bound of the bucket. The prior bucket's upper bound
        // in the `_snapshot` list is the exclusive lower bound for this
        // bucket.
        //
        // This will only not be set for the +Infinity bucket, otherwise it
        // is expected for the upper bound to be set.
        boost::optional<int64_t> upper;

        // Count snapshot recorded when `takeSnapshot()` is invoked.
        std::atomic_int64_t count;
    };

    Histogram(std::string name,
              std::string description,
              LabelsVec labels,
              std::vector<int64_t> buckets);

    void takeSnapshot() override {
        for (size_t i = 0; i < _counts.size(); ++i) {
            _snapshot[i].count.store(_counts[i].load());
        }
    }

    std::vector<Bucket> snapshotValue() const {
        std::vector<Bucket> vec(_snapshot.size());
        for (size_t i = 0; i < _snapshot.size(); ++i) {
            vec[i].upper = _snapshot[i].upper;
            vec[i].count.store(_snapshot[i].count.load());
        }
        return vec;
    }

private:
    // Snapshot of each bucket count, this is recorded when `takeSnapshot()` is
    // invoked. The size of `_snapshot` is always the same as `_counts` in
    // `mongo::Histogram`. The last bucket, which has no upper bound set, represents
    // the +Inf catch-all bucket for observations that are larger than the largest
    // user-defined bucket.
    std::vector<Bucket> _snapshot;
};  // class Histogram

// Generates exponentially increasing histogram buckets.
std::vector<int64_t> makeExponentialValueBuckets(int64_t start, int64_t factor, int64_t count);

// Generates exponentially increasing duration histogram buckets.
template <typename Rep, typename Period>
std::vector<int64_t> makeExponentialDurationBuckets(
    mongo::stdx::chrono::duration<Rep, Period> start, int64_t factor, int64_t count) {
    return makeExponentialValueBuckets(
        mongo::stdx::chrono::duration_cast<mongo::stdx::chrono::milliseconds>(start).count(),
        factor,
        count);
}

// Generates linearly increasing histogram buckets.
std::vector<int64_t> makeLinearValueBuckets(int64_t start, int64_t width, int64_t count);

// Generates linearly increasing duration histogram buckets.
template <typename Rep, typename Period>
std::vector<int64_t> makeLinearDurationBuckets(mongo::stdx::chrono::duration<Rep, Period> start,
                                               int64_t width,
                                               int64_t count) {
    return makeLinearValueBuckets(
        mongo::stdx::chrono::duration_cast<mongo::stdx::chrono::milliseconds>(start).count(),
        width,
        count);
}

}  // namespace streams
