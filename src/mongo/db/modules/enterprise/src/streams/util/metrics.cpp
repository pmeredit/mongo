/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/util/metrics.h"

#include <cstddef>
#include <cstdio>
#include <fmt/format.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"

using namespace mongo;

namespace streams {

std::shared_ptr<Counter> CounterVec::withLabels(LabelValues extraLabelValues) {
    tassert(10092201,
            fmt::format("Provided {} label values but expected {}",
                        extraLabelValues.size(),
                        _extraLabelNames.size()),
            extraLabelValues.size() == _extraLabelNames.size());

    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto it = _countersByExtraLabelValues.find(extraLabelValues);

    if (it == _countersByExtraLabelValues.end()) {
        Metric::LabelsVec counterLabels{_baseLabels.begin(), _baseLabels.end()};
        for (size_t i = 0; i < _extraLabelNames.size(); i++) {
            counterLabels.push_back({_extraLabelNames[i], extraLabelValues[i]});
        }

        it = _countersByExtraLabelValues
                 .insert({std::move(extraLabelValues),
                          std::make_shared<Counter>(_name, _description, std::move(counterLabels))})
                 .first;
    }

    return it->second;
}

std::vector<std::shared_ptr<Counter>> CounterVec::getCounters() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    std::vector<std::shared_ptr<Counter>> counters;
    counters.reserve(_countersByExtraLabelValues.size());

    auto it = _countersByExtraLabelValues.begin();
    while (it != _countersByExtraLabelValues.end()) {
        counters.push_back(it->second);
        ++it;
    }

    return counters;
}

void CounterVec::takeSnapshot() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto it = _countersByExtraLabelValues.begin();
    while (it != _countersByExtraLabelValues.end()) {
        it->second->takeSnapshot();
        ++it;
    }
}

Histogram::Histogram(std::string name,
                     std::string description,
                     LabelsVec labels,
                     std::vector<int64_t> buckets)
    : Metric{std::move(name), std::move(description), std::move(labels)},
      BaseHistogram(std::move(buckets)),
      _snapshot(_counts.size()) {

    // `_counts` is one element larger than `_partitions` because the last item
    // in `_counts` is the catch-all bucket for +Inf.
    for (size_t i = 0; i < _partitions.size(); ++i) {
        _snapshot[i].upper = _partitions[i];
    }
}

std::vector<int64_t> makeExponentialValueBuckets(int64_t start, int64_t factor, int64_t count) {
    invariant(start > 0);
    invariant(count > 0);
    invariant(factor > 1);

    std::vector<int64_t> buckets;
    buckets.reserve(count);

    int64_t curr{start};
    for (size_t i = 0; i < static_cast<size_t>(count); ++i) {
        buckets.push_back(curr);
        curr *= factor;
    }

    return buckets;
}

std::vector<int64_t> makeLinearValueBuckets(int64_t start, int64_t width, int64_t count) {
    invariant(count > 0);
    invariant(width > 0);

    std::vector<int64_t> buckets;
    buckets.reserve(count);

    int64_t curr{start};
    for (size_t i = 0; i < static_cast<size_t>(count); ++i) {
        buckets.push_back(curr);
        curr += width;
    }

    return buckets;
}

};  // namespace streams
