/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/util/metrics.h"

#include "mongo/util/assert_util.h"

using namespace mongo;

namespace streams {

Histogram::Histogram(std::vector<int64_t> buckets)
    : BaseHistogram(std::move(buckets)), _snapshot(_counts.size()) {

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
