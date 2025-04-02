/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/util/metrics.h"

#include <cstddef>

#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace streams {

namespace {

using namespace mongo;

TEST(MetricTest, CounterVec_ShouldProperlyIncrementACounter) {
    CounterVec counterVec{"counter1", "description1", {}, {"b"}};

    auto counter = counterVec.withLabels({"0"});

    counterVec.withLabels({"0"})->increment();
    ASSERT_EQ(counter->value(), 1);

    counterVec.withLabels({"0"})->increment(2);
    ASSERT_EQ(counter->value(), 3);
}

TEST(MetricTest, CounterVec_CorrectlyMaintainsCounters) {
    CounterVec counterVec{"counter1", "description1", {{"a", "0"}}, {"b", "c"}};

    counterVec.withLabels({"0", "0"});
    ASSERT_EQ(counterVec.getCounters().size(), 1);

    counterVec.withLabels({"0", "0"});
    ASSERT_EQ(counterVec.getCounters().size(), 1);

    counterVec.withLabels({"0", "1"});
    ASSERT_EQ(counterVec.getCounters().size(), 2);

    counterVec.withLabels({"1", "0"});
    ASSERT_EQ(counterVec.getCounters().size(), 3);

    counterVec.withLabels({"1", "1"});
    ASSERT_EQ(counterVec.getCounters().size(), 4);

    auto labels = counterVec.withLabels({"1", "1"})->getLabels();
    Metric::LabelsVec expectedLabels{{"a", "0"}, {"b", "1"}, {"c", "1"}};
    ASSERT_EQ(labels.size(), expectedLabels.size());
    for (size_t i = 0; i < expectedLabels.size(); i++) {
        ASSERT_EQ(labels[i], expectedLabels[i]);
    }
}

}  // namespace
}  // namespace streams
