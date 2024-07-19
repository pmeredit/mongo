/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/duration.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

TEST(StatsTest, StatsDecayTest) {
    OperatorStats stats;

    // Tests to demonstrate the decay function.
    auto out = calculateDecayTimeSpent(0, 1000, 1);
    ASSERT_EQ(out.count(), 999);

    out = calculateDecayTimeSpent(1000, 1000, 300000);
    ASSERT_EQ(out.count(), 1000);

    out = calculateDecayTimeSpent(0, 1000, 1000);
    ASSERT_EQ(out.count(), 970);

    out = calculateDecayTimeSpent(0, 1000, 23093);
    ASSERT_EQ(out.count(), 500);

    out = calculateDecayTimeSpent(0, 1000, 46203);
    ASSERT_EQ(out.count(), 250);

    out = calculateDecayTimeSpent(0, 1000, 150000);
    ASSERT_EQ(out.count(), 11);

    out = calculateDecayTimeSpent(0, 1000, 300000);
    ASSERT_EQ(out.count(), 0);

    out = calculateDecayTimeSpent(0, 1000, 3000000);
    ASSERT_EQ(out.count(), 0);

    out = calculateDecayTimeSpent(1000, 0, 3000000);
    ASSERT_EQ(out.count(), 1000);
}

TEST(StatsTest, OperatorStatsDecayTest) {
    OperatorStats stats;

    stats += {.timeSpent = mongo::Microseconds{10000}};
    ASSERT_EQ(stats.timeSpent.count(), 10000);
    sleep(1);
    stats += {.timeSpent = mongo::Microseconds{5000}};
    ASSERT_GT(stats.timeSpent.count(), 5000);
    double alpha = exp(-0.00003 * 1000);
    auto calculatedValue = (int64_t)(10000 * alpha + 5000 * (1 - alpha));
    ASSERT_LTE(stats.timeSpent.count(), calculatedValue);
}

}  // namespace streams
