/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/duration.h"
#include "streams/exec/external_api_operator.h"
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

struct ComputeStreamSummaryStatsTestCase {
    const std::vector<OperatorStats>& operatorStats;
    StreamSummaryStats expectedStats;
};

TEST(StatsTest, ComputeStreamSummaryStatsTest) {
    ComputeStreamSummaryStatsTestCase tests[] = {
        {std::vector<OperatorStats>{
             {
                 .operatorName = "ChangeStreamConsumerOperator",
                 .numInputDocs = 2,
                 .numInputBytes = 32,
                 .numDlqDocs = 1,
                 .numDlqBytes = 16,
                 .memoryUsageBytes = 16,
                 .watermark = 1,
             },
             {
                 .operatorName = "KafkaEmitOperator",
                 .numOutputDocs = 1,
                 .numOutputBytes = 16,
                 .watermark = 1,
             },
         },
         StreamSummaryStats{
             .numInputDocs = 2,
             .numOutputDocs = 1,
             .numInputBytes = 32,
             .numOutputBytes = 16,
             .memoryUsageBytes = 16,
             .watermark = 1,
             .numDlqDocs = 1,
             .numDlqBytes = 16,
         }},
        {std::vector<OperatorStats>{
             {
                 .operatorName = "ChangeStreamConsumerOperator",
                 .numInputDocs = 3,
                 .numInputBytes = 32,
                 .numDlqDocs = 1,
                 .numDlqBytes = 16,
                 .memoryUsageBytes = 32,
                 .watermark = 1,
             },
             {
                 .operatorName = ExternalApiOperator::kName.toString(),
                 .numInputBytes = 16,
                 .numOutputBytes = 32,
                 .numDlqDocs = 1,
                 .numDlqBytes = 16,
             },
             {
                 .operatorName = "KafkaEmitOperator",
                 .numOutputDocs = 1,
                 .numOutputBytes = 16,
             },
         },
         StreamSummaryStats{
             .numInputDocs = 3,
             .numOutputDocs = 1,
             .numInputBytes = 48,
             .numOutputBytes = 48,
             .memoryUsageBytes = 32,
             .watermark = 1,
             .numDlqDocs = 2,
             .numDlqBytes = 32,
         }},
    };

    for (const auto& tc : tests) {
        auto result = computeStreamSummaryStats(tc.operatorStats);
        ASSERT_EQ(result.numInputDocs, tc.expectedStats.numInputDocs);
        ASSERT_EQ(result.numOutputDocs, tc.expectedStats.numOutputDocs);
        ASSERT_EQ(result.numInputBytes, tc.expectedStats.numInputBytes);
        ASSERT_EQ(result.numOutputBytes, tc.expectedStats.numOutputBytes);
        ASSERT_EQ(result.memoryUsageBytes, tc.expectedStats.memoryUsageBytes);
        ASSERT_EQ(result.watermark, tc.expectedStats.watermark);
        ASSERT_EQ(result.numDlqDocs, tc.expectedStats.numDlqDocs);
        ASSERT_EQ(result.numDlqBytes, tc.expectedStats.numDlqBytes);
    }

    // Return nothing if empty
    auto result = computeStreamSummaryStats(std::vector<OperatorStats>{});
    ASSERT_EQ(result.numInputDocs, 0);
    ASSERT_EQ(result.numOutputDocs, 0);
    ASSERT_EQ(result.numInputBytes, 0);
    ASSERT_EQ(result.numOutputBytes, 0);
    ASSERT_EQ(result.memoryUsageBytes, 0);
    ASSERT_EQ(result.watermark, -1);
    ASSERT_EQ(result.numDlqDocs, 0);
    ASSERT_EQ(result.numDlqBytes, 0);
}

}  // namespace streams
