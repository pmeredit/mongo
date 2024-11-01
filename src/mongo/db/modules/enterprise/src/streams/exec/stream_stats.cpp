/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/stream_stats.h"

#include "mongo/util/assert_util.h"
#include "streams/exec/external_api_operator.h"

using namespace mongo;

namespace streams {

mongo::Microseconds calculateDecayTimeSpent(int64_t currentTimeSpentMicros,
                                            int64_t previousTimeSpentMicros,
                                            int64_t timeDeltaMillis) {
    if (previousTimeSpentMicros == 0) {
        return mongo::Microseconds{currentTimeSpentMicros};
    }
    const double theta = 0.00003;  // to keep weighting in the last 5 minutes = 300000ms
    auto alpha = std::exp(-1 * theta * timeDeltaMillis);

    return (mongo::Microseconds){
        (int64_t)(currentTimeSpentMicros * (1 - alpha) + previousTimeSpentMicros * (alpha))};
}

StreamSummaryStats computeStreamSummaryStats(const std::vector<OperatorStats>& operatorStats) {
    StreamSummaryStats out;
    if (operatorStats.empty()) {
        return out;
    }

    dassert(operatorStats.size() >= 2);
    out.numInputDocs = operatorStats.begin()->numInputDocs;
    out.numInputBytes = operatorStats.begin()->numInputBytes;
    out.numOutputDocs = operatorStats.rbegin()->numOutputDocs;
    out.numOutputBytes = operatorStats.rbegin()->numOutputBytes;
    out.watermark = operatorStats.begin()->watermark;

    for (const auto& s : operatorStats) {
        out.memoryUsageBytes += s.memoryUsageBytes;
        out.numDlqDocs += s.numDlqDocs;
        out.numDlqBytes += s.numDlqBytes;
        // We want to include bytes performed by the $externalApi even when not a source/sink
        if (s.operatorName == ExternalApiOperator::kName) {
            out.numInputBytes += s.numInputBytes;
            out.numOutputBytes += s.numOutputBytes;
        }
    }

    return out;
}

}  // namespace streams
