/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/stream_stats.h"

#include "mongo/util/assert_util.h"

using namespace mongo;

namespace streams {

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
    }

    return out;
}

}  // namespace streams
