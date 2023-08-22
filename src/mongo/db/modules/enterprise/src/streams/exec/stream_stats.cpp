/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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
    // Output docs/bytes are input docs/bytes for the sink.
    out.numOutputDocs = operatorStats.rbegin()->numInputDocs;
    out.numOutputBytes = operatorStats.rbegin()->numInputBytes;
    return out;
}

}  // namespace streams
