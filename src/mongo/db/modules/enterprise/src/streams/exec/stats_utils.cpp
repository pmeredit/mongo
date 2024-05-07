/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/stats_utils.h"

using namespace mongo;

namespace streams {

OperatorStats toOperatorStats(const mongo::OperatorStatsDoc& stats) {
    return OperatorStats{.operatorName = stats.getName().toString(),
                         .numInputDocs = stats.getInputDocs(),
                         .numInputBytes = stats.getInputBytes(),
                         .numOutputDocs = stats.getOutputDocs(),
                         .numOutputBytes = stats.getOutputBytes(),
                         .numDlqDocs = stats.getDlqDocs(),
                         .numDlqBytes = stats.getDlqBytes(),
                         .executionTime =
                             mongo::duration_cast<Microseconds>(stats.getExecutionTime())};
}

mongo::OperatorStatsDoc toOperatorStatsDoc(const OperatorStats& stats) {
    mongo::OperatorStatsDoc statsDoc;
    statsDoc.setName(stats.operatorName);
    statsDoc.setInputDocs(stats.numInputDocs);
    statsDoc.setInputBytes(stats.numInputBytes);
    statsDoc.setOutputDocs(stats.numOutputDocs);
    statsDoc.setOutputBytes(stats.numOutputBytes);
    statsDoc.setDlqDocs(stats.numDlqDocs);
    statsDoc.setDlqBytes(stats.numDlqBytes);
    statsDoc.setStateSize(stats.memoryUsageBytes);
    statsDoc.setExecutionTime(mongo::duration_cast<Milliseconds>(stats.executionTime));
    return statsDoc;
}

std::vector<OperatorStats> combineAdditiveStats(
    const std::vector<OperatorStats>& operatorStats,
    const std::vector<OperatorStats>& restoreCheckpointStats) {
    uassert(75920,
            "Unexpected number of stats in checkpoint",
            restoreCheckpointStats.size() == operatorStats.size());
    std::vector<OperatorStats> result;
    result.reserve(operatorStats.size());
    for (size_t i = 0; i < restoreCheckpointStats.size(); ++i) {
        uassert(75921,
                "Unexpected operator name in checkpoint stats",
                operatorStats[i].operatorName == restoreCheckpointStats[i].operatorName);
        auto stats = operatorStats[i];
        stats += restoreCheckpointStats[i].getAdditiveStats();
        result.push_back(stats);
    }
    return result;
}

}  // namespace streams
