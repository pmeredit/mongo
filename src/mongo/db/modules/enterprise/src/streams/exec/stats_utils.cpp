/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/stats_utils.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/stream_stats.h"

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

mongo::CheckpointSummaryStats toSummaryStatsDoc(const StreamSummaryStats& stats) {
    mongo::CheckpointSummaryStats statsDoc;
    statsDoc.setInputMessageCount(stats.numInputDocs);
    statsDoc.setInputMessageSize(stats.numInputBytes);
    statsDoc.setOutputMessageCount(stats.numOutputDocs);
    statsDoc.setOutputMessageSize(stats.numOutputBytes);
    statsDoc.setDlqMessageCount(stats.numDlqDocs);
    statsDoc.setDlqMessageSize(stats.numDlqBytes);
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

std::vector<mongo::CheckpointOperatorInfo> toCheckpointOpInfo(
    const std::vector<OperatorStats>& operatorStats) {
    std::vector<CheckpointOperatorInfo> checkpointOpInfo;
    checkpointOpInfo.reserve(operatorStats.size());
    for (size_t opId = 0; opId < operatorStats.size(); ++opId) {
        checkpointOpInfo.push_back(
            CheckpointOperatorInfo{int(opId), toOperatorStatsDoc(operatorStats[opId])});
    }
    return checkpointOpInfo;
}

std::vector<OperatorStats> toOperatorStats(
    const std::vector<mongo::CheckpointOperatorInfo>& restoreCheckpointOpInfo) {
    std::vector<OperatorStats> result;
    result.reserve(restoreCheckpointOpInfo.size());
    for (const auto& op : restoreCheckpointOpInfo) {
        result.push_back(toOperatorStats(op.getStats()));
    }
    return result;
}

StreamSummaryStats toSummaryStats(const mongo::CheckpointSummaryStats& stats) {
    return StreamSummaryStats{.numInputDocs = stats.getInputMessageCount(),
                              .numOutputDocs = stats.getOutputMessageCount(),
                              .numInputBytes = stats.getInputMessageSize(),
                              .numOutputBytes = stats.getOutputMessageSize(),
                              .numDlqDocs = stats.getDlqMessageCount(),
                              .numDlqBytes = stats.getDlqMessageSize()};
}

}  // namespace streams
