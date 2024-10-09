/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/stream_stats.h"

namespace streams {

// Convert the stats IDL representation to the internal struct.
OperatorStats toOperatorStats(const mongo::OperatorStatsDoc& stats);

// Convert the internal struct to the stats IDL representation.
mongo::OperatorStatsDoc toOperatorStatsDoc(const OperatorStats& stats);

// Convert the internal struct to the checkpoint summary stats IDL.
mongo::CheckpointSummaryStats toSummaryStatsDoc(const StreamSummaryStats& stats);

// Convert the IDL checkpoint summary stats to the internal struct.
StreamSummaryStats toSummaryStats(const mongo::CheckpointSummaryStats& stats);

// Combines operator stats from the current execution and restore checkpoint's stats.
std::vector<OperatorStats> combineAdditiveStats(
    const std::vector<OperatorStats>& operatorStats,
    const std::vector<OperatorStats>& restoreCheckpointStats);

// Converts a vector of OperatorStats into a vector of CheckpointOperatorInfo.
std::vector<mongo::CheckpointOperatorInfo> toCheckpointOpInfo(
    const std::vector<OperatorStats>& operatorStats);

// Converts a vector of CheckpointOperatorInfo into a vector of OperatorStats.
std::vector<OperatorStats> toOperatorStats(
    const std::vector<mongo::CheckpointOperatorInfo>& restoreCheckpointOpInfo);

}  // namespace streams
