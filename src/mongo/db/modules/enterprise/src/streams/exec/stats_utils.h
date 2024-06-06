/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/stream_stats.h"

namespace streams {

// Convert the stats IDL representation to the internal struct.
OperatorStats toOperatorStats(const mongo::OperatorStatsDoc& stats);

// Convert the internal struct to the stats IDL representation.
mongo::OperatorStatsDoc toOperatorStatsDoc(const OperatorStats& stats);

// Combines cumulative stats from the current execution and restore checkpoint's stats.
std::vector<OperatorStats> combineAdditiveStats(
    const std::vector<OperatorStats>& operatorStats,
    const std::vector<OperatorStats>& restoreCheckpointStats);

}  // namespace streams
