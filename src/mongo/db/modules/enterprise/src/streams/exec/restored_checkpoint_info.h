/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <vector>

#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/exec_internal_gen.h"

namespace streams {

// Information about a restore checkpoint.
struct RestoredCheckpointInfo {
    // A serializable description of the restore checkpoint.
    mongo::CheckpointDescription description;
    // User pipeline in the restore checkpoint.
    std::vector<mongo::BSONObj> userPipeline;
    // Pipeline version in the restore checkpoint.
    int pipelineVersion{0};
    // Summary stats in the restore checkpoint.
    // TODO(STREAMS-950): Make this non-optional once all checkpoints >= version 4.
    boost::optional<mongo::CheckpointSummaryStats> summaryStats;
    // Operator level stats in the restore checkpoint.
    boost::optional<std::vector<mongo::CheckpointOperatorInfo>> operatorInfo;
    // The optimized execution plan in the restore checkpoint.
    std::vector<mongo::BSONObj> executionPlan;
    // Min window start time from the Replay checkpoint after modify
    boost::optional<int64_t> minWindowStartTime;
};

}  // namespace streams
