/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "streams/exec/checkpoint/restorer.h"

namespace streams {

// This class provides a Checkpoint Restorer implementation for a single replay checkpoint after
// modify. This class is used by the checkpoint storage to restore the Source operator from a Replay
// checkpoint.
class ReplayCheckpointRestorer : public Restorer {
public:
    ReplayCheckpointRestorer(CheckpointId checkpointId,
                             Context* context,
                             mongo::BSONObj replaySourceState,
                             std::filesystem::path restoreRootDir,
                             std::vector<mongo::CheckpointOperatorInfo> stats)
        : Restorer(checkpointId, context, restoreRootDir, stats),
          _replaySourceState{replaySourceState} {}

    boost::optional<mongo::Document> getNextRecord(OperatorId opId) override;

private:
    mongo::BSONObj _replaySourceState;
};

}  // namespace streams
