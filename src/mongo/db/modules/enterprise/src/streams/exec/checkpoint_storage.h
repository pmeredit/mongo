#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"

namespace streams {

/**
 * CheckpointStorage is used during checkpoint operations to create and commit checkpoints,
 * and save and retrieve state. There is a CheckpointStorage instance per streamProcessor in its
 * Context.
 */
class CheckpointStorage {
public:
    virtual ~CheckpointStorage() = default;

    /**
     * Create a new checkpoint.
     */
    CheckpointId createCheckpointId();

    /**
     * Add BSON state for an operatorId to a checkpoint.
     * Safe to call multiple times. operatorState can be any size, the
     * implementation will handle chunking if required.
     */
    void addState(CheckpointId checkpointId,
                  OperatorId operatorId,
                  mongo::BSONObj operatorState,
                  int32_t chunkNumber);

    /**
     * Adds to an operator's stats for a checkpoint.
     */
    void addStats(CheckpointId checkpointId, OperatorId operatorId, const OperatorStats& stats);

    /**
     * Commit a checkpoint.
     */
    void commit(CheckpointId id);

    /**
     * Find the latest checkpoint ID for restore. If boost::none is
     * returned, no committed checkpoint exists for this streamProcessor.
     */
    boost::optional<CheckpointId> readLatestCheckpointId();

    /**
     * Retrieve OperatorState for an operatorId in a checkpoint.
     */
    boost::optional<mongo::BSONObj> readState(CheckpointId checkpointId,
                                              OperatorId operatorId,
                                              int32_t chunkNumber);

    /**
     * Return the CheckpointInfo document containing metadata and ID for a checkpoint.
     */
    boost::optional<mongo::CheckpointInfo> readCheckpointInfo(CheckpointId checkpointId);

protected:
    virtual CheckpointId doCreateCheckpointId() = 0;
    virtual void doAddState(CheckpointId checkpointId,
                            OperatorId operatorId,
                            mongo::BSONObj operatorState,
                            int32_t chunkNumber) = 0;
    virtual void doCommit(CheckpointId id, mongo::CheckpointInfo checkpointInfo) = 0;
    virtual boost::optional<CheckpointId> doReadLatestCheckpointId() = 0;
    virtual boost::optional<mongo::BSONObj> doReadState(CheckpointId checkpointId,
                                                        OperatorId operatorId,
                                                        int32_t chunkNumber) = 0;
    virtual boost::optional<mongo::CheckpointInfo> doReadCheckpointInfo(
        CheckpointId checkpointId) = 0;

private:
    // _stats to track per-operator stats for ongoing checkpoints.
    mongo::stdx::unordered_map<CheckpointId, std::map<OperatorId, OperatorStats>> _stats;
};

}  // namespace streams
