#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/message.h"

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
     * Commit a checkpoint.
     */
    void commit(CheckpointId id);

    /**
     * Find the latest checkpoint ID for restore.
     */
    boost::optional<CheckpointId> readLatestCheckpointId();

    /**
     * Retrieve OperatorState for an operatorId in a checkpoint.
     */
    boost::optional<mongo::BSONObj> readState(CheckpointId checkpointId,
                                              OperatorId operatorId,
                                              int32_t chunkNumber);

protected:
    virtual CheckpointId doCreateCheckpointId() = 0;
    virtual void doAddState(CheckpointId checkpointId,
                            OperatorId operatorId,
                            mongo::BSONObj operatorState,
                            int32_t chunkNumber) = 0;
    virtual void doCommit(CheckpointId id) = 0;
    virtual boost::optional<CheckpointId> doReadLatestCheckpointId() = 0;
    virtual boost::optional<mongo::BSONObj> doReadState(CheckpointId checkpointId,
                                                        OperatorId operatorId,
                                                        int32_t chunkNumber) = 0;
};

}  // namespace streams
