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
    void addState(const CheckpointId& checkpointId,
                  OperatorId operatorId,
                  std::vector<mongo::BSONObj> operatorState);

    /**
     * Commit a checkpoint.
     */
    void commit(const CheckpointId& id);

    /**
     * Find the latest checkpoint ID for restore.
     */
    boost::optional<CheckpointId> readLatestCheckpointId();

    /**
     * Retrieve OperatorState for an operatorId in a checkpoint.
     */
    std::vector<mongo::BSONObj> readState(const CheckpointId& checkpointId, OperatorId operatorId);

protected:
    virtual CheckpointId doCreateCheckpointId() = 0;
    virtual void doAddState(const CheckpointId& checkpointId,
                            OperatorId operatorId,
                            std::vector<mongo::BSONObj> operatorState) = 0;
    virtual void doCommit(const CheckpointId& id) = 0;
    virtual boost::optional<CheckpointId> doReadLatestCheckpointId() = 0;
    virtual std::vector<mongo::BSONObj> doReadState(const CheckpointId& checkpointId,
                                                    OperatorId operatorId) = 0;
};

}  // namespace streams
