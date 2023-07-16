/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/checkpoint_storage.h"

namespace streams {

CheckpointId CheckpointStorage::createCheckpointId() {
    return doCreateCheckpointId();
}

void CheckpointStorage::addState(CheckpointId checkpointId,
                                 OperatorId operatorId,
                                 mongo::BSONObj operatorState,
                                 int32_t chunkNumber) {
    invariant(chunkNumber >= 0);
    invariant(operatorId >= 0);
    invariant(checkpointId >= 0);
    doAddState(checkpointId, operatorId, std::move(operatorState), chunkNumber);
}

void CheckpointStorage::commit(CheckpointId checkpointId) {
    invariant(checkpointId >= 0);
    doCommit(checkpointId);
}

boost::optional<CheckpointId> CheckpointStorage::readLatestCheckpointId() {
    return doReadLatestCheckpointId();
}

boost::optional<mongo::BSONObj> CheckpointStorage::readState(CheckpointId checkpointId,
                                                             OperatorId operatorId,
                                                             int32_t chunkNumber) {
    invariant(chunkNumber >= 0);
    invariant(operatorId >= 0);
    invariant(checkpointId >= 0);
    return doReadState(checkpointId, operatorId, chunkNumber);
}

}  // namespace streams
