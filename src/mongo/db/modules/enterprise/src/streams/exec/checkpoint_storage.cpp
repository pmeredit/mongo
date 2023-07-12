/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/checkpoint_storage.h"

namespace streams {

CheckpointId CheckpointStorage::createCheckpointId() {
    return doCreateCheckpointId();
}

void CheckpointStorage::addState(const CheckpointId& checkpointId,
                                 OperatorId operatorId,
                                 std::vector<mongo::BSONObj> operatorState) {
    doAddState(checkpointId, operatorId, std::move(operatorState));
}

void CheckpointStorage::commit(const CheckpointId& id) {
    doCommit(id);
}

boost::optional<CheckpointId> CheckpointStorage::readLatestCheckpointId() {
    return doReadLatestCheckpointId();
}

std::vector<mongo::BSONObj> CheckpointStorage::readState(const CheckpointId& checkpointId,
                                                         OperatorId operatorId) {
    return doReadState(checkpointId, operatorId);
}

}  // namespace streams
