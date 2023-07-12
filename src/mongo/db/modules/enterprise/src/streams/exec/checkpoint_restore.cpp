/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/checkpoint_restore.h"

#include "streams/exec/message.h"
#include "streams/exec/parser.h"

namespace streams {

using namespace mongo;

std::unique_ptr<OperatorDag> CheckpointRestore::createDag(
    CheckpointId checkpointId,
    const std::vector<BSONObj>& startCommandBsonPipeline,
    const stdx::unordered_map<std::string, Connection>& connections) {
    // TODO(SERVER-78464): We shouldn't be re-parsing the user supplied BSON here to create the DAG.
    // Instead, we should re-parse from the exact plan stored in the checkpoint data.
    // TODO(SERVER-78464): Validate somewhere that the startCommandBsonPipeline is still the same.
    Parser parser(_context, {}, connections);
    return parser.fromBson(startCommandBsonPipeline);
}

void CheckpointRestore::restoreFromCheckpoint(OperatorDag* dag, CheckpointId checkpointId) {
    // Restore each operator from the checkpoint.
    for (auto& op : dag->operators()) {
        op->restoreFromCheckpoint(checkpointId);
    }
}

}  // namespace streams
