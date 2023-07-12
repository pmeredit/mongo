#pragma once

#include "streams/exec/operator_dag.h"
#include "streams/exec/stages_gen.h"

namespace streams {

struct Context;

/**
 * CheckpointRestore is used for restoring an OperatorDag from the checkpoint data
 * in storage.
 */
class CheckpointRestore {
public:
    CheckpointRestore(Context* context) : _context(context) {}

    /**
     * Create an OperatorDag from the checkpoint data.
     */
    std::unique_ptr<OperatorDag> createDag(
        CheckpointId checkpointId,
        const std::vector<mongo::BSONObj>& startCommandBsonPipeline,
        const mongo::stdx::unordered_map<std::string, mongo::Connection>& connections);

    /**
     * Restore the operators in an OperatorDag with their state in checkpointId.
     */
    void restoreFromCheckpoint(OperatorDag* dag, CheckpointId checkpointId);

private:
    Context* _context;
};

}  // namespace streams
