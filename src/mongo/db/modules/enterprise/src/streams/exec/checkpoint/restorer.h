/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <filesystem>

#include "streams/exec/checkpoint/manifest_builder.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"

namespace streams {

// This is an Abstract class used by checkpoint storage to read/retore state of one checkpoint.
class Restorer {
public:
    using OpStateRanges = ManifestBuilder::OpStateRanges;
    using OpsRangeMap = mongo::stdx::unordered_map<OperatorId, OpStateRanges>;
    using FileChecksums = mongo::stdx::unordered_map<int, uint32_t>;

    Restorer(CheckpointId checkpointId,
             Context* context,
             std::filesystem::path restoreRootDir,
             std::vector<mongo::CheckpointOperatorInfo> stats)
        : _checkpointId{checkpointId},
          _context{context},
          _restoreRootDir{std::move(restoreRootDir)},
          _stats(std::move(stats)) {}

    virtual ~Restorer() = default;

    const std::filesystem::path& restoreRootDir() const {
        return _restoreRootDir;
    }

    const std::string& getStreamProcessorId() const {
        return _context->streamProcessorId;
    }

    const std::vector<mongo::CheckpointOperatorInfo>& getStats() const {
        return _stats;
    }

    CheckpointId getCheckpointId() const {
        return _checkpointId;
    }

    // Returns the next record for this operator, if any
    virtual boost::optional<mongo::Document> getNextRecord(OperatorId opId) = 0;

    // Read all state for this operator
    virtual void markOperatorDone(OperatorId opId) {}

protected:
    Context* getContext() {
        return _context;
    }

private:
    CheckpointId _checkpointId;
    Context* _context{nullptr};
    std::filesystem::path _restoreRootDir;
    // Operator stats in the checkpoint, ordered by operatorId.
    std::vector<mongo::CheckpointOperatorInfo> _stats;
};

}  // namespace streams
