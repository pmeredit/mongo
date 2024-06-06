/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/util/duration.h"
#include <filesystem>

#include "streams/exec/checkpoint/manifest_builder.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"

namespace streams {

// This is an internal class in charge of restoring the state of one checkpoint
class Restorer {
public:
    using OpStateRanges = ManifestBuilder::OpStateRanges;
    using OpsRangeMap = mongo::stdx::unordered_map<OperatorId, OpStateRanges>;
    using FileChecksums = mongo::stdx::unordered_map<int, uint32_t>;

    Restorer(CheckpointId checkpointId,
             Context* context,
             OpsRangeMap opRanges,
             FileChecksums fileChecksums,
             std::filesystem::path restoreRootDir,
             std::vector<mongo::CheckpointOperatorInfo> stats)
        : _checkpointId{checkpointId},
          _context{context},
          _restoreRootDir{std::move(restoreRootDir)},
          _opRanges{std::move(opRanges)},
          _fileChecksums{std::move(fileChecksums)},
          _stats(std::move(stats)) {}

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

    // When a state file is needed for the first time it is read in in its entirety and the
    // uncompressed contents are stored in _cachedStateFile. The checksum of the file is recomputed
    // and validated against the expected checksum recorded in the manifest at the time of saving
    // this checkpoint.
    const std::string& getStateFile(int fileIdx);
    void readStateFile(int fileIdx);

    // Returns the next record for this operator, if any
    boost::optional<mongo::Document> getNextRecord(OperatorId opId);

    // Read all state for this operator
    void markOperatorDone(OperatorId opId);

private:
    // This is an internal helper in charge of restoring the state of one operator. After reading
    // the manifest, an OpRestorer is initialized with the OpRanges for that operator. Operator
    // state may be spread across multiple files but one individual Document is always contained
    // entirely in one file. Further, within one file, the operator state is always present in a
    // contiguous range. One range within one file is hydrated on demand and then all Documents in
    // this range are yielded to the caller in each successive getNextRecord call. Caller is
    // expected to keep calling getNextRecord() till it returns boost::none
    struct OpRestorer {
        OpRestorer(OperatorId opId, OpStateRanges ranges, Restorer* restorer);
        // Returns the next record for this operator if there is a next record
        boost::optional<mongo::Document> getNextRecord();

        OperatorId opId;
        OpStateRanges ranges;
        Restorer* restorer = nullptr;
        size_t currRange = 0;
        std::unique_ptr<mongo::BufReader> bufReader;

    private:
        // Returns true if there are more records, either in this range or in the next range.
        // Internally advances the read-from point so as to advance to the next range if necessary
        bool hasMore();

        // Reads the file contents of the current range into _buf.
        void hydrateCurrentRange();
    };

    CheckpointId _checkpointId;
    Context* _context{nullptr};
    std::filesystem::path _restoreRootDir;
    boost::optional<OpRestorer> _currOpRestorer;
    mongo::stdx::unordered_map<OperatorId, OpStateRanges> _opRanges;
    mongo::stdx::unordered_map<int, uint32_t> _fileChecksums;
    // We will read in the entire state file and keep it around as the operator ranges within this
    // file are restored
    boost::optional<std::pair<int, std::string>> _cachedStateFile;
    // Operator stats in the checkpoint, ordered by operatorId.
    std::vector<mongo::CheckpointOperatorInfo> _stats;
};

}  // namespace streams
