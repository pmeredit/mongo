/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/checkpoint/restorer.h"

namespace streams {

// This class provides a Checkpoint Restorer implementation for a single checkpoint Id.
// This class is used by the checkpoint storage to restore operator states.
class CheckpointRestorer : public Restorer {
public:
    CheckpointRestorer(CheckpointId checkpointId,
                       Context* context,
                       OpsRangeMap opRanges,
                       FileChecksums fileChecksums,
                       std::filesystem::path restoreRootDir,
                       std::vector<mongo::CheckpointOperatorInfo> stats)
        : Restorer(checkpointId, context, restoreRootDir, stats),
          _opRanges{std::move(opRanges)},
          _fileChecksums{std::move(fileChecksums)} {}

    // When a state file is needed for the first time it is read in in its entirety and the
    // uncompressed contents are stored in _cachedStateFile. The checksum of the file is recomputed
    // and validated against the expected checksum recorded in the manifest at the time of saving
    // this checkpoint.
    const std::string& getStateFile(int fileIdx);
    void readStateFile(int fileIdx);

    // Returns the next record for this operator, if any
    boost::optional<mongo::Document> getNextRecord(OperatorId opId) override;

    // Read all state for this operator
    void markOperatorDone(OperatorId opId) override;

private:
    // This is an internal helper in charge of restoring the state of one operator. After reading
    // the manifest, an OpRestorer is initialized with the OpRanges for that operator. Operator
    // state may be spread across multiple files but one individual Document is always contained
    // entirely in one file. Further, within one file, the operator state is always present in a
    // contiguous range. One range within one file is hydrated on demand and then all Documents in
    // this range are yielded to the caller in each successive getNextRecord call. Caller is
    // expected to keep calling getNextRecord() till it returns boost::none
    struct OpRestorer {
        OpRestorer(OperatorId opId, OpStateRanges ranges, CheckpointRestorer* restorer);
        // Returns the next record for this operator if there is a next record
        boost::optional<mongo::Document> getNextRecord();

        OperatorId opId;
        OpStateRanges ranges;
        CheckpointRestorer* restorer = nullptr;
        size_t currRange = 0;
        std::unique_ptr<mongo::BufReader> bufReader;

    private:
        // Returns true if there are more records, either in this range or in the next range.
        // Internally advances the read-from point so as to advance to the next range if necessary
        bool hasMore();

        // Reads the file contents of the current range into _buf.
        void hydrateCurrentRange();
    };

    boost::optional<OpRestorer> _currOpRestorer;
    mongo::stdx::unordered_map<OperatorId, OpStateRanges> _opRanges;
    mongo::stdx::unordered_map<int, uint32_t> _fileChecksums;
    // We will read in the entire state file and keep it around as the operator ranges within this
    // file are restored
    boost::optional<std::pair<int, std::string>> _cachedStateFile;
};

}  // namespace streams
