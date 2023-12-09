#pragma once

#include <absl/container/inlined_vector.h>
#include <filesystem>

#include "mongo/bson/util/builder.h"
#include "mongo/util/time_support.h"

#include "streams/exec/checkpoint_storage.h"
#include "streams/util/units.h"

namespace streams {

// The manifest of a checkpoint has some meta information about the checkpoint data. It tracks the
// locations in the different state files where the operator states were persisted. It also
// maintains file checksums for integrity checks when restoring from a checkpoint The manifest is
// maintained in memory as the operators write their state. In the end, when a checkpoint commit is
// triggered, the manifest is serialized and written to disk in a separate manifest file

class ManifestBuilder {
public:
    // This is one contiguous range within a file that holds 1 or more Documents for the same
    // operator
    struct OpStateRange {
        int stateFileIdx;
        // Closed-Open range
        off_t begin;
        off_t end;

        OpStateRange(int stateFileIdx_, off_t begin_, off_t end_)
            : stateFileIdx{stateFileIdx_}, begin{begin_}, end{end_} {}

        void extend(uint32_t len) {
            end += len;
        }

        size_t len() const {
            return end - begin;
        }
    };

    // OpStateRanges has all the ranges where an operator state is present. For many operators, they
    // will be all contained within one state file and so will have only one OpRange as their state.
    // But large operators will span multiple files and so can have several ranges. Note that within
    // one file, an operator is always present in one contigous span i.e. there will be atmost one
    // OpRange per stateFileIdx
    using OpStateRanges = absl::InlinedVector<OpStateRange, 1>;

    static const int kVersion;

    ManifestBuilder(CheckpointId checkpointId,
                    std::string streamProcessorId,
                    std::string tenantId,
                    std::filesystem::path manifestFilePath,
                    mongo::Date_t checkpointStartTime)
        : _checkpointId{checkpointId},
          _streamProcessorId{std::move(streamProcessorId)},
          _tenantId{std::move(tenantId)},
          _manifestFilePath{std::move(manifestFilePath)},
          _checkpointStartTime{checkpointStartTime} {}
    // This function gets called when an operator provides a new state record of size recLen. It
    // 1) create a new Range entry for this operator if needed (either operator is seen for the
    // first time or operator state needs to be added to a new state file). 2) If the last range
    // entry in the current state file is already for this operator, we simply extend that range
    void addOpRecord(OperatorId opId, int fileIdx, off_t begOffset, size_t recLen);
    void addStateFileChecksum(int fileIdx, uint32_t checksum);
    void writeToDisk();

private:
    // The opRanges for each OperatorId.
    std::vector<std::pair<OperatorId, OpStateRanges>> _opRanges;

    // The state file checksums for each file
    std::map<int, uint32_t> _stateFileChecksums;

    CheckpointId _checkpointId;
    std::string _streamProcessorId;
    std::string _tenantId;
    std::filesystem::path _manifestFilePath;
    mongo::Date_t _checkpointStartTime;
};

}  // namespace streams
