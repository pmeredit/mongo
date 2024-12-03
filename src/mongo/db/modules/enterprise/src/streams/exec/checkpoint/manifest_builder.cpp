/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/checkpoint/manifest_builder.h"

#include "mongo/bson/json.h"
#include "mongo/logv2/log.h"
#include "streams/exec/checkpoint/file_util.h"
#include "streams/exec/context.h"

using fspath = std::filesystem::path;
using namespace mongo;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

const int ManifestBuilder::kVersionWithNoExecutionPlan = 1;
const int ManifestBuilder::kVersion = 5;
const int ManifestBuilder::kVersionWithNoSummaryStats = 3;
const int ManifestBuilder::kVersionWithNoWindowReplayInfo = 4;
const int ManifestBuilder::kMinAllowedVersion = 1;

void ManifestBuilder::addOpRecord(OperatorId opId, int fileIdx, off_t begOffset, size_t recLen) {
    invariant(recLen);

    // Assume that if an operator exists it is at the end. This is ok since we are expecting
    // (and asserting on) operators to add their state one at a time.
    auto itr = _opRanges.end();
    if (!_opRanges.empty() && _opRanges.back().first == opId) {
        itr = std::prev(_opRanges.end());
    }

    if (itr == _opRanges.end()) {
        // The first time we see an operator, create an entry for it
        _opRanges.push_back({opId, {OpStateRange{fileIdx, begOffset, begOffset + (off_t)recLen}}});
        itr = std::prev(_opRanges.end());
    } else {
        // See if we need to add a new entry for the operator (for when we have created a
        // new state file)
        if (itr->second.back().stateFileIdx == fileIdx - 1) {
            invariant(begOffset == 0);
            itr->second.push_back(OpStateRange{fileIdx, begOffset, begOffset + (off_t)recLen});
        } else {
            invariant(itr->second.back().stateFileIdx == fileIdx);
            invariant(itr->second.back().end == begOffset);
            itr->second.back().extend(recLen);
        }
    }
}

void ManifestBuilder::addStateFileChecksum(int fileIdx, uint32_t checksum) {
    invariant(_stateFileChecksums.find(fileIdx) == _stateFileChecksums.end());
    _stateFileChecksums[fileIdx] = checksum;
}

void ManifestBuilder::writeToDisk(CheckpointMetadata metadata) {
    invariant(!_stateFileChecksums.empty());
    int currStateFileIdx = _stateFileChecksums.rbegin()->first;

    mongo::Manifest manifest;
    manifest.setVersion(kVersion);

    CheckpointFiles checkpointFiles;
    checkpointFiles.setChecksumAlgo("murmur3");
    std::vector<CheckpointFileInfo> svec;
    for (int i = 0; i <= currStateFileIdx; i++) {
        svec.push_back({getStateFileNameFromIdx(i), int64_t(_stateFileChecksums[i])});
    }
    checkpointFiles.setFiles(std::move(svec));
    manifest.setCheckpointFileList(std::move(checkpointFiles));

    // TODO(SERVER-83239) - Add missing required fields to metadata as per doc
    manifest.setMetadata(std::move(metadata));

    std::vector<OperatorCheckpointFileRanges> operatorRanges;
    for (auto& [opId, locs] : _opRanges) {
        std::vector<CheckpointFileRange> opLocs;
        for (auto& r : locs) {
            opLocs.push_back({getStateFileNameFromIdx(r.stateFileIdx),
                              static_cast<int32_t>(r.begin),
                              static_cast<int32_t>(r.end)});
        }
        operatorRanges.push_back({opId, std::move(opLocs)});
    }
    manifest.setOperatorCheckpointFileRanges(std::move(operatorRanges));

    // Now write the manifest object out. Just like in the state file case, we write to a different
    // filename first and then rename it to the eventual name to ensure that the manifest file is
    // visible atomically to an external process
    std::string shadowFilePath = getShadowFilePath(_manifestFilePath);
    BSONObj manifestObj = manifest.toBSON();

    // Store the checksum of the manifest file as a 4-byte preamble in the file itself.
    uint32_t mcrc = getChecksum32(manifestObj.objdata(), manifestObj.objsize());
    try {
        writeFile(shadowFilePath, manifestObj.objdata(), manifestObj.objsize(), mcrc);
    } catch (const DBException& e) {
        LOGV2_WARNING(7863425, "File I/O error", "msg"_attr = e.what(), "context"_attr = *_context);
        tasserted(7863426,
                  fmt::format("Could not write file: {}, msg: {}, context:{}",
                              shadowFilePath,
                              e.what(),
                              tojson(_context->toBSON())));
    }
    try {
        // Rename to original name
        std::filesystem::rename(shadowFilePath, _manifestFilePath);
    } catch (const DBException& e) {
        LOGV2_WARNING(7863435, "File I/O error", "msg"_attr = e.what(), "context"_attr = *_context);
        tasserted(7863418,
                  fmt::format("Could not rename: {} -> {} : [], context={}",
                              shadowFilePath,
                              _manifestFilePath.native(),
                              e.what(),
                              tojson(_context->toBSON())));
    }
}

}  // namespace streams
