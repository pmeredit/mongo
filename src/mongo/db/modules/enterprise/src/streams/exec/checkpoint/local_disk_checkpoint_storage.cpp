/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"

#include <atomic>
#include <boost/optional/optional.hpp>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <regex>

#include <snappy.h>

#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/future.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"
#include "streams/exec/checkpoint/file_util.h"
#include "streams/exec/checkpoint/manifest_builder.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"

using namespace std::chrono_literals;
namespace fs = std::filesystem;
using fspath = std::filesystem::path;
using namespace mongo;
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {
// E.g. Given a file like "/path/to/state00064.bin[.sz]", returns 64
boost::optional<int> getStateFileIdxFromName(const std::string& fname) {
    // hardcoded to assume "state{:04}.bin" format
    static std::regex re{R"(state(\d+).bin)"};
    std::smatch pattern;
    if (fname.length() == 14 && std::regex_search(fname, pattern, re)) {
        int fidx = std::stoi(pattern[1]);
        tassert(
            7863408, fmt::format("Invalid fidx={}", fidx), fidx >= 0 && fidx < kMaxStateFileIdx);
        return fidx;
    } else {
        LOGV2_WARNING(7863409, "Could not extract file idx from file name: ", "fname"_attr = fname);
        return boost::none;
    }
}

}  // namespace

LocalDiskCheckpointStorage::LocalDiskCheckpointStorage(Options opts, const Context* ctxt)
    : _opts{std::move(opts)},
      _tenantId{ctxt->tenantId},
      _streamName{ctxt->streamName},
      _streamProcessorId{ctxt->streamProcessorId} {}

bool LocalDiskCheckpointStorage::isActiveCheckpoint(CheckpointId chkId) const {
    return _activeCheckpointSave && _activeCheckpointSave->checkpointId == chkId;
}

bool LocalDiskCheckpointStorage::isCheckpointBeingRestored(CheckpointId chkId) const {
    return _activeRestorer && _activeRestorer->getCheckpointId() == chkId;
}

CheckpointId LocalDiskCheckpointStorage::doStartCheckpoint() {
    invariant(!_activeCheckpointSave);

    CheckpointId next = Date_t::now().toMillisSinceEpoch();
    fspath dir = _opts.writeRootDir / _streamProcessorId / std::to_string(next);
    invariant(!std::filesystem::exists(dir));

    std::filesystem::create_directories(dir);

    _activeCheckpointSave = ActiveCheckpointSave{
        .checkpointId = next,
        .manifest = ManifestBuilder{next,
                                    _streamProcessorId,
                                    _tenantId,
                                    getManifestFilePath(writeRootDir(), _streamProcessorId, next),
                                    Date_t::now()}};
    return next;
}

void LocalDiskCheckpointStorage::doAppendRecord(WriterHandle* writer, mongo::Document doc) {
    CheckpointId chkId = writer->getCheckpointId();
    OperatorId opId = writer->getOperatorId();

    invariant(isActiveCheckpoint(chkId),
              fmt::format("Checkpoint - {}/{} - is not active!", _streamProcessorId, chkId));
    invariant(isActiveWriter(chkId, opId),
              fmt::format("Invalid writer - {}/{}/{}!", _streamProcessorId, chkId, opId));

    if (!_activeCheckpointSave->stateFileBuf) {
        _activeCheckpointSave->stateFileBuf =
            std::make_unique<BufBuilder>(_opts.maxStateFileSizeHint * 2);
    }

    // We will know the length of the serialized doc only after serializing it
    size_t beforeLen = _activeCheckpointSave->stateFileBuf->len();
    doc.serializeForSorter(*_activeCheckpointSave->stateFileBuf);
    size_t afterLen = _activeCheckpointSave->stateFileBuf->len();
    size_t docLen = afterLen - beforeLen;

    // Now update the operator state range info
    _activeCheckpointSave->manifest.addOpRecord(opId,
                                                _activeCheckpointSave->currStateFileIdx,
                                                _activeCheckpointSave->currStateFileOffset,
                                                docLen);

    _activeCheckpointSave->currStateFileOffset += docLen;

    // See if we have crossed the soft limit and if so, write curr state file to disk
    if ((size_t)_activeCheckpointSave->currStateFileOffset >= _opts.maxStateFileSizeHint) {
        writeActiveStateFileToDisk();
    }
}

void LocalDiskCheckpointStorage::writeActiveStateFileToDisk() {
    invariant(_activeCheckpointSave);
    if (!_activeCheckpointSave->stateFileBuf) {
        // This can happen if none of the operators actually added any state
        return;
    }

    // compress/checksum the file
    std::string compressed;
    snappy::Compress(_activeCheckpointSave->stateFileBuf->buf(),
                     _activeCheckpointSave->stateFileBuf->len(),
                     &compressed);
    uint32_t checksum = getChecksum32(compressed.data(), compressed.length());

    fspath stateFilePath = getStateFilePath(_opts.writeRootDir,
                                            _streamProcessorId,
                                            _activeCheckpointSave->checkpointId,
                                            _activeCheckpointSave->currStateFileIdx,
                                            ".sz");

    fspath shadowPath = getShadowFilePath(stateFilePath);

    // write it to disk. We write to a different filename first and then rename it to the eventual
    // name to ensure that the file is visible atomically to an external process
    writeFile(shadowPath, compressed.data(), compressed.length(), boost::none);

    // Rename to eventual name
    try {
        std::filesystem::rename(shadowPath, stateFilePath);
    } catch (const std::exception&) {
        tasserted(
            7863410,
            fmt::format("Could not rename: {} -> {}", shadowPath.native(), stateFilePath.native()));
    }

    // Store compressed file checksum in manifest
    _activeCheckpointSave->manifest.addStateFileChecksum(_activeCheckpointSave->currStateFileIdx,
                                                         checksum);

    // Advance to next state file
    ++_activeCheckpointSave->currStateFileIdx;
    _activeCheckpointSave->currStateFileOffset = 0;
    _activeCheckpointSave->stateFileBuf.reset();
}

void LocalDiskCheckpointStorage::doCommitCheckpoint(CheckpointId chkId) {
    invariant(isActiveCheckpoint(chkId));
    invariant(!hasActiveWriter(chkId));

    // write the state file if needed
    writeActiveStateFileToDisk();

    // Write the manifest
    _activeCheckpointSave->manifest.writeToDisk();

    // Reset ActiveSaver
    _activeCheckpointSave.reset();

    // clean up _finalized writers
    for (auto itr = _finalizedWriters.begin(); itr != _finalizedWriters.end();) {
        if (std::get<0>(*itr) == chkId) {
            _finalizedWriters.erase(itr++);
        }
    }
}

std::unique_ptr<CheckpointStorage::WriterHandle> LocalDiskCheckpointStorage::doCreateStateWriter(
    CheckpointId chkId, OperatorId opId) {
    invariant(isActiveCheckpoint(chkId));
    invariant(!hasActiveWriter(chkId));
    invariant(!isFinalizedWriter(chkId, opId));
    WriterHandle::Options opts{this, chkId, opId};
    auto ret = std::unique_ptr<WriterHandle>(new WriterHandle{opts});
    _activeWriter = std::make_pair(chkId, opId);
    return ret;
}

void LocalDiskCheckpointStorage::doCloseStateWriter(WriterHandle* writer) {
    CheckpointId chkId = writer->getCheckpointId();
    OperatorId opId = writer->getOperatorId();
    invariant(isActiveCheckpoint(chkId));
    invariant(isActiveWriter(chkId, opId));
    invariant(!isFinalizedWriter(chkId, opId));
    _activeWriter = boost::none;
    _finalizedWriters.insert({chkId, opId});
}

bool LocalDiskCheckpointStorage::validateManifest(const mongo::Manifest& manifest) const {
    int version = manifest.getVersion();
    tassert(
        7863411,
        fmt::format("version mismatch!, expected/actual={}/{}", ManifestBuilder::kVersion, version),
        ManifestBuilder::kVersion == version);

    return true;
}

auto LocalDiskCheckpointStorage::getManifestInfo(const fspath& manifestFile)
    -> std::pair<OpsRangeMap, FileChecksums> {
    OpsRangeMap opRanges;
    mongo::stdx::unordered_map<int, uint32_t> checksums;

    std::string buf = readFile(manifestFile.native());

    // Validate that the checksum stored as a 4 byte preamble matches the computed checksum
    uint32_t mcrc = *(uint32_t*)buf.data();
    uint32_t computedChecksum = getChecksum32(buf.data() + 4, buf.size() - 4);
    tassert(7863412,
            fmt::format("manifest file checksum mismatch. {}/{}", mcrc, computedChecksum),
            mcrc == computedChecksum);

    // checksum matches. Now construct the manifest BSONObj
    BSONObj manifestObj{&buf[0] + 4};

    auto manifest =
        mongo::Manifest::parseOwned(IDLParserContext{"Manifest"}, manifestObj.getOwned());

    // Some validity checks at a logical level
    if (!validateManifest(manifest)) {
        tasserted(7863413, "could not validate manifest");
        return {{}, {}};
    }

    // TODO(SERVER-83239): For now assume that checkpointFileList and operatorRanges are always
    // present and ignore metadata
    for (auto& fInfo : manifest.getCheckpointFileList()->getFiles()) {
        std::string fName = fInfo.getName().toString();
        uint32_t checksum = (uint32_t)fInfo.getChecksum();
        boost::optional<int> fidx = getStateFileIdxFromName(fName);
        tassert(7863414, fmt::format("Could not get file idx from state file - {}", fName), fidx);
        tassert(7863415,
                fmt::format("Duplicate file idx - {}", *fidx),
                checksums.find(*fidx) == checksums.end());

        checksums[*fidx] = (uint32_t)checksum;
    }

    for (auto& e : *manifest.getOperatorCheckpointFileRanges()) {
        absl::InlinedVector<ManifestBuilder::OpStateRange, 1> ranges;
        for (auto& loc : e.getFileRanges()) {
            std::string fName = loc.getFile().toString();
            auto fidx = getStateFileIdxFromName(fName);
            tassert(7863416, fmt::format("Could not convert filename to idx: {}", fName), fidx);
            off_t beg = loc.getBegin();
            off_t end = loc.getEnd();
            ranges.push_back(ManifestBuilder::OpStateRange{*fidx, beg, end});
        }
        tassert(7863417,
                "Multiple entries found for operator!",
                opRanges.find(e.getOpid()) == opRanges.end());
        opRanges[e.getOpid()] = std::move(ranges);
    }
    return {opRanges, checksums};
}

void LocalDiskCheckpointStorage::doStartCheckpointRestore(CheckpointId chkId) {
    invariant(!_activeRestorer);
    fspath manifestFile = getManifestFilePath(_opts.restoreRootDir, _streamProcessorId, chkId);
    auto [opsRangeMap, fileChecksums] = getManifestInfo(manifestFile);
    _activeRestorer = std::make_unique<Restorer>(chkId,
                                                 _streamProcessorId,
                                                 std::move(opsRangeMap),
                                                 std::move(fileChecksums),
                                                 _opts.restoreRootDir);
}

std::unique_ptr<CheckpointStorage::ReaderHandle> LocalDiskCheckpointStorage::doCreateStateReader(
    CheckpointId chkId, OperatorId opId) {
    invariant(isCheckpointBeingRestored(chkId));
    invariant(!hasActiveReader(chkId));
    invariant(!isFinalizedReader(chkId, opId));
    ReaderHandle::Options opts{this, chkId, opId};
    auto ret = std::unique_ptr<ReaderHandle>(new ReaderHandle{opts});
    _activeReader = std::make_pair(chkId, opId);
    return ret;
}

void LocalDiskCheckpointStorage::doCloseStateReader(CheckpointStorage::ReaderHandle* reader) {
    CheckpointId chkId = reader->getCheckpointId();
    OperatorId opId = reader->getOperatorId();
    invariant(isCheckpointBeingRestored(chkId));
    invariant(isActiveReader(chkId, opId));
    invariant(!isFinalizedReader(chkId, opId));
    _activeReader = boost::none;
    _finalizedReaders.insert({chkId, opId});

    invariant(_activeRestorer);
    _activeRestorer->markOperatorDone(opId);
}

void LocalDiskCheckpointStorage::doMarkCheckpointRestored(CheckpointId chkId) {
    invariant(isCheckpointBeingRestored(chkId));
    invariant(!hasActiveReader(chkId));
    _activeRestorer.reset();

    // clean up _finalized readers
    for (auto itr = _finalizedReaders.begin(); itr != _finalizedReaders.end();) {
        if (std::get<0>(*itr) == chkId) {
            _finalizedReaders.erase(itr++);
        }
    }
}

boost::optional<mongo::Document> LocalDiskCheckpointStorage::doGetNextRecord(ReaderHandle* reader) {
    CheckpointId chkId = reader->getCheckpointId();
    OperatorId opId = reader->getOperatorId();
    invariant(_activeRestorer && _activeRestorer->getCheckpointId() == chkId);
    return _activeRestorer->getNextRecord(opId);
}

}  // namespace streams
