/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"

#include <boost/optional/optional.hpp>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <regex>
#include <snappy.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/util/duration.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/checkpoint/file_util.h"
#include "streams/exec/checkpoint/manifest_builder.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_stats.h"

using namespace std::chrono_literals;
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
        tassert(ErrorCodes::InternalError,
                fmt::format("Invalid fidx={}", fidx),
                fidx >= 0 && fidx < kMaxStateFileIdx);
        return fidx;
    } else {
        return boost::none;
    }
}

}  // namespace

LocalDiskCheckpointStorage::LocalDiskCheckpointStorage(Options opts, Context* ctxt)
    : CheckpointStorage{ctxt},
      _opts{std::move(opts)},
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
    if (_lastCreatedCheckpointId && *_lastCreatedCheckpointId == next) {
        sleepmillis(1);
        // checkpointId is chosen based on the current wallclock milliseconds.
        // Checkpoints are usually spaced out by a few minutes, so we should never
        // end up with the same wallclock millis on the same node. Some test flows can cause this
        // to occur though, so we handle the situation and print a warning.
        LOGV2_WARNING(7712805,
                      "Next checkpoint ID is the same as the last checkpoint ID, retrying",
                      "context"_attr = _context,
                      "checkpointId"_attr = next);
        next = Date_t::now().toMillisSinceEpoch();
    }

    fspath dir = _opts.writeRootDir / std::to_string(next);
    invariant(!std::filesystem::exists(dir));
    _lastCreatedCheckpointId = next;

    std::filesystem::create_directories(dir);

    _activeCheckpointSave =
        ActiveCheckpointSave{.checkpointId = next,
                             .checkpointStartTime = Date_t::now(),
                             .manifest = ManifestBuilder{_context, getManifestFilePath(dir)},
                             .directory = dir};

    LOGV2_INFO(
        7863451, "checkpoint started", "context"_attr = _context, "checkpointId"_attr = next);

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
    _memoryUsageHandle.set(_activeCheckpointSave->stateFileBuf->capacity());
    _maxMemoryUsageBytes->set(std::max(_maxMemoryUsageBytes->value(),
                                       (double)_memoryUsageHandle.getCurrentMemoryUsageBytes()));

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
    _activeCheckpointSave->checkpointSizeBytes += compressed.length();

    fspath stateFilePath = getStateFilePath(
        _activeCheckpointSave->directory, _activeCheckpointSave->currStateFileIdx, ".sz");

    fspath shadowPath = getShadowFilePath(stateFilePath);
    try {
        // write it to disk. We write to a different filename first and then rename it to the
        // eventual name to ensure that the file is visible atomically to an external process
        writeFile(shadowPath, compressed.data(), compressed.length(), boost::none);
    } catch (const DBException& msg) {
        LOGV2_WARNING(
            7863403, "File I/O error: ", "msg"_attr = msg.what(), "context"_attr = _context);
        tasserted(ErrorCodes::InternalError,
                  fmt::format("Error writing to file={}, errno={}, context={}",
                              shadowPath.native(),
                              errno,
                              tojson(_context->toBSON())));
    }
    try {
        // Rename to eventual name
        std::filesystem::rename(shadowPath, stateFilePath);
    } catch (const DBException& msg) {
        LOGV2_WARNING(
            7863405, "File I/O error: ", "msg"_attr = msg.what(), "context"_attr = _context);
        tasserted(ErrorCodes::InternalError,
                  fmt::format("Could not rename: {} -> {}, err:{}, context={}",
                              shadowPath.native(),
                              stateFilePath.native(),
                              msg.what(),
                              tojson(_context->toBSON())));
    }

    // Store compressed file checksum in manifest
    _activeCheckpointSave->manifest.addStateFileChecksum(_activeCheckpointSave->currStateFileIdx,
                                                         checksum);

    // Advance to next state file
    ++_activeCheckpointSave->currStateFileIdx;
    _activeCheckpointSave->currStateFileOffset = 0;
    _activeCheckpointSave->stateFileBuf.reset();
    _memoryUsageHandle.set(0);
}

void LocalDiskCheckpointStorage::doCommitCheckpoint(CheckpointId chkId) {
    invariant(isActiveCheckpoint(chkId));
    invariant(!hasActiveWriter(chkId));

    // write the state file if needed
    writeActiveStateFileToDisk();
    std::string filepath = _activeCheckpointSave->manifest.filePath();

    // Write the manifest
    CheckpointMetadata metadata;
    metadata.setTenantId(_tenantId);
    metadata.setStreamProcessorId(_streamProcessorId);
    metadata.setCheckpointId(chkId);
    metadata.setCheckpointStartTime(_activeCheckpointSave->checkpointStartTime);
    metadata.setCheckpointEndTime(Date_t::now());
    metadata.setCheckpointSizeBytes(_activeCheckpointSave->checkpointSizeBytes);
    if (!_opts.hostName.empty()) {
        metadata.setHostName(_opts.hostName);
    }

    metadata.setExecutionPlan(_context->executionPlan);
    metadata.setUserPipeline(_opts.userPipeline);

    // Compute the summary stats for this checkpoint.
    // This is the current $source and sink operator stats, plus the summary stats
    // in the restore checkpoint.
    std::vector<OperatorStats> operatorStats;
    for (const auto& [opId, stats] : _activeCheckpointSave->stats) {
        operatorStats.push_back(stats);
    }
    auto summaryStats = computeStreamSummaryStats(operatorStats);
    if (_context->restoredCheckpointInfo) {
        tassert(ErrorCodes::InternalError,
                "Expected summaryStats to be set",
                _context->restoredCheckpointInfo->summaryStats);
        summaryStats += toSummaryStats(*_context->restoredCheckpointInfo->summaryStats);
        if (_context->restoredCheckpointInfo->operatorInfo) {
            // If there is a restore checkpoint, add its stats to the current operator stats.
            operatorStats = combineAdditiveStats(
                operatorStats, toOperatorStats(*_context->restoredCheckpointInfo->operatorInfo));
        }
    }
    metadata.setSummaryStats(toSummaryStatsDoc(std::move(summaryStats)));

    // Save the operator level stats in the checkpoint.
    metadata.setOperatorStats(toCheckpointOpInfo(operatorStats));

    int64_t writeDurationMs =
        Milliseconds{metadata.getCheckpointEndTime() - metadata.getCheckpointStartTime()}.count();
    std::string directory = _activeCheckpointSave->directory.string();

    metadata.setPipelineVersion(_context->pipelineVersion);

    addUnflushedCheckpoint(chkId,
                           CheckpointDescription{chkId,
                                                 directory,
                                                 _lastCheckpointSizeBytes,
                                                 mongo::Date_t::now(),
                                                 Milliseconds{writeDurationMs}});

    _activeCheckpointSave->manifest.writeToDisk(std::move(metadata));
    // bookkeeping for checkpoint sizes
    _checkpointSizeBytes->increment(_activeCheckpointSave->checkpointSizeBytes);
    _lastCheckpointSizeBytes = _activeCheckpointSave->checkpointSizeBytes;
    // Reset ActiveSaver
    _activeCheckpointSave.reset();

    // clean up _finalized writers
    for (auto itr = _finalizedWriters.begin(); itr != _finalizedWriters.end();) {
        if (std::get<0>(*itr) == chkId) {
            _finalizedWriters.erase(itr++);
        }
    }

    LOGV2_INFO(7863450,
               "Committed checkpoint",
               "context"_attr = _context,
               "checkpointId"_attr = chkId,
               "fullManifestPath"_attr = filepath);
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
    tassert(ErrorCodes::InternalError,
            fmt::format("Expected version greater than or equal to {}, got {}",
                        ManifestBuilder::kMinAllowedVersion,
                        version),
            version >= ManifestBuilder::kMinAllowedVersion);

    return true;
}

void LocalDiskCheckpointStorage::populateManifestInfo(const fspath& manifestFile) {
    std::string buf;
    _restoredManifestInfo = ManifestInfo{};
    try {
        buf = readFile(manifestFile.native());
    } catch (const DBException& msg) {
        LOGV2_WARNING(7863401,
                      "Caught exception from readFile",
                      "file"_attr = manifestFile.native(),
                      "msg"_attr = msg.what(),
                      "context"_attr = _context);
        tasserted(ErrorCodes::InternalError, msg.what());
    }

    // Validate that the checksum stored as a 4 byte preamble matches the computed checksum
    uint32_t mcrc = *(uint32_t*)buf.data();
    uint32_t computedChecksum = getChecksum32(buf.data() + 4, buf.size() - 4);
    tassert(ErrorCodes::InternalError,
            fmt::format("manifest file checksum mismatch. {}/{}", mcrc, computedChecksum),
            mcrc == computedChecksum);

    // checksum matches. Now construct the manifest BSONObj
    BSONObj manifestObj{&buf[0] + 4};

    auto manifest =
        mongo::Manifest::parseOwned(IDLParserContext{"Manifest"}, manifestObj.getOwned());

    // Some validity checks at a logical level
    if (!validateManifest(manifest)) {
        tasserted(ErrorCodes::InternalError, "could not validate manifest");
        return;
    }

    _restoredManifestInfo->checkpointId = manifest.getMetadata().getCheckpointId();
    _restoredManifestInfo->checkpointCommitTs = manifest.getMetadata().getCheckpointEndTime();

    auto checkpointSizeBytesOpt = manifest.getMetadata().getCheckpointSizeBytes();
    if (checkpointSizeBytesOpt) {
        _restoredManifestInfo->checkpointSizeBytes = *checkpointSizeBytesOpt;
    }

    // TODO(SERVER-83239): For now assume that checkpointFileList and operatorRanges are always
    // present and ignore metadata
    for (auto& fInfo : manifest.getCheckpointFileList()->getFiles()) {
        std::string fName = fInfo.getName().toString();
        uint32_t checksum = (uint32_t)fInfo.getChecksum();
        boost::optional<int> fidx = getStateFileIdxFromName(fName);
        if (!fidx) {
            LOGV2_WARNING(7863409,
                          "Could not extract file idx from file name: ",
                          "fname"_attr = fName,
                          "context"_attr = _context);
            tasserted(ErrorCodes::InternalError,
                      fmt::format("Could not get file idx from state file: {}, context: {}",
                                  fName,
                                  tojson(_context->toBSON())));
        }
        tassert(ErrorCodes::InternalError,
                fmt::format("Duplicate file idx - {}", *fidx),
                _restoredManifestInfo->fileChecksums.find(*fidx) ==
                    _restoredManifestInfo->fileChecksums.end());

        _restoredManifestInfo->fileChecksums[*fidx] = (uint32_t)checksum;
    }

    for (auto& e : *manifest.getOperatorCheckpointFileRanges()) {
        absl::InlinedVector<ManifestBuilder::OpStateRange, 1> ranges;
        for (auto& loc : e.getFileRanges()) {
            std::string fName = loc.getFile().toString();
            auto fidx = getStateFileIdxFromName(fName);
            if (!fidx) {
                LOGV2_WARNING(7863436,
                              "Could not extract file idx from file name: ",
                              "fname"_attr = fName,
                              "context"_attr = _context);
                tasserted(ErrorCodes::InternalError,
                          fmt::format("Could not get file idx from state file: {}, context: {}",
                                      fName,
                                      tojson(_context->toBSON())));
            }
            off_t beg = loc.getBegin();
            off_t end = loc.getEnd();
            ranges.push_back(ManifestBuilder::OpStateRange{*fidx, beg, end});
        }
        tassert(ErrorCodes::InternalError,
                "Multiple entries found for operator!",
                _restoredManifestInfo->opsRangeMap.find(e.getOpid()) ==
                    _restoredManifestInfo->opsRangeMap.end());
        _restoredManifestInfo->opsRangeMap[e.getOpid()] = std::move(ranges);
    }

    if (manifest.getMetadata().getOperatorStats()) {
        _restoredManifestInfo->stats = *manifest.getMetadata().getOperatorStats();
    }

    _restoredManifestInfo->writeDurationMs = manifest.getMetadata().getCheckpointEndTime() -
        manifest.getMetadata().getCheckpointStartTime();

    _restoredManifestInfo->version = manifest.getVersion();

    _restoredManifestInfo->metadata = CheckpointMetadata::parseOwned(
        IDLParserContext("LocalDiskCheckpointStorage::populateManifestInfo"),
        manifest.getMetadata().toBSON());
}

boost::optional<CheckpointId> LocalDiskCheckpointStorage::doGetRestoreCheckpointId() {
    if (_opts.restoreRootDir.empty()) {
        return boost::none;
    }

    if (!_restoredManifestInfo) {
        populateManifestInfo(getManifestFilePath(_opts.restoreRootDir));
    }

    return _restoredManifestInfo->checkpointId;
}

// Currently, this will be called from the stream manager thread before
// the executor is started. So, for e.g. metrics are not yet available
RestoredCheckpointInfo LocalDiskCheckpointStorage::doStartCheckpointRestore(CheckpointId chkId) {
    invariant(!_activeRestorer);
    tassert(
        ErrorCodes::InternalError, "Expected the restored ManifestInfo.", _restoredManifestInfo);

    auto [checkpointId,
          opsRangeMap,
          fileChecksums,
          stats,
          lastCheckpointCommitTs,
          lastCheckpointSizeBytes,
          writeDurationMs,
          metadata,
          version] = *_restoredManifestInfo;
    // Most of the time, we will be restoring from the last committed checkpoint, so using the size
    // of the checkpoint being restored as the lastCheckpointSizeBytes should be fine
    _lastCheckpointCommitTs = lastCheckpointCommitTs;
    _lastCheckpointSizeBytes = lastCheckpointSizeBytes;
    _activeRestorer = std::make_unique<Restorer>(chkId,
                                                 _context,
                                                 std::move(opsRangeMap),
                                                 std::move(fileChecksums),
                                                 _opts.restoreRootDir,
                                                 std::move(stats));

    LOGV2_INFO(7863452,
               "Checkpoint restore started",
               "context"_attr = _context,
               "checkpointId"_attr = chkId);

    tassert(ErrorCodes::InternalError,
            "Expected operatorStats to be set in checkpoint",
            metadata.getOperatorStats());
    tassert(ErrorCodes::InternalError,
            "Expected userPipeline to be set in checkpoint",
            metadata.getUserPipeline());
    if (_restoredManifestInfo->version > ManifestBuilder::kVersionWithNoSummaryStats) {
        tassert(ErrorCodes::InternalError,
                "Expected summaryStats to be set in checkpoint",
                metadata.getSummaryStats());
        tassert(ErrorCodes::InternalError,
                "Expected pipelineVersion to be set in checkpoint",
                metadata.getPipelineVersion());
    }

    RestoredCheckpointInfo info;
    info.operatorInfo = metadata.getOperatorStats();
    info.summaryStats = metadata.getSummaryStats();
    if (metadata.getPipelineVersion()) {
        info.pipelineVersion = *metadata.getPipelineVersion();
    }
    info.userPipeline = std::vector<BSONObj>{};
    info.userPipeline.reserve(metadata.getUserPipeline()->size());
    for (const auto& stage : *metadata.getUserPipeline()) {
        info.userPipeline.push_back(stage.getOwned());
    }
    CheckpointDescription details;
    details.setFilepath(_activeRestorer->restoreRootDir().string());
    details.setId(_activeRestorer->getCheckpointId());
    details.setCheckpointSizeBytes(_lastCheckpointSizeBytes);
    details.setCheckpointTimestamp(_lastCheckpointCommitTs);
    details.setWriteDurationMs(Milliseconds{writeDurationMs});
    info.description = std::move(details);

    // Populate the execution plan from the restore checkpoint.
    const auto& executionPlan = metadata.getExecutionPlan();
    if (executionPlan) {
        info.executionPlan.reserve(executionPlan->size());
        for (const auto& stage : *executionPlan) {
            info.executionPlan.push_back(stage.getOwned());
        }
    } else {
        // TODO(SERVER-92447): Remove the else block.
        tassert(ErrorCodes::InternalError,
                fmt::format("Missing execution plan in checkpoint for manifest version {}",
                            _restoredManifestInfo->version),
                _restoredManifestInfo->version <= ManifestBuilder::kVersionWithNoExecutionPlan);
    }

    return info;
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

    LOGV2_INFO(
        7863453, "Restored checkpoint", "context"_attr = _context, "checkpointId"_attr = chkId);
}

boost::optional<mongo::Document> LocalDiskCheckpointStorage::doGetNextRecord(ReaderHandle* reader) {
    CheckpointId chkId = reader->getCheckpointId();
    OperatorId opId = reader->getOperatorId();
    invariant(_activeRestorer && _activeRestorer->getCheckpointId() == chkId);
    return _activeRestorer->getNextRecord(opId);
}

void LocalDiskCheckpointStorage::doAddStats(CheckpointId checkpointId,
                                            OperatorId operatorId,
                                            const OperatorStats& stats) {
    tassert(ErrorCodes::InternalError, "Unexpected checkpointId", isActiveCheckpoint(checkpointId));

    if (!_activeCheckpointSave->stats.contains(operatorId)) {
        _activeCheckpointSave->stats[operatorId] =
            OperatorStats{.operatorName = stats.operatorName};
    }
    _activeCheckpointSave->stats[operatorId] += stats;
}

}  // namespace streams
