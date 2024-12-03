/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/checkpoint/checkpoint_restorer.h"

#include <chrono>
#include <fcntl.h>
#include <snappy.h>
#include <unistd.h>

#include "mongo/base/data_type_endian.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/producer_consumer_queue.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"
#include "streams/exec/checkpoint/file_util.h"
#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace std::chrono_literals;
using namespace mongo;

namespace streams {

using fspath = std::filesystem::path;

CheckpointRestorer::OpRestorer::OpRestorer(OperatorId opId_,
                                           OpStateRanges ranges_,
                                           CheckpointRestorer* restorer_)
    : opId{opId_}, ranges{std::move(ranges_)}, restorer{restorer_} {}

bool CheckpointRestorer::OpRestorer::hasMore() {
    if (currRange >= ranges.size()) {
        return false;
    }

    if (!bufReader) {
        // We have not yet started reading the current range
        hydrateCurrentRange();
    }

    if (!bufReader->atEof()) {
        // We still have not read the entire current range
        return true;
    }

    // We are the end of the current range. Advance and retry
    ++currRange;
    bufReader.reset();
    return hasMore();
}

void CheckpointRestorer::OpRestorer::hydrateCurrentRange() {
    int fileIdx = ranges[currRange].stateFileIdx;
    size_t rangeLen = ranges[currRange].len();
    const std::string& fileBuf = restorer->getStateFile(fileIdx);
    tassert(7863419,
            fmt::format("Invalid end range: {}/{}", ranges[currRange].end, fileBuf.size()),
            (size_t)ranges[currRange].end <= fileBuf.size());
    bufReader = std::make_unique<BufReader>(&fileBuf[ranges[currRange].begin], rangeLen);
}

boost::optional<mongo::Document> CheckpointRestorer::OpRestorer::getNextRecord() {
    if (!hasMore()) {
        return boost::none;
    }
    invariant(bufReader && !bufReader->atEof());
    Document doc = Document::deserializeForSorter(*bufReader, {});
    return std::move(doc);
}

const std::string& CheckpointRestorer::getStateFile(int fileIdx) {
    if (_cachedStateFile) {
        if (_cachedStateFile->first != fileIdx) {
            _cachedStateFile = boost::none;
        }
    }
    if (!_cachedStateFile) {
        readStateFile(fileIdx);
    }
    invariant(_cachedStateFile && _cachedStateFile->first == fileIdx);
    return _cachedStateFile->second;
}

void CheckpointRestorer::readStateFile(int fileIdx) {
    fspath stateFile = getStateFilePath(restoreRootDir(), fileIdx, ".sz");

    // Read file into a buffer
    std::string inputFile;
    try {
        inputFile = readFile(stateFile.native());
    } catch (const DBException& msg) {
        LOGV2_WARNING(7863433,
                      "Caught exception from readFile",
                      "file"_attr = stateFile.native(),
                      "msg"_attr = msg.what(),
                      "context"_attr = getContext());
        tasserted(7863434, msg.what());
    }

    // Validate checksum - The manifest has an expected checksum for each state file. Recompute the
    // checksum and validate against the expected value
    uint32_t checksum = getChecksum32(inputFile.data(), inputFile.length());
    uint32_t expectedChecksum = _fileChecksums[fileIdx];

    tassert(7863420,
            fmt::format("Checksum mismatch for state file: {}; Actual: {}, Expected:{}",
                        fileIdx,
                        checksum,
                        expectedChecksum),
            checksum == expectedChecksum);

    // Uncompress the file
    std::string uncompressedFile;
    if (!snappy::Uncompress(&inputFile[0], inputFile.size(), &uncompressedFile)) {
        tasserted(7863421, fmt::format("Error in uncompressing file - {}", stateFile.native()));
    }
    _cachedStateFile = std::make_pair(fileIdx, std::move(uncompressedFile));
}

void CheckpointRestorer::markOperatorDone(OperatorId opId) {
    _currOpRestorer = boost::none;
}

boost::optional<mongo::Document> CheckpointRestorer::getNextRecord(OperatorId opId) {
    if (_currOpRestorer) {
        tassert(
            7863422,
            fmt::format("Attempt to read opId={}, when currOpId={}", opId, _currOpRestorer->opId),
            _currOpRestorer->opId == opId);
    } else {
        auto itr = _opRanges.find(opId);
        uassert(7863423, fmt::format("opId - {} - not found.", opId), itr != _opRanges.end());
        _currOpRestorer.emplace(opId, itr->second, this);
    }

    return _currOpRestorer->getNextRecord();
}

}  // namespace streams
