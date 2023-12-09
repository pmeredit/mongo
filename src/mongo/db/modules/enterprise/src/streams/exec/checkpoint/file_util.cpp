/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint/file_util.h"

#include <charconv>
#include <fcntl.h>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>

#include "mongo/logv2/log.h"
#include "mongo/util/assert_util_core.h"
#include "mongo/util/murmur3.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;
using fspath = std::filesystem::path;

namespace streams {

std::string getStateFileNameFromIdx(int fileIdx) {
    invariant(fileIdx >= 0 && fileIdx < kMaxStateFileIdx);
    return fmt::format("state{:05}.bin", fileIdx);
}

fspath getStateFilePath(const std::filesystem::path& rootDir,
                        const std::string& streamProcessorId,
                        CheckpointId chkId,
                        int fileIdx,
                        const std::string& suffix) {
    std::string fname = getStateFileNameFromIdx(fileIdx) + suffix;
    return rootDir / streamProcessorId / std::to_string(chkId) / fname;
}

fspath getManifestFilePath(const fspath& rootDir,
                           const std::string& streamProcessorId,
                           CheckpointId chkId) {
    return rootDir / streamProcessorId / std::to_string(chkId) / "MANIFEST";
}

std::string getShadowFilePath(const fspath& path) {
    return path.native() + ".shadow";
}

std::string readFile(const std::string& path) {
    std::string file;
    try {
        size_t sz = std::filesystem::file_size(path);
        file.resize(sz);

        std::ifstream ifs;
        ifs.exceptions(std::ios::failbit | std::ios::badbit);
        ifs.open(path, std::ios::binary | std::ios::in);
        ifs.read(file.data(), sz);
        ifs.close();
    } catch (const std::exception& e) {
        std::string msg =
            fmt::format("Caught exception when trying to read file={}, exception={}, errno={}",
                        path,
                        e.what(),
                        errno);
        LOGV2_WARNING(7863401, "Caught exception: ", "msg"_attr = msg);
        tasserted(7863402, msg);
    }
    return file;
}

void writeFile(const std::string& path,
               const char* data,
               const size_t datalen,
               boost::optional<uint32_t> checksum) {
    try {
        std::ofstream file;
        file.exceptions(std::ios::failbit | std::ios::badbit);
        file.open(path, std::ios::out | std::ios::trunc | std::ios::binary);

        if (checksum) {
            uint32_t bytes = *checksum;
            file.write((const char*)&bytes, 4);
        }

        file.write(data, datalen);
        file.flush();
        file.close();
    } catch (const std::exception& e) {
        std::string msg = fmt::format(
            "Caught exception when trying to open/write file={}, exception={}, errno={}",
            path,
            e.what(),
            errno);
        LOGV2_WARNING(7863403, "Caught exception: ", "msg"_attr = msg);
        tasserted(7863404, fmt::format("Error writing to file={}, errno={}", path, errno));
    }
}

std::pair<std::filesystem::path, CheckpointId> getRestoreRootDirAndCheckpointId(
    const std::string& streamProcessorId, const std::filesystem::path& restoreDir) {
    CheckpointId checkpointId;
    std::filesystem::path restoreDirGrandparent;
    std::string checkpointIdStr = restoreDir.filename();
    try {
        checkpointId = std::stol(checkpointIdStr);
        std::string parent = restoreDir.parent_path().filename();
        tassert(7863405,
                fmt::format("StreamprocessorId mismatch in restoreDir - {}/{}/{}",
                            streamProcessorId,
                            parent,
                            restoreDir.native()),
                streamProcessorId == parent);
        restoreDirGrandparent = restoreDir.parent_path().parent_path();
        return {restoreDirGrandparent, checkpointId};
    } catch (const std::exception& e) {
        std::string msg = fmt::format(
            "Error in retrieving restoreRootDir/checkpointId from supplied restoreDir; input={}; "
            "exception={}",
            restoreDir.native(),
            e.what());
        LOGV2_ERROR(7863406, "getRestoreRootDirAndCheckpoint failed: ", "msg"_attr = msg);
        tasserted(7863407, msg);
    }
}

uint32_t getChecksum32(const char* data, size_t len) {
    uint32_t seed = 0;
    return murmur3<sizeof(uint32_t)>(ConstDataRange{data, len}, seed);
}

}  // namespace streams
