/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/checkpoint/file_util.h"

#include <charconv>
#include <fcntl.h>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>

#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
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
                        int fileIdx,
                        const std::string& suffix) {
    std::string fname = getStateFileNameFromIdx(fileIdx) + suffix;
    return rootDir / fname;
}

fspath getManifestFilePath(const fspath& rootDir) {
    return rootDir / "MANIFEST";
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
        uasserted(ErrorCodes::FileStreamFailed, msg);
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
        uasserted(ErrorCodes::FileStreamFailed, msg);
    }
}

uint32_t getChecksum32(const char* data, size_t len) {
    uint32_t seed = 0;
    return murmur3<sizeof(uint32_t)>(ConstDataRange{data, len}, seed);
}

}  // namespace streams
