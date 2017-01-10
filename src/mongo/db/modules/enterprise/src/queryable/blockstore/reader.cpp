/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "reader.h"

#include <memory>
#include <sstream>
#include <string>

#include "http_client.h"

#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
namespace queryable {

Reader::Reader(std::unique_ptr<HttpClientInterface> httpClient,
               std::string path,
               std::size_t fileSize,
               std::size_t blockSize)
    : _httpClient(std::move(httpClient)),
      _path(std::move(path)),
      _fileSize(fileSize),
      _blockSize(blockSize) {}

StatusWith<std::size_t> Reader::read(DataRange buf, std::size_t offset, std::size_t count) const {
    if (static_cast<std::uint64_t>(offset) + count > _fileSize)
        return {ErrorCodes::InvalidLength,
                str::stream() << "Attempted to read beyond the end of the file. File: " << _path
                              << " Filesize: "
                              << _fileSize
                              << " Offset: "
                              << offset
                              << " Bytes to read: "
                              << count};

    if (count > buf.length()) {
        return {ErrorCodes::InvalidLength,
                str::stream() << "Buffer to read into was underallocated. Buffer size: "
                              << buf.length()
                              << " Bytes to read: "
                              << count};
    }

    auto blockIdx = static_cast<std::size_t>(offset / _blockSize);
    auto blockOfs = offset % _blockSize;

    std::size_t bytesRead = 0;
    auto blockBuf = stdx::make_unique<char[]>(_blockSize);
    DataRange blockRange(blockBuf.get(), _blockSize);
    while (bytesRead < count) {
        auto readStatus = readBlockInto(blockRange, blockIdx);
        if (!readStatus.isOK()) {
            return readStatus;
        }

        auto bytesRemainingInBlock = _blockSize - blockOfs;
        auto bytesToCopy = std::min(bytesRemainingInBlock, count - bytesRead);

        char* copyTo = const_cast<char*>(buf.data() + bytesRead);
        std::memcpy(copyTo, blockRange.data() + blockOfs, bytesToCopy);

        bytesRead += bytesToCopy;

        blockOfs = 0;
        ++blockIdx;
    }

    return bytesRead;
}

Status Reader::readBlockInto(DataRange buf, std::size_t blockIdx) const {
    const std::size_t kOffset = _blockSize * blockIdx;
    const std::size_t kCount = _blockSize;

    return _httpClient->read(_path, buf, kOffset, kCount).getStatus();
}

Status Reader::readInto(std::ostream* writer) const {
    auto buf = stdx::make_unique<char[]>(_blockSize);
    DataRange bufRange(buf.get(), _blockSize);
    for (std::size_t blockIdx = 0; blockIdx < getNumBlocks(); ++blockIdx) {
        uassertStatusOK(readBlockInto(bufRange, blockIdx));
        writer->write(buf.get(), _blockSize);
    }

    return Status::OK();
}

}  // namespace queryable
}  // namespace mongo
