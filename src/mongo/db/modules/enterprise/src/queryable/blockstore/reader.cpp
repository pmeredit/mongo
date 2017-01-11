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
    invariant(offset + count <= _fileSize);
    invariant(count <= buf.length());

    return _httpClient->read(_path, buf, offset, count);
}

StatusWith<std::size_t> Reader::readBlockInto(DataRange buf, std::size_t blockIdx) const {
    const std::size_t kOffset = _blockSize * blockIdx;
    const std::size_t kCount = getBlockSizeForIdx(blockIdx);
    invariant(buf.length() >= kCount);

    return _httpClient->read(_path, buf, kOffset, kCount);
}

Status Reader::readInto(std::ostream* writer) const {
    auto buf = stdx::make_unique<char[]>(_blockSize);
    DataRange bufRange(buf.get(), _blockSize);
    for (std::size_t blockIdx = 0; blockIdx < getNumBlocks(); ++blockIdx) {
        auto swBytesRead = readBlockInto(bufRange, blockIdx);
        uassertStatusOK(swBytesRead);
        writer->write(buf.get(), swBytesRead.getValue());
    }

    return Status::OK();
}

}  // namespace queryable
}  // namespace mongo
