/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "reader.h"

namespace mongo {
namespace queryable {

Reader::Reader(BlockstoreHTTP blockstore, std::string path, std::size_t fileSize)
    : _blockstore(std::move(blockstore)), _path(std::move(path)), _fileSize(fileSize) {}

StatusWith<std::size_t> Reader::read(DataRange buf, std::size_t offset, std::size_t count) const {
    invariant(offset + count <= _fileSize);
    invariant(count <= buf.length());

    return _blockstore.read(_path, buf, offset, count);
}

}  // namespace queryable
}  // namespace mongo
