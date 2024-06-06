/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "reader_writer.h"

namespace mongo {
namespace queryable {

ReaderWriter::ReaderWriter(BlockstoreHTTP blockstore, std::string path, std::size_t fileSize)
    : _blockstore(std::move(blockstore)), _path(std::move(path)), _fileSize(fileSize) {}

StatusWith<std::size_t> ReaderWriter::read(DataRange buf,
                                           std::size_t offset,
                                           std::size_t count) const {
    invariant(offset + count <= _fileSize);
    invariant(count <= buf.length());

    return _blockstore.read(_path, buf, offset, count);
}

StatusWith<DataBuilder> ReaderWriter::write(ConstDataRange buf,
                                            std::size_t offset,
                                            std::size_t count) const {
    invariant(count <= buf.length());

    return _blockstore.write(_path, buf, offset, count);
}

}  // namespace queryable
}  // namespace mongo
