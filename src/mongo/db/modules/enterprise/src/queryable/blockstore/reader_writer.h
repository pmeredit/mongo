/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "blockstore_http.h"

#include "mongo/base/data_range.h"
#include "mongo/base/status_with.h"

namespace mongo {
namespace queryable {

/**
 * Contains context for reading from and writing to a specific file. Given a backend BlockstoreHTTP
 * object and filename (`path`), creates an instance that can service requests to read `count` bytes
 * from a file starting at `offset` and to write `count` bytes to a file starting at `offset`.
 */
class ReaderWriter {
public:
    ReaderWriter(BlockstoreHTTP blockstore, std::string path, std::size_t fileSize);

    /**
     * Read the file from [offset, offset+count) into `buf`. Returns the number of bytes written
     * into `buf`.
     */
    StatusWith<std::size_t> read(DataRange buf, std::size_t offset, std::size_t count) const;

    /**
     * Writes `count` from `buf` to the file starting at `offset`.
     */
    StatusWith<DataBuilder> write(ConstDataRange buf, std::size_t offset, std::size_t count) const;

    const std::string& getFileName() const {
        return _path;
    }

    std::size_t getFileSize() const {
        return _fileSize;
    }

    void addToFileSize(size_t bytes) {
        _fileSize += bytes;
    }

private:
    BlockstoreHTTP _blockstore;
    std::string _path;
    std::size_t _fileSize;
};

}  // namespace queryable
}  // namespace mongo
