/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "blockstore_http.h"

#include "mongo/base/data_range.h"
#include "mongo/base/status_with.h"

namespace mongo {
namespace queryable {

/**
 * Contains context for reading a specific file. Given a backend BlockstoreHTTP object and filename
 * (`path`), creates a reader instance that can service requests to read `length` bytes from a file
 * starting at `offset`.
 */
class Reader {
public:
    Reader(BlockstoreHTTP blockstore, std::string path, std::size_t fileSize);

    /**
     * Read the file from [offset, offset+count) into `buf`. Returns the number of bytes written
     * into `buf`.
     */
    StatusWith<std::size_t> read(DataRange buf, std::size_t offset, std::size_t count) const;

    const std::string& getFileName() const {
        return _path;
    }

    std::size_t getFileSize() const {
        return _fileSize;
    }

private:
    BlockstoreHTTP _blockstore;
    std::string _path;
    std::size_t _fileSize;
};

}  // namespace queryable
}  // namespace mongo
