/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <iosfwd>

#include "mongo/base/data_range.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/oid.h"

#include "http_client.h"

namespace mongo {
namespace queryable {

/**
 * Contains context for reading a specific file. Given a filename (`path`) along with the `BackupDB`
 * and `snapshotId` provided by `Context`, creates a reader instance that can service requests to
 * read `length` bytes from a file starting at `offset.
 */
class Reader {
public:
    Reader(std::unique_ptr<HttpClientInterface> httpClient,
           std::string path,
           std::size_t fileSize,
           std::size_t blockSize);

    /**
     * Read the file from [offset, offset+count) into `buf`. Returns the number of bytes written
     * into `buf`.
     */
    StatusWith<std::size_t> read(DataRange buf, std::size_t offset, std::size_t count) const;

    /**
     * Read the entirety of a block into `buf`. `buf` must be allocated large enough to hold the
     * block i.e: `readerInstance.getBlockSizeForIdx(blockIdx)`. Returns the number of bytes
     * written into `buf`.
     */
    StatusWith<std::size_t> readBlockInto(DataRange buf, std::size_t blockIdx) const;

    /**
     * Reads the entire remote file and writes it to the `writer`.
     */
    Status readInto(std::ostream* writer) const;

    const std::string& getFileName() const {
        return _path;
    }

    std::size_t getFileSize() const {
        return _fileSize;
    }

    std::size_t getBlockSize() const {
        return _blockSize;
    }

    // Return the block size at an index. All are the same except the last block which is truncated
    // with respect to the fileSize.
    std::size_t getBlockSizeForIdx(std::size_t blockIdx) const {
        const std::size_t lastBlockIdx = getNumBlocks() - 1;
        if (blockIdx < lastBlockIdx) {
            return _blockSize;
        }

        std::size_t lastBlockOffset = blockIdx * _blockSize;
        return _fileSize - lastBlockOffset;
    }

    std::size_t getNumBlocks() const {
        std::size_t numBlocks = _fileSize / _blockSize;
        if (_fileSize % _blockSize) {
            ++numBlocks;
        }

        return numBlocks;
    }

private:
    std::unique_ptr<HttpClientInterface> _httpClient;
    std::string _path;
    std::size_t _fileSize;
    std::size_t _blockSize;
};

}  // namespace queryable
}  // namespace mongo
