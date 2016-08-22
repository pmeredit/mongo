/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <vector>

#include "mongo/db/concurrency/lock_state.h"
#include "mongo/stdx/mutex.h"

#include "../blockstore/reader.h"

namespace mongo {
namespace queryable {
namespace {
std::size_t kDefaultPageSize = 2 * 1024 * 1024;
}  // namespace

class AllocState;

/**
 * Upon creation, a datafile will virtually allocate its entire `filesize` with `MAP_ANONYMOUS` and
 * `PROT_NONE`. That allocation is stored at `basePtr`. An assumption is that `mmap` calls are too
 * expensive to call on each block retrieval. Thus, instead of physically allocating pages (remove
 * the `PROT_NONE` for a region) on a per block retrieved basis, we will remove `PROT_NONE` on a
 * larger page size (`pageSize` parameter) to reduce the number of `mmap` calls. `_mappedPages`
 * tracks which regions of the top-level allocation are in physical memory and `_mappedBlocks`
 * tracks which blocks have their data stored in memory. A `_mappedBlock` may never be true if the
 * corresponding `_mappedPage` is false.
 */
class DataFile {
public:
    DataFile(std::unique_ptr<Reader> reader,
             AllocState* allocState,
             std::size_t pageSize = kDefaultPageSize);
    ~DataFile();

    Status ensureRange(const std::size_t offset, const std::size_t count);

    // Must be called with an exclusive lock. Set the given page to
    // `PROT_NONE` and set all required bits in the `_mappedPages` and
    // `_mappedBlocks` to false.
    Status releasePage(const std::size_t pageIdx);

    void* getBasePtr() {
        return _basePtr;
    }

    std::size_t getPageSize() const {
        return _pageSize;
    }

    /**
     * Only used for testing.
     */
    const std::vector<bool>& getMappedPages() const {
        return _mappedPages;
    }

    /**
     * Only used for testing.
     */
    const std::vector<bool>& getMappedBlocks() const {
        return _mappedBlocks;
    }

private:
    std::unique_ptr<Reader> _reader;
    std::size_t _pageSize;

    // Must be held when reading or writing `_mappedBlocks` and `_mappedPages`, or any data relative
    // to the `_basePtr`.
    stdx::mutex _mappingLock;

    // The beginning of the memory mapped region for this file.
    void* _basePtr;

    // The vector will have a length of `filesize / page size`.
    std::vector<bool> _mappedPages;

    // The vector will have a length of `filesize / block size`.
    std::vector<bool> _mappedBlocks;

    // A global instance owned by the BlockstoreBackedExtentManager::Factory. All allocations/frees
    // must be recorded here. The `_mappingLock` must be held when accessing this object.
    AllocState* const _allocState;
};

}  // namespace queryable
}  // namespace mongo
