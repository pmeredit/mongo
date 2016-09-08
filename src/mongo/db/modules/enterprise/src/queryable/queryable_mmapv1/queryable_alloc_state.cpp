/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "queryable_alloc_state.h"

#include "queryable_datafile.h"

#include "mongo/base/error_codes.h"

namespace mongo {
namespace queryable {

void AllocState::allocPage(DataFile* dataFile, std::size_t pageIdx) {
    stdx::lock_guard<stdx::mutex> lock(_lock);

    _pagesAlloced[dataFile].insert(pageIdx);
    ++_numPagesAllocated;
}

Status AllocState::allocBlock(std::size_t blockSize) {
    stdx::lock_guard<stdx::mutex> lock(_lock);
    // To prevent edge cases where a block can be larger than the quota, simply allow allocations
    // until the current allocation exceeds the quota. The effective quota becomes `<input quota> +
    // <last block allocated>`.
    if (_memoryQuotaBytes > 0 && _memoryAllocated >= _memoryQuotaBytes) {
        return {ErrorCodes::ExceededMemoryLimit,
                str::stream() << "Cannot allocate more memory. Quota: " << _memoryQuotaBytes
                              << " Allocated: "
                              << _memoryAllocated};
    }

    _memoryAllocated += blockSize;
    return Status::OK();
}

void AllocState::freePage(DataFile* dataFile, std::size_t pageIdx) {
    stdx::lock_guard<stdx::mutex> lock(_lock);

    _pagesAlloced[dataFile].erase(pageIdx);
    --_numPagesAllocated;
}

void AllocState::freeBlock(std::size_t blockSize) {
    stdx::lock_guard<stdx::mutex> lock(_lock);

    _memoryAllocated -= blockSize;
}

void AllocState::selectPageForFree(DataFile** dataFileToFree, std::size_t* pageIdx) {
    stdx::lock_guard<stdx::mutex> lock(_lock);
    if (_numPagesAllocated == 0) {
        return;
    }

    // Finding the page to remove is O(pageToSelect + _pagesAlloced.size()).
    std::size_t pageToSelect = _random.nextInt64(_numPagesAllocated);

    auto fileIt = _pagesAlloced.begin();
    // Some values in the `_pagesAlloced` might be empty sets, skip to the first non-empty one.
    while (fileIt->second.size() == 0) {
        fileIt++;
    }

    auto pageIt = fileIt->second.begin();
    for (std::size_t idx = 0; idx < pageToSelect; ++idx) {
        pageIt++;

        // If we've exhausted the pages for this set, find the next datafile with allocated pages.
        while (pageIt == fileIt->second.end()) {
            fileIt++;
            pageIt = fileIt->second.begin();
        }
    }

    *dataFileToFree = fileIt->first;
    *pageIdx = *pageIt;
}

}  // namespace queryable
}  // namespace mongo
