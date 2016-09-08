/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <map>
#include <set>

#include "mongo/base/status.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/mutex.h"

namespace mongo {
namespace queryable {

class DataFile;

/*
 * AllocState keeps the state of all memory allocations with the exception of reserving virtual
 * memory space. Being a shared state, this class is made to be thread-safe. All page allocations
 * and blocks written to memory by the queryable extent manager must be tracked here. This structure
 * maintains a collection of all physically allocated pages with blocks of data written to them.
 */
class AllocState {
public:
    AllocState(std::uint64_t memoryQuotaBytes)
        : _memoryQuotaBytes(memoryQuotaBytes),
          _lock(),
          _pagesAlloced(),
          _numPagesAllocated(0),
          _memoryAllocated(0),
          // The constant seed is not expected to be a problem.
          _random(0) {}

    std::uint64_t getMemoryQuotaBytes() const {
        return _memoryQuotaBytes;
    }

    std::size_t getNumPagesAllocated() {
        stdx::lock_guard<stdx::mutex> lock(_lock);
        return _numPagesAllocated;
    }

    std::uint64_t getMemoryAllocated() {
        stdx::lock_guard<stdx::mutex> lock(_lock);
        return _memoryAllocated;
    }

    // Called to declare a page is being allocated.
    void allocPage(DataFile* dataFile, std::size_t pageIdx);

    // Return OK if the current allocation is less than the quota, otherwise `ExceededMemoryLimit`.
    Status allocBlock(std::size_t blockSize);

    // Called to declare a page is being freed.
    void freePage(DataFile* dataFile, std::size_t pageIdx);

    // Declare that memory is being freed which can be reflected in `getMemoryAllocated`.
    void freeBlock(std::size_t blockSize);

    // All params to `selectPageForFree` are output parameters. If zero pages are allocated, the
    // call will return immediately.
    void selectPageForFree(DataFile** dataFileToFree, std::size_t* pageIdx);

private:
    // The quota for number of bytes permitted to be written before blocking writes. Non-positive is
    // treated as unlimited.
    const std::uint64_t _memoryQuotaBytes;

    // This is a shared structure; each method must grab this lock.
    stdx::mutex _lock;

    // DataFile -> list of page indexes virtually allocated. Each page is guaranteed to have at
    // least one block physically allocated.
    std::map<DataFile*, std::set<std::size_t>> _pagesAlloced;

    std::size_t _numPagesAllocated;

    // Physical memory, counted from `allocBlock` and `freeBlock`.
    std::uint64_t _memoryAllocated;

    PseudoRandom _random;
};

}  // namespace queryable
}  // namespace mongo
