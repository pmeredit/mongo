/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "queryable_datafile.h"

#ifndef _WIN32
#include <sys/mman.h>
#endif

#include "mongo/base/error_codes.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "../blockstore/reader.h"
#include "queryable_alloc_state.h"

namespace mongo {
namespace queryable {

namespace {

// A tuple of <firstBlockIdx,numBlocks>.
using BlockIdxRange = std::tuple<std::size_t, std::size_t>;

static BlockIdxRange getRange(std::size_t offset, std::size_t count, std::size_t bucketSize) {
    auto startBlockIdx = offset / bucketSize;
    // The last byte to read, not the first byte to not* read.
    auto endOfsInclusive = offset + count - 1;
    auto endBlockIdx = endOfsInclusive / bucketSize;
    return std::make_tuple(startBlockIdx, endBlockIdx - startBlockIdx + 1);
}

}  // namespace

DataFile::DataFile(std::unique_ptr<queryable::Reader> reader,
                   AllocState* const allocState,
                   std::size_t pageSize)
    : _reader(std::move(reader)),
      _pageSize(pageSize),
      _mappingLock(),
// If one of the vector allocations (_mappedPages/_mappedBlocks) fails, the _basePtr will
// leak. We're ignoring this case because the program is not expected to operate in this degraded
// state and should exit.
#ifdef _WIN32
      _basePtr(VirtualAlloc(nullptr, _reader->getFileSize(), MEM_RESERVE, PAGE_NOACCESS)),
#else
      _basePtr(mmap(nullptr, _reader->getFileSize(), PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0)),
#endif
      _mappedPages(
          // math.ceil(filesize / pagesize)
          (_reader->getFileSize() + pageSize - 1) / pageSize,
          false),
      _mappedBlocks(_reader->getNumBlocks(), false),
      _allocState(allocState) {
    uassert(ErrorCodes::InternalError,
            str::stream() << "Failed to allocate virtual memory. File: " << _reader->getFileName()
                          << " Size: "
                          << _reader->getFileSize(),
            _basePtr != nullptr);
}

DataFile::~DataFile() {
#ifdef _WIN32
    VirtualFree(_basePtr, 0, MEM_RELEASE);
#else
    munmap(_basePtr, _reader->getFileSize());
#endif
}

std::size_t DataFile::getPageSizeForIdx(std::size_t pageIdx) const {
    const std::size_t lastPageIdx = _mappedBlocks.size() - 1;
    if (pageIdx < lastPageIdx) {
        return _pageSize;
    }

    std::size_t lastPageOffset = pageIdx * _pageSize;
    return _reader->getFileSize() - lastPageOffset;
}

Status DataFile::ensureRange(const std::size_t offset, const std::size_t count) {
    stdx::lock_guard<stdx::mutex> lock(_mappingLock);

    // Map pages into memory for block data to be written into.
    size_t pageOffset;
    size_t numPages;
    std::tie(pageOffset, numPages) = getRange(offset, count, _pageSize);
    for (std::size_t num = 0; num < numPages; ++num) {
        auto pageIdx = pageOffset + num;
        if (_mappedPages[pageIdx]) {
            continue;
        }

        auto startPos = static_cast<char*>(_basePtr) + (pageIdx * _pageSize);
#ifdef _WIN32
        auto vpRet = VirtualAlloc(startPos, getPageSizeForIdx(pageIdx), MEM_COMMIT, PAGE_READWRITE);
        uassert(ErrorCodes::OperationFailed,
                str::stream() << "Failed to make a page read/write. Code: " << GetLastError(),
                vpRet);
#else
        auto mmapRet = mmap(startPos,
                            getPageSizeForIdx(pageIdx),
                            PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANON | MAP_FIXED,
                            -1,
                            0);
        uassert(ErrorCodes::ExceededMemoryLimit, "Failed to mmap a page", mmapRet != MAP_FAILED);
        uassert(ErrorCodes::BadValue, "Mmap returned an unexpected address", mmapRet == startPos);
#endif
        _mappedPages[pageIdx] = true;
        _allocState->allocPage(this, pageIdx);
    }

    auto fileBlockSize = _reader->getBlockSize();
    auto blockIdxRange = getRange(offset, count, fileBlockSize);
    for (std::size_t num = 0; num < std::get<1>(blockIdxRange); ++num) {
        auto blockIdx = std::get<0>(blockIdxRange) + num;
        if (_mappedBlocks[blockIdx]) {
            continue;
        }

        auto thisBlockSize = _reader->getBlockSizeForIdx(blockIdx);
        if (!_allocState->allocBlock(thisBlockSize).isOK()) {
            throw WriteConflictException();
        }

        auto startPos = static_cast<char*>(_basePtr) + (blockIdx * fileBlockSize);
        auto status = _reader->readBlockInto(DataRange(startPos, thisBlockSize), blockIdx);
        if (!status.isOK()) {
            return status.getStatus();
        }

        _mappedBlocks[blockIdx] = true;
    }

    return Status::OK();
}

Status DataFile::releasePage(const std::size_t pageIdx) {
    auto startPos = static_cast<char*>(_basePtr) + (pageIdx * _pageSize);

    stdx::lock_guard<stdx::mutex> lock(_mappingLock);
#ifdef _WIN32
    auto vpRet = VirtualFree(startPos, getPageSizeForIdx(pageIdx), MEM_DECOMMIT);
    if (!vpRet) {
        severe() << "Failed to decommit a page. Code: " << GetLastError();
        fassertFailedNoTrace(40369);
    }
#else
    auto mmapRet = mmap(
        startPos, getPageSizeForIdx(pageIdx), PROT_NONE, MAP_PRIVATE | MAP_ANON | MAP_FIXED, -1, 0);
    invariant(mmapRet != MAP_FAILED);
    invariant(mmapRet == startPos);
#endif

    _mappedPages[pageIdx] = false;
    _allocState->freePage(this, pageIdx);

    auto offset = pageIdx * _pageSize;
    // Convert [offset -> offset + _pageSize] into block indexes.
    auto blockIdxRange = getRange(offset, _pageSize, _reader->getBlockSize());
    for (std::size_t num = 0; num < std::get<1>(blockIdxRange); ++num) {
        auto blockIdx = std::get<0>(blockIdxRange) + num;
        if (blockIdx >= _mappedBlocks.size()) {
            break;
        }

        if (_mappedBlocks[blockIdx]) {
            _allocState->freeBlock(_reader->getBlockSizeForIdx(blockIdx));
            _mappedBlocks[blockIdx] = false;
        }
    }

    return Status::OK();
}

}  // namespace queryable
}  // namespace mongo
