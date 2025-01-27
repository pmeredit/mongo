/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <cstdint>
#include <memory>


#include "mongo/util/assert_util.h"
#include "streams/exec/source_buffer_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

void SourceBufferManager::SourceBufferDeleter::operator()(SourceBuffer* sourceBuffer) const {
    std::unique_ptr<SourceBuffer> sourceBufferOwner(sourceBuffer);
    sourceBuffer->_bufferManager->deregisterSourceBuffer(sourceBuffer);
}

SourceBufferManager::SourceBufferManager(Options options) : _options(std::move(options)) {
    uassert(mongo::ErrorCodes::InternalError,
            "Options.bufferPreallocationFraction must be in the range [0, 1]",
            _options.bufferPreallocationFraction >= 0 && _options.bufferPreallocationFraction <= 1);
    uassert(mongo::ErrorCodes::InternalError,
            "Options.metricManager must not be nullptr",
            _options.metricManager);

    registerMetrics();

    auto writeLock = _mutex.writeLock();
    recomputePreallocatedPages(writeLock);
}

SourceBufferManager::~SourceBufferManager() {
    auto writeLock = _mutex.writeLock();
    uassert(mongo::ErrorCodes::InternalError, "_buffers is not empty", _buffers.empty());
}

void SourceBufferManager::registerMetrics() {
    _numSourceBuffersGauge =
        _options.metricManager->registerIntGauge("source_buffer_manager_num_source_buffers",
                                                 "Number of source buffers",
                                                 _options.metricLabels);
    _numPreallocatedPagesGauge = _options.metricManager->registerIntGauge(
        "source_buffer_manager_num_preallocated_pages",
        "Number of pages preallocated to each source buffer",
        _options.metricLabels);
    _numAvailablePagesGauge = _options.metricManager->registerIntGauge(
        "source_buffer_manager_num_available_pages",
        "Number of pages available for allocation beyond the preallocated pages",
        _options.metricLabels);
}

SourceBufferManager::SourceBufferHandle SourceBufferManager::registerSourceBuffer() {
    // Create a SourceBuffer instance while not holding _mutex so that when an exception is
    // thrown, the SourceBuffer instance goes out of scope and gets deregistered.
    SourceBufferHandle handle(std::make_unique<SourceBuffer>(this).release());
    {
        auto writeLock = _mutex.writeLock();
        auto [it, inserted] = _buffers.emplace(handle.get(), std::make_shared<SourceBufferInfo>());
        dassert(inserted);
        _numSourceBuffersGauge->set(_buffers.size());

        // Now that there is one more source buffer, recompute the number of pages preallocated to
        // each source buffer.
        recomputePreallocatedPages(writeLock);
    }
    return handle;
}

void SourceBufferManager::deregisterSourceBuffer(SourceBufferManager::SourceBuffer* sourceBuffer) {
    auto writeLock = _mutex.writeLock();
    auto it = _buffers.find(sourceBuffer);
    uassert(mongo::ErrorCodes::InternalError, "source buffer not found", it != _buffers.end());

    // Assert that the source buffer has already released all the buffer space allocated to it.
    auto bufferInfo = it->second;
    {
        stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);
        uassert(mongo::ErrorCodes::InternalError,
                "source buffer did not release all the memory allocated to it "
                "before deregistering itself",
                bufferInfo->size == 0);
    }
    _buffers.erase(it);
    _numSourceBuffersGauge->set(_buffers.size());

    // Now that there is one less source buffer, recompute the number of pages preallocated to each
    // source buffer.
    recomputePreallocatedPages(writeLock);
}

bool SourceBufferManager::allocPages(SourceBufferManager::SourceBuffer* sourceBuffer,
                                     int64_t curSize,
                                     int32_t numPages) {
    // We currently only support the following 2 cases.
    invariant(curSize >= 0);
    uassert(mongo::ErrorCodes::InternalError,
            str::stream() << "Unexpected numPages value: " << numPages,
            numPages == 0 || numPages == 1);

    ScopeGuard guard([&] { _numAvailablePagesGauge->set(_availablePages.load()); });

    auto readLock = _mutex.readLock();
    auto it = _buffers.find(sourceBuffer);
    uassert(mongo::ErrorCodes::InternalError, "source buffer not found", it != _buffers.end());
    auto bufferInfo = it->second;

    stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);
    if (bufferInfo->size != curSize) {
        int32_t oldTotalPages = toPages(bufferInfo->size);
        int32_t newTotalPages = toPages(curSize);
        int32_t oldPagesAllocated = std::max(oldTotalPages - _sourceBufferPreallocatedPages, 0);
        int32_t newPagesAllocated = std::max(newTotalPages - _sourceBufferPreallocatedPages, 0);
        if (newPagesAllocated != oldPagesAllocated) {
            _availablePages.fetchAndAdd(oldPagesAllocated - newPagesAllocated);
        }
        bufferInfo->size = curSize;
    }

    if (numPages == 0) {
        // No additional page allocation requested.
        return true;
    }

    int32_t totalPages = toPages(bufferInfo->size);
    if (totalPages + 1 > toPages(_options.maxSourceBufferSize)) {
        // This source buffer is not allowed to grow any further.
        // If this source buffer has already grown beyond its fair share of global buffer space, it
        // will shrink as and when the data in this source buffer gets processed.
        return false;
    }

    if (_availablePages.load() < 0) {
        // This can happen immediately after a new source buffer registered with the buffer manager
        // or when a source buffer reports that it malloced significantly more memory than it had
        // allocated from the buffer manager.
        // Wait until _availablePages is >= 0 before allocating any pages (even any preallocated
        // pages) to any source buffers.
        return false;
    }

    if (totalPages + 1 <= _sourceBufferPreallocatedPages) {
        // This source buffer can use one more of its preallocated pages.
        bufferInfo->size += _pageSize;
        return true;
    }

    // This source buffer has already used all of its preallocated pages, try to allocate a page
    // out of _availablePages.
    if (_availablePages.subtractAndFetch(1) < 0) {
        // Buffer space allocation failed. Return the allocated page back to _availablePages.
        _availablePages.fetchAndAdd(1);
        return false;
    }

    // Buffer space allocation succeeded.
    bufferInfo->size += _pageSize;
    return true;
}

void SourceBufferManager::recomputePreallocatedPages(const mongo::WriteRarelyRWMutex::WriteLock&) {
    int32_t numBuffers = std::max<size_t>(_buffers.size(), 1);
    int64_t newSourceBufferPreallocatedBytes = _options.bufferTotalSize *
        static_cast<int32_t>(_options.bufferPreallocationFraction * 100) / (numBuffers * 100);
    newSourceBufferPreallocatedBytes =
        std::min(newSourceBufferPreallocatedBytes, _options.maxSourceBufferSize);

    // Compute new page size.
    int32_t newPageSize{_options.maxPageSize};
    if (newSourceBufferPreallocatedBytes < _options.maxPageSize) {
        if (newSourceBufferPreallocatedBytes >= _options.minPageSize) {
            // Choose the largest multiple of _options.minPageSize less than
            // newSourceBufferPreallocatedBytes as the new page size.
            newPageSize = newSourceBufferPreallocatedBytes -
                newSourceBufferPreallocatedBytes % _options.minPageSize;
        } else {
            newPageSize = _options.minPageSize;
        }
    }
    _pageSize = newPageSize;

    // We ensure that each source buffer is preallocated at least one page.
    uassert(mongo::ErrorCodes::InternalError,
            "Cannot preallocate even a single page to all the available source buffers",
            newSourceBufferPreallocatedBytes >= _pageSize);
    int32_t newSourceBufferPreallocatedPages = toPages(newSourceBufferPreallocatedBytes);
    uassert(mongo::ErrorCodes::InternalError,
            "newSourceBufferPreallocatedPages is not >= 1",
            newSourceBufferPreallocatedPages >= 1);

    _sourceBufferPreallocatedPages = newSourceBufferPreallocatedPages;
    _availablePages.store(std::max<int32_t>(
        toPages(_options.bufferTotalSize) - numBuffers * _sourceBufferPreallocatedPages, 0));

    // Recompute _availablePages due to the change in _sourceBufferPreallocatedPages.
    for (auto& [_, bufferInfo] : _buffers) {
        stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);

        int32_t totalPages = toPages(bufferInfo->size);
        int32_t pagesAllocated = std::max(totalPages - _sourceBufferPreallocatedPages, 0);
        if (pagesAllocated > 0) {
            _availablePages.subtractAndFetch(pagesAllocated);
        }
    }

    _numPreallocatedPagesGauge->set(_sourceBufferPreallocatedPages);
    _numAvailablePagesGauge->set(_availablePages.load());
}

int32_t SourceBufferManager::toPages(int64_t bytes) const {
    return (bytes + _pageSize - 1) / _pageSize;
}

}  // namespace streams
