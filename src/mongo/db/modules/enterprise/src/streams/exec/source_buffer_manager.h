/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/platform/rwmutex.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/concurrency/with_lock.h"
#include "streams/util/metric_manager.h"

namespace streams {

/**
 * This class manages memory allocations out of a global buffer space for source operators.
 * Source operators should call allocPages() method of this class before allocating more memory to
 * hold input docs.
 * This class is thread-safe.
 */
class SourceBufferManager {
public:
    struct Options {
        // The total byte size of the global buffer space.
        int64_t bufferTotalSize{800L * 1024 * 1024};
        // Fraction of global buffer space that is uniformly preallocated to registered source
        // buffers. All source buffers compete to allocate space out of the remaining buffer space.
        // Note that maxSourceBufferSize takes precedence over this option.
        // Also, we ensure that each source buffer is preallocated at least one page, so we may
        // exceed the specified preallocation ratio when there are too many source buffers.
        double bufferPreallocationFraction{0.5};
        // Maximum byte size allowed for a source buffer at any given time.
        // Ideally, this number should be <= 20% of bufferTotalSize so that one very busy source
        // buffer does not hog all the buffer space.
        int64_t maxSourceBufferSize{160L * 1024 * 1024};
        // Memory is allocated to source buffers in units of pages.
        // Following 2 options specify the min/max byte size of a page. SourceBufferManager starts
        // with max page size and reduces the page size as the number of source buffers increases
        // to ensure that at least one page can be preallocated for each source buffer.
        int32_t minPageSize{100 * 1024};
        int32_t maxPageSize{4 * 1024 * 1024};
        // MetricManager instance with which all the metrics are registered.
        MetricManager* metricManager{nullptr};
        // Labels to use for the metrics.
        MetricManager::LabelsVec metricLabels;
    };

    // An entity that represents a buffer created in the source operator to hold input docs.
    struct SourceBuffer {
        SourceBuffer(SourceBufferManager* bufferManager) : _bufferManager(bufferManager) {}

        SourceBufferManager* _bufferManager{nullptr};
    };

    // Deleter used to deregister a source buffer when it goes out of scope.
    class SourceBufferDeleter {
    public:
        void operator()(SourceBuffer* sourceBuffer) const;
    };

    // A handle returned to the caller when it registers itself with this class.
    using SourceBufferHandle = std::unique_ptr<SourceBuffer, SourceBufferDeleter>;

    SourceBufferManager(Options options);

    virtual ~SourceBufferManager();

    // Registers a source buffer with this class. The returned handle should be kept alive as long
    // as the caller intends to allocate memory for its source buffer.
    virtual SourceBufferHandle registerSourceBuffer();

    // Deregisters a source buffer with this class. The source buffer should not have any memory
    // allocated to it when this method is called.
    virtual void deregisterSourceBuffer(SourceBuffer* sourceBuffer);

    // Attempts to allocates a few pages for the given source buffer. Caller can also use this
    // method to simply report its current accurate memory usage and not allocate any pages.
    //
    // 'curSize' specifies the current total memory usage of the source buffer. If this is different
    // from the size known to the buffer manager, buffer manager updates the size for this source
    // buffer.
    // 'numPages' specifies the number of pages to allocate for the given source buffer. Currently,
    // we only support 0 or 1 value for this param.
    virtual bool allocPages(SourceBuffer* sourceBuffer, int64_t curSize, int32_t numPages);

    int32_t getPageSize() const {
        auto readLock = _mutex.readLock();
        return _pageSize;
    }

protected:
    // Test-only constructor.
    SourceBufferManager() = default;

private:
    friend class SourceBufferManagerTest;

    // Encapsulates all the metadata for a source buffer.
    struct SourceBufferInfo {
        // Guards all the member variables of this struct.
        // This mutex is never acquired before SourceBufferManager::_mutex.
        mongo::stdx::mutex mutex;

        // Current byte size of this buffer. This is an approximate value, the actual value may be
        // slightly smaller or larger.
        int64_t size{0};
    };

    // Registers metrics with the given MetricManager.
    void registerMetrics();

    // Recomputes the values of _sourceBufferPreallocatedPages and _availablePages after any source
    // buffers are registered or deregistered.
    void recomputePreallocatedPages(const mongo::WriteRarelyRWMutex::WriteLock&);

    // Returns the number of pages needed to store the given number of bytes.
    int32_t toPages(int64_t bytes) const;

    Options _options;
    // Pages available for allocation from the global buffer space. Pages are allocated from this
    // free pool only after a source buffer has already used all of its preallocated pages.
    mongo::AtomicWord<int32_t> _availablePages{0};
    // Guards access to the following 2 variables.
    mutable mongo::WriteRarelyRWMutex _mutex;
    // Current page size.
    int32_t _pageSize{4 * 1024 * 1024};
    // Number of pages preallocated to each source buffer.
    int32_t _sourceBufferPreallocatedPages{0};
    mongo::stdx::unordered_map<SourceBuffer*, std::shared_ptr<SourceBufferInfo>> _buffers;
    // Exports the count of source buffers.
    std::shared_ptr<IntGauge> _numSourceBuffersGauge;
    // Exports the count of pages preallocated to each source buffer.
    std::shared_ptr<IntGauge> _numPreallocatedPagesGauge;
    // Exports the count of pages available for allocation beyond the preallocated pages.
    std::shared_ptr<IntGauge> _numAvailablePagesGauge;
};

// No-op SourceBufferManager implementation for test-only purposes.
class NoOpSourceBufferManager : public SourceBufferManager {
public:
    NoOpSourceBufferManager() : SourceBufferManager() {}

    ~NoOpSourceBufferManager() override = default;

    SourceBufferHandle registerSourceBuffer() override {
        return SourceBufferHandle{};
    }

    void deregisterSourceBuffer(SourceBuffer* sourceBuffer) override {}

    bool allocPages(SourceBuffer* sourceBuffer, int64_t curSize, int32_t numPages) override {
        return true;
    }
};

}  // namespace streams
