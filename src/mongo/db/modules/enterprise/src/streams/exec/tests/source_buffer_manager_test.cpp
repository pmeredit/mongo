/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/util/str.h"
#include <cmath>
#include <memory>

#include "mongo/unittest/assert.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/source_buffer_manager.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

namespace {
int randomInt(mongo::PseudoRandom& random, int min, int max) {
    dassert(max > min);
    // nextInt32 returns a half open [0, max) interval so we add 1 to max.
    auto randomValue = random.nextInt32(max + 1 - min);
    return randomValue + min;
}
}  // namespace

class SourceBufferManagerTest : public unittest::Test {
public:
    SourceBufferManagerTest() : _metricManager(std::make_unique<MetricManager>()) {}

    void createSourceBufferManager(SourceBufferManager::Options options) {
        options.metricManager = _metricManager.get();
        _bufMgr = std::make_unique<SourceBufferManager>(std::move(options));
    }

    auto getSourceBufferInfo(SourceBufferManager::SourceBuffer* sourceBuffer) {
        auto readLock = _bufMgr->_mutex.readLock();
        return _bufMgr->_buffers[sourceBuffer];
    }

    int32_t getAvailablePages() const {
        return _bufMgr->_availablePages.load();
    }

    int32_t getSourceBufferPreallocatedPages() const {
        auto readLock = _bufMgr->_mutex.readLock();
        return _bufMgr->_sourceBufferPreallocatedPages;
    }

    int32_t toPages(int64_t bytes) const {
        return _bufMgr->toPages(bytes);
    }

    void assertNumSourceBuffersEquals(int64_t value) const {
        auto readLock = _bufMgr->_mutex.readLock();
        ASSERT_EQUALS(_bufMgr->_buffers.size(), value);
        ASSERT_EQUALS(_bufMgr->_numSourceBuffersGauge->value(), value);
    }

    void assertPreallocatedPagesEquals(int32_t value) const {
        auto readLock = _bufMgr->_mutex.readLock();
        ASSERT_EQUALS(_bufMgr->_sourceBufferPreallocatedPages, value);
        ASSERT_EQUALS(_bufMgr->_numPreallocatedPagesGauge->value(), value);
    }

    void assertAvailablePagesEquals(int32_t value) const {
        ASSERT_EQUALS(_bufMgr->_availablePages.load(), value);
        ASSERT_EQUALS(_bufMgr->_numAvailablePagesGauge->value(), value);
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<SourceBufferManager> _bufMgr;
};

TEST_F(SourceBufferManagerTest, Initialization) {
    SourceBufferManager::Options options;
    options.bufferTotalSize = 800;
    options.bufferPreallocationFraction = 0.5;
    options.maxSourceBufferSize = 600;
    options.minPageSize = 16;
    options.maxPageSize = 16;
    createSourceBufferManager(options);
    assertPreallocatedPagesEquals(800 * 0.5 / 16);
    assertAvailablePagesEquals(800 / 16 - getSourceBufferPreallocatedPages());

    // Change bufferPreallocationFraction from 0.5 to 0.7.
    options.bufferTotalSize = 850;
    options.bufferPreallocationFraction = 0.7;
    createSourceBufferManager(options);
    assertPreallocatedPagesEquals(ceill(850 * 0.7 / 16));
    assertAvailablePagesEquals(ceill(850.0 / 16) - getSourceBufferPreallocatedPages());

    // Change maxSourceBufferSize from 600 to 200.
    // Verify that maxSourceBufferSize is honored when determining the number of preallocated pages.
    options.maxSourceBufferSize = 200;
    createSourceBufferManager(options);
    assertPreallocatedPagesEquals(ceill(200.0 / 16));
    assertAvailablePagesEquals(ceill(850.0 / 16) - getSourceBufferPreallocatedPages());
}

TEST_F(SourceBufferManagerTest, RegisterDeregister) {
    // Test that we are able to register as many source buffers as permitted by the configured
    // minPageSize.

    SourceBufferManager::Options options;
    options.bufferTotalSize = 800;
    options.bufferPreallocationFraction = 0.5;
    options.maxSourceBufferSize = 600;
    options.minPageSize = 2;
    options.maxPageSize = 100;
    createSourceBufferManager(options);

    // Register 200 source buffers and verify that they get expected number of pages preallocated to
    // them.
    std::vector<SourceBufferManager::SourceBufferHandle> handles;
    handles.reserve(200);
    for (int i = 0; i < 200; ++i) {
        handles.push_back(_bufMgr->registerSourceBuffer());

        auto pageSize = _bufMgr->getPageSize();
        int32_t numExpectedPreallocatedBytes = ceill(int64_t(
            options.bufferTotalSize * options.bufferPreallocationFraction / handles.size()));
        if (numExpectedPreallocatedBytes >= options.maxPageSize) {
            ASSERT_EQ(pageSize, options.maxPageSize);
        } else {
            ASSERT_TRUE(pageSize <= numExpectedPreallocatedBytes);
            ASSERT_TRUE(pageSize + options.minPageSize >= numExpectedPreallocatedBytes);
        }
        int32_t numExpectedPreallocatedPages = toPages(numExpectedPreallocatedBytes);
        numExpectedPreallocatedPages =
            std::min(numExpectedPreallocatedPages, toPages(options.maxSourceBufferSize));
        assertPreallocatedPagesEquals(numExpectedPreallocatedPages);
        assertAvailablePagesEquals(toPages(options.bufferTotalSize) -
                                   handles.size() * getSourceBufferPreallocatedPages());
        ASSERT_EQUALS(toPages(options.bufferTotalSize),
                      handles.size() * getSourceBufferPreallocatedPages() + getAvailablePages());
    }
    ASSERT_EQUALS(handles.size(), 200);

    // Verify that we cannot register any more source buffers due to the options we provided above.
    ASSERT_THROWS_CODE_AND_WHAT(
        _bufMgr->registerSourceBuffer(),
        DBException,
        ErrorCodes::InternalError,
        "Cannot preallocate even a single page to all the available source buffers");

    assertNumSourceBuffersEquals(200);

    // Deregister all source buffers and verify that the remaining source buffers get expected
    // number of pages preallocated to them.
    for (int i = 0; i < 200; ++i) {
        { handles.pop_back(); }

        auto numHandles = std::max<size_t>(handles.size(), 1);
        auto pageSize = _bufMgr->getPageSize();
        int32_t numExpectedPreallocatedBytes = ceill(
            int64_t(options.bufferTotalSize * options.bufferPreallocationFraction / numHandles));
        if (numExpectedPreallocatedBytes >= options.maxPageSize) {
            ASSERT_EQ(pageSize, options.maxPageSize);
        } else {
            ASSERT_TRUE(pageSize <= numExpectedPreallocatedBytes);
            ASSERT_TRUE(pageSize + options.minPageSize >= numExpectedPreallocatedBytes);
        }
        int32_t numExpectedPreallocatedPages = toPages(numExpectedPreallocatedBytes);
        numExpectedPreallocatedPages =
            std::min(numExpectedPreallocatedPages, toPages(options.maxSourceBufferSize));
        assertPreallocatedPagesEquals(numExpectedPreallocatedPages);
        assertAvailablePagesEquals(toPages(options.bufferTotalSize) -
                                   numHandles * getSourceBufferPreallocatedPages());
        ASSERT_EQUALS(toPages(options.bufferTotalSize),
                      numHandles * getSourceBufferPreallocatedPages() + getAvailablePages());
    }
    ASSERT_TRUE(handles.empty());
}

TEST_F(SourceBufferManagerTest, AllocPagesFails) {
    // Test that allocPages() does not allocate memory beyond minPageSize when too many source
    // buffers are registered with the SourceBufferManager.

    SourceBufferManager::Options options;
    options.bufferTotalSize = 400;
    options.bufferPreallocationFraction = 1.0;
    options.maxSourceBufferSize = 400;
    options.minPageSize = 20;
    options.maxPageSize = 200;
    createSourceBufferManager(options);

    // Register a source buffer.
    auto handle1 = _bufMgr->registerSourceBuffer();
    auto bufferInfo1 = getSourceBufferInfo(handle1.get());
    ASSERT_EQ(options.maxPageSize, _bufMgr->getPageSize());
    assertPreallocatedPagesEquals(2 /* 400 / 200 */);
    assertAvailablePagesEquals(0 /* 400 / 200 - getSourceBufferPreallocatedPages() */);

    // Register 19 more source buffers permitted by the minPageSize of 20.
    std::vector<SourceBufferManager::SourceBufferHandle> handles;
    handles.reserve(19);
    for (int i = 0; i < 19; ++i) {
        handles.push_back(_bufMgr->registerSourceBuffer());
    }
    ASSERT_EQ(options.minPageSize, _bufMgr->getPageSize());

    // Now verify that the first source buffer can allocate any more pages beyond its 20 bytes of
    // preallocation.
    ASSERT_FALSE(
        _bufMgr->allocPages(handle1.get(), options.minPageSize /* curSize */, 1 /* numPages */));
    // Deallocate all memory to exit gracefully.
    _bufMgr->allocPages(handle1.get(), 0 /* curSize */, 0 /* numPages */);
}

TEST_F(SourceBufferManagerTest, AllocPagesTwoSourceBuffers) {
    SourceBufferManager::Options options;
    options.bufferTotalSize = 160;
    options.bufferPreallocationFraction = 0.5;
    options.maxSourceBufferSize = 160;
    options.minPageSize = 16;
    options.maxPageSize = 16;
    createSourceBufferManager(options);
    assertPreallocatedPagesEquals(5 /* 160 * 0.5 / 16 */);
    assertAvailablePagesEquals(5 /* 160 / 16 - getSourceBufferPreallocatedPages() */);

    // Register a source buffer.
    auto handle1 = _bufMgr->registerSourceBuffer();
    auto bufferInfo1 = getSourceBufferInfo(handle1.get());
    assertPreallocatedPagesEquals(5 /* 160 * 0.5 / 16 */);
    assertAvailablePagesEquals(5 /* 160 / 16 - getSourceBufferPreallocatedPages() */);

    // Call allocPages() with numPages=0 to simply report curSize=0.
    ASSERT_TRUE(_bufMgr->allocPages(handle1.get(), 0 /* curSize */, 0 /* numPages */));
    {
        stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
        ASSERT_EQUALS(bufferInfo1->size, 0);
    }
    assertAvailablePagesEquals(5);

    // Call allocPages() to allocate all preallocated pages.
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), i * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
            ASSERT_EQUALS(bufferInfo1->size, (i + 1) * options.maxPageSize);
        }
        assertAvailablePagesEquals(5);
    }

    // Now call allocPages() to allocate beyond the preallocated pages.
    for (int i = 5; i < 10; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), i * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
            ASSERT_EQUALS(bufferInfo1->size, (i + 1) * options.maxPageSize);
        }
        assertAvailablePagesEquals(10 - i - 1);
    }

    // Verify that no more pages can be allocated by this source buffer.
    ASSERT_FALSE(_bufMgr->allocPages(
        handle1.get(), 10 * options.maxPageSize /* curSize */, 1 /* numPages */));

    // Register a second source buffer now.
    auto handle2 = _bufMgr->registerSourceBuffer();
    auto bufferInfo2 = getSourceBufferInfo(handle2.get());
    assertPreallocatedPagesEquals(3 /* 160 * 0.5 / 2 / 16 */);
    // Source buffer 1 already allocated 10 pages and 3 pages are now prellocated to the
    // source buffer 2. So the count of available pages should be -3.
    assertAvailablePagesEquals(-3);

    // Verify that no pages can be allocated by any of the source buffers until source buffer 1
    // deallocates the extra pages it allocated.
    ASSERT_FALSE(_bufMgr->allocPages(
        handle1.get(), 10 * options.maxPageSize /* curSize */, 1 /* numPages */));
    ASSERT_FALSE(_bufMgr->allocPages(handle2.get(), 0 /* curSize */, 1 /* numPages */));

    // Source buffer 1 calls allocPages() to deallocate 3 out of 10 pages.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), (9 - i) * options.maxPageSize /* curSize */, 0 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
            ASSERT_EQUALS(bufferInfo1->size, (9 - i) * options.maxPageSize);
        }
        assertAvailablePagesEquals(-3 + i + 1);
    }
    assertAvailablePagesEquals(0);

    // Verify that the source buffer 1 still cannot allocate any more pages.
    ASSERT_FALSE(_bufMgr->allocPages(
        handle1.get(), 7 * options.maxPageSize /* curSize */, 1 /* numPages */));

    // Verify that source buffer 2 can allocate all 3 of its preallocated pages.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle2.get(), i * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo2->mutex);
            ASSERT_EQUALS(bufferInfo2->size, (i + 1) * options.maxPageSize);
        }
        assertAvailablePagesEquals(0);
    }

    // Verify that the source buffer 1 can now deallocate one of its 7 pages and reallocate it.
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), 6 * options.maxPageSize /* curSize */, 0 /* numPages */));
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), 6 * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
            ASSERT_EQUALS(bufferInfo1->size, 7 * options.maxPageSize);
        }
        assertAvailablePagesEquals(0);
    }

    // Verify that the source buffer 2 cannot allocate any more pages until source buffer 1
    // deallocates more of its pages.
    ASSERT_FALSE(_bufMgr->allocPages(
        handle2.get(), 3 * options.maxPageSize /* curSize */, 1 /* numPages */));

    // Source buffer 1 calls allocPages() to deallocate 2 more pages.
    ASSERT_TRUE(_bufMgr->allocPages(
        handle1.get(), 5 * options.maxPageSize /* curSize */, 0 /* numPages */));
    {
        stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
        ASSERT_EQUALS(bufferInfo1->size, 5 * options.maxPageSize);
    }
    assertAvailablePagesEquals(2);

    // Verify that the source buffer 2 can now allocate 2 more pages.
    for (int i = 3; i < 5; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle2.get(), i * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo2->mutex);
            ASSERT_EQUALS(bufferInfo2->size, (i + 1) * options.maxPageSize);
        }
    }
    assertAvailablePagesEquals(0);

    // Verify that both source buffers can now deallocate one of their pages and reallocate them.
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), 4 * options.maxPageSize /* curSize */, 0 /* numPages */));
        ASSERT_TRUE(_bufMgr->allocPages(
            handle1.get(), 4 * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
            ASSERT_EQUALS(bufferInfo1->size, 5 * options.maxPageSize);
        }

        ASSERT_TRUE(_bufMgr->allocPages(
            handle2.get(), 4 * options.maxPageSize /* curSize */, 0 /* numPages */));
        ASSERT_TRUE(_bufMgr->allocPages(
            handle2.get(), 4 * options.maxPageSize /* curSize */, 1 /* numPages */));
        {
            stdx::lock_guard<stdx::mutex> lk(bufferInfo2->mutex);
            ASSERT_EQUALS(bufferInfo2->size, 5 * options.maxPageSize);
        }
        assertAvailablePagesEquals(0);
    }

    // Deallocate all memory to exit gracefully.
    _bufMgr->allocPages(handle1.get(), 0 /* curSize */, 0 /* numPages */);
    {
        stdx::lock_guard<stdx::mutex> lk(bufferInfo1->mutex);
        ASSERT_EQUALS(bufferInfo1->size, 0);
    }
    assertAvailablePagesEquals(2);
    _bufMgr->allocPages(handle2.get(), 0 /* curSize */, 0 /* numPages */);
    {
        stdx::lock_guard<stdx::mutex> lk(bufferInfo2->mutex);
        ASSERT_EQUALS(bufferInfo2->size, 0);
    }
    assertAvailablePagesEquals(4);
}

TEST_F(SourceBufferManagerTest, AllocPagesAlwaysSuccessful) {
    // Initialize buffer manager such with buffer space of 1000 pages.
    SourceBufferManager::Options options;
    options.bufferTotalSize = 16000;
    options.bufferPreallocationFraction = 0.5;
    options.maxSourceBufferSize = 160;
    options.minPageSize = 16;
    options.maxPageSize = 16;
    createSourceBufferManager(options);
    assertPreallocatedPagesEquals(10);
    assertAvailablePagesEquals(990 /* 16000 / 16 - getSourceBufferPreallocatedPages() */);

    // Create 100 source buffers on 100 threads and make them perform a bunch of allocations and
    // deallocations while staying within 10 page per source buffer limit. Since there is enough
    // space in the buffer to allocate 10 pages per source buffer, all allocation requests by these
    // threads should succeed.
    std::vector<stdx::thread> threads;
    for (int threadId = 0; threadId < 100; ++threadId) {
        threads.emplace_back([this, options, threadId]() {
            auto handle = _bufMgr->registerSourceBuffer();
            auto bufferInfo = getSourceBufferInfo(handle.get());
            int64_t numPagesAllocated{0};
            int64_t numAllocations{0}, numDeallocations{0};
            mongo::PseudoRandom random(threadId);
            for (int i = 0; i < 100000; ++i) {
                bool shouldAllocate = randomInt(random, 0, 5) > 0;
                if (shouldAllocate) {
                    if (numPagesAllocated == 10) {
                        shouldAllocate = false;
                    }
                } else {
                    if (numPagesAllocated == 0) {
                        shouldAllocate = true;
                    }
                }

                if (shouldAllocate) {
                    ++numAllocations;
                    // Allocate a page.
                    ASSERT_TRUE(
                        _bufMgr->allocPages(handle.get(),
                                            numPagesAllocated * options.maxPageSize /* curSize */,
                                            1 /* numPages */));
                    ++numPagesAllocated;
                } else {
                    ++numDeallocations;
                    // Deallocate up to 5 pages.
                    int64_t numPagesToDeallocate = std::min<int64_t>(numPagesAllocated, 5);
                    numPagesAllocated -= numPagesToDeallocate;
                    ASSERT_TRUE(
                        _bufMgr->allocPages(handle.get(),
                                            numPagesAllocated * options.maxPageSize /* curSize */,
                                            0 /* numPages */));
                }

                {
                    stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);
                    ASSERT_EQUALS(bufferInfo->size, numPagesAllocated * options.maxPageSize);
                }
            }

            // Deallocate all memory to exit gracefully.
            _bufMgr->allocPages(handle.get(), 0 /* curSize */, 0 /* numPages */);
            {
                stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);
                ASSERT_EQUALS(bufferInfo->size, 0);
            }
            std::cout << str::stream() << "thread " << threadId
                      << " numAllocations: " << numAllocations
                      << " numDeallocations: " << numDeallocations << std::endl;
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    assertAvailablePagesEquals(990);
}

TEST_F(SourceBufferManagerTest, AllocPagesFailsSometimes) {
    // Run this test with different values for bufferPreallocationFraction.
    for (float fraction = 0.1; fraction <= 1; fraction += 0.1) {
        // Initialize buffer manager such with buffer space of 1000 pages.
        SourceBufferManager::Options options;
        options.bufferTotalSize = 16000;
        options.bufferPreallocationFraction = fraction;
        options.maxSourceBufferSize = 1600;
        options.minPageSize = 16;
        options.maxPageSize = 16;
        createSourceBufferManager(options);
        auto origAvailablePages = getAvailablePages();

        // Create 100 source buffers on 100 threads and make them perform a bunch of allocations and
        // deallocations while staying within 50 page per source buffer limit. Since there is not
        // enough space in the buffer to allocate 50 pages per source buffer, some allocation
        // requests by these threads would fail.
        std::vector<stdx::thread> threads;
        for (int threadId = 0; threadId < 100; ++threadId) {
            threads.emplace_back([this, options, threadId]() {
                auto handle = _bufMgr->registerSourceBuffer();
                auto bufferInfo = getSourceBufferInfo(handle.get());
                int64_t numPagesAllocated{0};
                int64_t maxNumPagesAllocated{0};
                int64_t numAllocationAttempts{0}, numSuccessfulAllocations{0}, numDeallocations{0};
                mongo::PseudoRandom random(threadId);
                for (int i = 0; i < 100000; ++i) {
                    bool shouldAllocate = randomInt(random, 0, 5) > 0;
                    if (shouldAllocate) {
                        if (numPagesAllocated == 50) {
                            shouldAllocate = false;
                        }
                    } else {
                        if (numPagesAllocated == 0) {
                            shouldAllocate = true;
                        }
                    }

                    if (shouldAllocate) {
                        ++numAllocationAttempts;
                        // Allocate a page.
                        if (_bufMgr->allocPages(handle.get(),
                                                numPagesAllocated *
                                                    options.maxPageSize /* curSize */,
                                                1 /* numPages */)) {
                            ++numSuccessfulAllocations;
                            ++numPagesAllocated;
                            maxNumPagesAllocated =
                                std::max(maxNumPagesAllocated, numPagesAllocated);
                        }
                    } else {
                        ++numDeallocations;
                        // Deallocate up to 5 pages.
                        int64_t numPagesToDeallocate = std::min<int64_t>(numPagesAllocated, 5);
                        numPagesAllocated -= numPagesToDeallocate;
                        ASSERT_TRUE(_bufMgr->allocPages(handle.get(),
                                                        numPagesAllocated *
                                                            options.maxPageSize /* curSize */,
                                                        0 /* numPages */));
                    }

                    {
                        stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);
                        ASSERT_EQUALS(bufferInfo->size, numPagesAllocated * options.maxPageSize);
                    }
                }

                // Deallocate all memory to exit gracefully.
                _bufMgr->allocPages(handle.get(), 0 /* curSize */, 0 /* numPages */);
                {
                    stdx::lock_guard<stdx::mutex> lk(bufferInfo->mutex);
                    ASSERT_EQUALS(bufferInfo->size, 0);
                }
                std::cout << str::stream()
                          << "bufferPreallocationFraction: " << options.bufferPreallocationFraction
                          << " thread " << threadId
                          << " numAllocationAttempts: " << numAllocationAttempts
                          << " numSuccessfulAllocations: " << numSuccessfulAllocations
                          << " numDeallocations: " << numDeallocations
                          << " maxNumPagesAllocated: " << maxNumPagesAllocated << std::endl;
            });
        }

        for (auto& t : threads) {
            t.join();
        }
        assertAvailablePagesEquals(origAvailablePages);
    }
}

}  // namespace streams
