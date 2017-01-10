/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <sstream>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/processinfo.h"

#include "../blockstore/http_client.h"
#include "../blockstore/reader.h"
#include "queryable_mmap_v1_extent_manager.h"

namespace mongo {
namespace queryable {
namespace {

class BlockstoreExtentManagerTest : public unittest::Test {};

class MockedHttpClient final : public HttpClientInterface {
public:
    StatusWith<std::size_t> read(std::string path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const override {
        std::string data(count, 0);

        for (std::size_t idx = 0; idx < count; ++idx) {
            std::size_t bytePosition = offset + idx;
            data[idx] = bytePosition % 256;
        }

        buf.write(ConstDataRange(data.c_str(), data.length()));

        return count;
    }

    StatusWith<DataBuilder> listDirectory() const override {
        return DataBuilder();
    }
};

TEST_F(BlockstoreExtentManagerTest, DataFileEnsureRange) {
    const std::size_t kNumBlocks = 10;
    const std::size_t kBlockSize = 1000;
    const std::size_t kFileSize = kNumBlocks * kBlockSize;
    // If `kPageSize` matches `kExpectedPageSize`, run some extra test conditions. On machines with
    // larger page sizes, ignore the extra assertions.
    const std::size_t kExpectedPageSize = 4 * 1024;
    const std::size_t kPageSize =
        std::max(kExpectedPageSize, static_cast<std::size_t>(ProcessInfo().getPageSize()));

    AllocState allocState(kFileSize);
    auto reader = stdx::make_unique<Reader>(
        stdx::make_unique<MockedHttpClient>(), "file.old", kFileSize, kBlockSize);
    DataFile dataFile(std::move(reader), &allocState, kPageSize);
    ASSERT_EQ(kPageSize, dataFile.getPageSize());

    for (auto pageItr = dataFile.getMappedPages().begin();
         pageItr != dataFile.getMappedPages().end();
         pageItr++) {
        ASSERT_FALSE(*pageItr);
    }

    for (auto blockItr = dataFile.getMappedBlocks().begin();
         blockItr != dataFile.getMappedBlocks().end();
         blockItr++) {
        ASSERT_FALSE(*blockItr);
    }

    // Ensuring byte 999 should mmap the first page and only write in data for the first block.
    uassertStatusOK(dataFile.ensureRange(kBlockSize - 1, 1));
    ASSERT_TRUE(dataFile.getMappedPages()[0]);
    ASSERT_FALSE(dataFile.getMappedPages()[1]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[0]);
    ASSERT_FALSE(dataFile.getMappedBlocks()[1]);
    ASSERT_EQ(static_cast<char>(999 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 999));

    // Ensuring 2 bytes starting at 999 should pull in the second block.
    uassertStatusOK(dataFile.ensureRange(kBlockSize - 1, 2));
    ASSERT_FALSE(dataFile.getMappedPages()[1]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[1]);
    ASSERT_EQ(static_cast<char>(1000 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 1000));

    // Ensuring on 1000 bytes starting at offset 1000 should not* pull in the third block.
    uassertStatusOK(dataFile.ensureRange(kBlockSize, kBlockSize));
    ASSERT_FALSE(dataFile.getMappedBlocks()[2]);

    // Ensuring the fifth block should mmap the second 4096 sized page.
    uassertStatusOK(dataFile.ensureRange(4 * kBlockSize, kBlockSize + 1));
    ASSERT_FALSE(dataFile.getMappedBlocks()[3]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[4]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[5]);
    if (kPageSize == kExpectedPageSize) {
        ASSERT_TRUE(dataFile.getMappedPages()[1]);
    }
    ASSERT_EQ(static_cast<char>(4000 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 4000));
    ASSERT_EQ(static_cast<char>(5999 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 5999));
}

TEST_F(BlockstoreExtentManagerTest, AllocStateTest) {
    const std::uint64_t kAllocationLimitBytes = 10000;
    AllocState allocState(kAllocationLimitBytes);
    ASSERT_EQ(kAllocationLimitBytes, allocState.getMemoryQuotaBytes());

    // `allocPage` only uses `DataFile` pointers for accounting as the keys in a map. Constructing a
    // real `DataFile` here would be cumbersome and provide no value in this test.
    DataFile* dummyFileOne = reinterpret_cast<DataFile*>(0x342310);
    DataFile* dummyFileTwo = reinterpret_cast<DataFile*>(0x342320);
    // Declare that 4 pages are successfully allocated.
    allocState.allocPage(dummyFileOne, 0);
    allocState.allocPage(dummyFileOne, 1);
    allocState.allocPage(dummyFileTwo, 0);
    allocState.allocPage(dummyFileTwo, 1);

    // Observe the pages are allocated.
    ASSERT_EQ(4U, allocState.getNumPagesAllocated());
    // Also note that `allocBlock` hasn't been called, thus "memory" has not been allocated.
    ASSERT_EQ(0U, allocState.getMemoryAllocated());

    // Now evict each page with four `selectPageForFree` calls followed by `freePage`.
    for (int pageToFree = 0; pageToFree < 4; ++pageToFree) {
        // Reset values after each run to make sure `selectPageForFree` found a correct value.
        DataFile* dataFile = nullptr;
        std::size_t pageIdx = 100;

        allocState.selectPageForFree(&dataFile, &pageIdx);
        // Decided against a more thorough check that enforces each result is seen exactly once.
        ASSERT_TRUE(dataFile == dummyFileOne || dataFile == dummyFileTwo);
        ASSERT_TRUE(pageIdx == 0 || pageIdx == 1);

        allocState.freePage(dataFile, pageIdx);
        ASSERT_EQ(static_cast<std::size_t>(4 - (pageToFree + 1)),
                  allocState.getNumPagesAllocated());
    }

    // The first allocation to exceed the limit is allowed.
    ASSERT_TRUE(allocState.allocBlock(2 * kAllocationLimitBytes).isOK());
    // But once the limit is reached, future allocations are disallowed.
    ASSERT_FALSE(allocState.allocBlock(1).isOK());
    // Free the original allocation.
    allocState.freeBlock(2 * kAllocationLimitBytes);

    // Repeat with a different allocation pattern.
    ASSERT_TRUE(allocState.allocBlock(kAllocationLimitBytes / 2).isOK());
    ASSERT_TRUE(allocState.allocBlock(kAllocationLimitBytes).isOK());
    ASSERT_FALSE(allocState.allocBlock(1).isOK());
}

TEST_F(BlockstoreExtentManagerTest, DataFileEnsureRangeWithAllocLimits) {
    const std::size_t kExpectedPageSize = 4 * 1024;
    const std::size_t kPageSize =
        std::max(kExpectedPageSize, static_cast<std::size_t>(ProcessInfo().getPageSize()));
    const std::size_t kNumPages = 2;
    const std::size_t kFileSize = kPageSize * kNumPages;
    const std::size_t kNumBlocks = 4;
    const std::size_t kBlockSize = kFileSize / kNumBlocks;

    // Create a "file" of two pages, each page consisting of two blocks (four blocks total). Make
    // the allocation limit three blocks. Note the comments will speak as if the page size is 4KB
    // as I think concrete numbers are simpler to follow.
    const std::uint64_t kAllocLimitBytes = 3 * kBlockSize;
    AllocState allocState(kAllocLimitBytes);
    auto reader = stdx::make_unique<Reader>(
        stdx::make_unique<MockedHttpClient>(), "file.old", kFileSize, kBlockSize);
    DataFile dataFile(std::move(reader), &allocState, kPageSize);
    ASSERT_EQ(kPageSize, dataFile.getPageSize());

    // Allocate three blocks, bringing us right to our limit.
    ASSERT_TRUE(dataFile.ensureRange(0, kAllocLimitBytes).isOK());
    ASSERT_EQ(2U, allocState.getNumPagesAllocated());
    ASSERT_EQ(kAllocLimitBytes, allocState.getMemoryAllocated());
    // Ensuring one byte not in the previous range should throw a `WriteConflictException`.
    ASSERT_THROWS(dataFile.ensureRange(kAllocLimitBytes, 1), WriteConflictException);

    // Releasing the 0th page (0->4K by default) leaves the 1st page (4K->8K) allocated. Frees
    // two blocks (4K worth of data).
    ASSERT_TRUE(dataFile.releasePage(0).isOK());
    ASSERT_EQ(1U, allocState.getNumPagesAllocated());
    ASSERT_EQ(kBlockSize, allocState.getMemoryAllocated());

    // Ensure one more byte which should succeed.
    ASSERT_TRUE(dataFile.ensureRange(kAllocLimitBytes, 1).isOK());
    // This range does not need any new pages allocated.
    ASSERT_EQ(1U, allocState.getNumPagesAllocated());
    // However the ensure does not write one byte of data, but rather the entire block.
    ASSERT_EQ(2U * kBlockSize, allocState.getMemoryAllocated());

    // Fail to allocate the entire first page again.
    ASSERT_THROWS(dataFile.ensureRange(0, kPageSize), WriteConflictException);
    // Despite failing, the first block should have succeeded along with allocating the 0th page.
    ASSERT_EQ(2U, allocState.getNumPagesAllocated());
    ASSERT_TRUE(dataFile.getMappedBlocks()[0]);
    ASSERT_FALSE(dataFile.getMappedBlocks()[1]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[2]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[3]);
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
