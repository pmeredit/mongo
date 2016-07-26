/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <sstream>

#include "mongo/stdx/memory.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"

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
    const std::size_t numBlocks = 10;
    const std::size_t blocksize = 1000;
    const std::size_t filesize = numBlocks * blocksize;
    const std::size_t pagesize = 4 * 1024;

    auto reader = stdx::make_unique<Reader>(
        stdx::make_unique<MockedHttpClient>(), "file.old", filesize, blocksize);
    DataFile dataFile(std::move(reader), pagesize);
    ASSERT_EQ(pagesize, dataFile.getPageSize());

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
    uassertStatusOK(dataFile.ensureRange(blocksize - 1, 1));
    ASSERT_TRUE(dataFile.getMappedPages()[0]);
    ASSERT_FALSE(dataFile.getMappedPages()[1]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[0]);
    ASSERT_FALSE(dataFile.getMappedBlocks()[1]);
    ASSERT_EQ(static_cast<char>(999 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 999));

    // Ensuring 2 bytes starting at 999 should pull in the second block.
    uassertStatusOK(dataFile.ensureRange(blocksize - 1, 2));
    ASSERT_FALSE(dataFile.getMappedPages()[1]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[1]);
    ASSERT_EQ(static_cast<char>(1000 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 1000));

    // Ensuring on 1000 bytes starting at offset 1000 should not* pull in the third block.
    uassertStatusOK(dataFile.ensureRange(blocksize, blocksize));
    ASSERT_FALSE(dataFile.getMappedBlocks()[2]);

    // Ensuring the fifth block should mmap the second 4096 sized page.
    uassertStatusOK(dataFile.ensureRange(4 * blocksize, blocksize + 1));
    ASSERT_FALSE(dataFile.getMappedBlocks()[3]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[4]);
    ASSERT_TRUE(dataFile.getMappedBlocks()[5]);
    ASSERT_TRUE(dataFile.getMappedPages()[1]);
    ASSERT_EQ(static_cast<char>(4000 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 4000));
    ASSERT_EQ(static_cast<char>(5999 % 256), *(static_cast<char*>(dataFile.getBasePtr()) + 5999));
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
