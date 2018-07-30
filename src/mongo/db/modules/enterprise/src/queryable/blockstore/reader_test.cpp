/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <iostream>
#include <sstream>

#include "mongo/stdx/memory.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/mongoutils/str.h"

#include "reader.h"

namespace mongo {
namespace queryable {
namespace {

class ReaderTest : public unittest::Test {};

class MockedHttpClient final : public HttpClient {
public:
    DataBuilder post(StringData, ConstDataRange) const final {
        invariant(false);
        return DataBuilder();
    }

    DataBuilder get(StringData url) const final {
        // This would be better with a proper URI parser,
        // but we're tightly coupled to blockstore http,
        // so just pull out the values the hard way.
        const auto urlString = url.toString();
        const char* offsetP = strstr(urlString.c_str(), "&offset=");
        const char* lengthP = strstr(urlString.c_str(), "&length=");
        invariant(offsetP && lengthP);
        auto offset = std::atoi(offsetP + strlen("&offset="));
        auto length = std::atoi(lengthP + strlen("&length="));

        // Block of 256 characters suitable for writing.
        std::array<char, 256> buffer;
        for (std::size_t i = 0; i < buffer.size(); ++i) {
            buffer[i] = static_cast<char>(i);
        }

        DataBuilder builder;

        // Align up to block boundary first.
        if (offset % 256) {
            const auto rel = offset % 256;
            const auto len = std::min<std::size_t>(length, 256 - rel);
            uassertStatusOK(builder.writeAndAdvance(ConstDataRange(buffer.data() + rel, len)));
            length -= len;
        }

        // Push whole blocks of 256 bytes.
        if (length >= 256) {
            ConstDataRange block(buffer.data(), buffer.size());
            while (length >= 256) {
                uassertStatusOK(builder.writeAndAdvance(block));
                length -= 256;
            }
        }

        if (length > 0) {
            ConstDataRange remain(buffer.data(), length);
            uassertStatusOK(builder.writeAndAdvance(remain));
        }

        return builder;
    }

    // Ignore client config.
    void allowInsecureHTTP(bool) final {}
    void setHeaders(const std::vector<std::string>&) final {}
};

TEST_F(ReaderTest, TestReadInto) {
    const std::size_t fileSize = 44 * 1000;
    // Test when the blockSize (relative to the fileSize):
    // 1) is small and evenly divides the file
    // 2) is small and leaves a remainder
    // 3) is the same size as the file
    // 4) is larger than the file
    for (auto blockSize : std::vector<std::size_t>{4 * 1000, 10 * 1000, 44 * 1000, 50 * 1000}) {
        BlockstoreHTTP blockstore("", mongo::OID(), std::make_unique<MockedHttpClient>());
        Reader reader(std::move(blockstore), "file", fileSize, blockSize);
        std::ostringstream buf;
        auto status = reader.readInto(&buf);
        ASSERT_EQ(Status::OK(), status);
        std::string res = buf.str();
        ASSERT_EQ(fileSize, res.size());
        for (std::size_t idx = 0; idx < res.size(); ++idx) {
            ASSERT_EQ(static_cast<char>(idx % 256), res[idx]);
        }
    }
}

TEST_F(ReaderTest, TestReadBlockInto) {
    const std::size_t fileSize = 9400;
    const std::size_t blockSize = 1000;
    std::array<char, blockSize> data;

    BlockstoreHTTP blockstore("", mongo::OID(), std::make_unique<MockedHttpClient>());
    Reader reader(std::move(blockstore), "file", fileSize, blockSize);
    // For each of the first nine full blocks:
    for (std::size_t blockIdx = 0; blockIdx < fileSize / blockSize; ++blockIdx) {
        DataRange buf(data.data(), data.data() + blockSize);
        auto swBytesRead = reader.readBlockInto(buf, blockIdx);
        ASSERT_EQ(Status::OK(), swBytesRead.getStatus());
        ASSERT_EQ(blockSize, swBytesRead.getValue());
        for (std::size_t idx = 0; idx < blockSize; ++idx) {
            const std::size_t offset = (blockIdx * blockSize) + idx;
            ASSERT_EQ(static_cast<char>(offset % 256), data[idx]);
        }
    }

    // The last block should only read 400 bytes
    DataRange buf(data.data(), data.data() + 1000);
    const auto lastBlockIdx = fileSize / blockSize;
    auto swBytesRead = reader.readBlockInto(buf, lastBlockIdx);
    ASSERT_EQ(Status::OK(), swBytesRead.getStatus());
    ASSERT_EQ(fileSize % blockSize, swBytesRead.getValue());
    for (std::size_t idx = 0; idx < swBytesRead.getValue(); ++idx) {
        const std::size_t offset = (lastBlockIdx * blockSize) + idx;
        ASSERT_EQ(static_cast<char>(offset % 256), data[idx]);
    }

    // Test the case where a buffer smaller than a full block is still sufficient for a small block.
    buf = DataRange(data.data(), data.data() + 500);
    swBytesRead = reader.readBlockInto(buf, lastBlockIdx);
    ASSERT_EQ(Status::OK(), swBytesRead.getStatus());
    ASSERT_EQ(fileSize % blockSize, swBytesRead.getValue());
    for (std::size_t idx = 0; idx < swBytesRead.getValue(); ++idx) {
        const std::size_t offset = (lastBlockIdx * blockSize) + idx;
        ASSERT_EQ(static_cast<char>(offset % 256), data[idx]);
    }
}

TEST_F(ReaderTest, TestRead) {
    const std::size_t fileSize = 9400;
    const std::size_t blockSize = 1000;
    std::array<char, fileSize> data;

    BlockstoreHTTP blockstore("", mongo::OID(), std::make_unique<MockedHttpClient>());
    Reader reader(std::move(blockstore), "file", fileSize, blockSize);
    for (auto bytesToRead : std::vector<std::size_t>{400, 1000, 1400, 2000, 2700}) {
        for (auto fileOffset : std::vector<std::size_t>{0, 400, 1000, 1400}) {
            DataRange buf(data.data(), data.data() + fileSize);
            auto swBytesRead = reader.read(buf, fileOffset, bytesToRead);
            ASSERT_EQ(Status::OK(), swBytesRead.getStatus());
            ASSERT_EQ(bytesToRead, swBytesRead.getValue());
            for (std::size_t idx = 0; idx < bytesToRead; ++idx) {
                const std::size_t offset = fileOffset + idx;
                ASSERT_EQ(static_cast<char>(offset % 256), data[idx]);
            }
        }
    }

    // Test reading to the end of file calculates
    const std::size_t bytesToRead = 1500;
    DataRange buf(data.data(), data.data() + bytesToRead);
    auto swBytesRead = reader.read(buf, fileSize - bytesToRead, bytesToRead);
    ASSERT_OK(swBytesRead.getStatus());
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
