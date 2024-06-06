/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <iostream>
#include <memory>
#include <sstream>

#include "mongo/base/parse_number.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/str.h"

#include "reader_writer.h"

namespace mongo {
namespace queryable {
namespace {

class ReaderWriterTest : public unittest::Test {};

class MockedHttpClient final : public HttpClient {
public:
    MockedHttpClient() {
        // Use a fixed size mock file to write POST requests to.
        _writeFileMock = std::vector<char>(1024);
    }

    HttpReply request(HttpMethod method,
                      StringData url,
                      ConstDataRange data = {nullptr, 0}) const final {
        uassert(ErrorCodes::BadValue,
                "Unsupported HTTP method",
                (method == HttpMethod::kGET) || (method == HttpMethod::kPOST));

        // This would be better with a proper URI parser,
        // but we're tightly coupled to blockstore http,
        // so just pull out the values the hard way.
        const auto urlString = url.toString();
        const char* offsetP = strstr(urlString.c_str(), "&offset=");
        const char* lengthP = strstr(urlString.c_str(), "&length=");
        invariant(offsetP && lengthP);
        int offset;
        auto parser = NumberParser::strToAny(10);
        auto s = parser(offsetP + strlen("&offset="), &offset);
        if (!s.isOK()) {
            offset = 0;
        }
        int length;
        s = parser(lengthP + strlen("&length="), &length);
        if (!s.isOK()) {
            length = 0;
        }

        if (method == HttpMethod::kPOST) {
            ASSERT_EQ(length, data.length());
            for (std::size_t idx = 0; idx < data.length(); ++idx) {
                _writeFileMock[idx + offset] = data.data()[idx];
            }
            return HttpReply(200, {}, {});
        }
        invariant(method == HttpMethod::kGET);

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

        return HttpReply(200, {}, std::move(builder));
    }

    // Ignore client config.
    void allowInsecureHTTP(bool) final {}
    void setHeaders(const std::vector<std::string>&) final {}

    std::vector<char> getMockFile() const {
        return _writeFileMock;
    }

private:
    mutable std::vector<char> _writeFileMock;
};

TEST_F(ReaderWriterTest, TestRead) {
    const std::size_t fileSize = 9400;
    std::array<char, fileSize> data;

    BlockstoreHTTP blockstore("", mongo::OID(), std::make_unique<MockedHttpClient>());
    ReaderWriter readerWriter(std::move(blockstore), "file", fileSize);
    for (auto bytesToRead : std::vector<std::size_t>{400, 1000, 1400, 2000, 2700}) {
        for (auto fileOffset : std::vector<std::size_t>{0, 400, 1000, 1400}) {
            DataRange buf(data.data(), data.data() + fileSize);
            auto swBytesRead = readerWriter.read(buf, fileOffset, bytesToRead);
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
    auto swBytesRead = readerWriter.read(buf, fileSize - bytesToRead, bytesToRead);
    ASSERT_OK(swBytesRead.getStatus());
}

TEST_F(ReaderWriterTest, TestWrite) {
    // Use a fixed size mock file of 1024 bytes.
    const std::size_t fileSize = 1024;

    auto ownedHttpClient = std::make_unique<MockedHttpClient>();
    auto httpClient = ownedHttpClient.get();
    BlockstoreHTTP blockstore("", mongo::OID(), std::move(ownedHttpClient));
    ReaderWriter readerWriter(std::move(blockstore), "file", fileSize);

    size_t offset = 0;

    // Write five bytes at the beginning of the file.
    std::array<char, 5> toWrite{5, 4, 3, 2, 1};
    ASSERT_OK(readerWriter.write(ConstDataRange(toWrite), offset, toWrite.size()).getStatus());

    std::vector<char> mockFile = httpClient->getMockFile();
    for (std::size_t idx = 0; idx < toWrite.size(); ++idx) {
        ASSERT_EQ(toWrite[idx], mockFile[idx + offset]);
    }

    // Overwrite some of the bytes at the beginning of the file and append additional bytes to the
    // file.
    offset += 3;
    toWrite = {6, 6, 6, 6, 6};
    ASSERT_OK(readerWriter.write(ConstDataRange(toWrite), offset, toWrite.size()).getStatus());

    mockFile = httpClient->getMockFile();
    std::array<char, 8> expectedContents{5, 4, 3, 6, 6, 6, 6, 6};
    for (std::size_t idx = 0; idx < expectedContents.size(); ++idx) {
        ASSERT_EQ(expectedContents[idx], mockFile[idx]);
    }
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
