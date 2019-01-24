/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <sstream>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"

#include "../blockstore/list_dir.h"

namespace mongo {
namespace queryable {
namespace {

class ListDirTest : public unittest::Test {};

class MockedHttpClient : public HttpClient {
public:
    virtual DataBuilder post(StringData, ConstDataRange) const {
        invariant(false);
        return DataBuilder();
    }

    virtual DataBuilder get(StringData) const {
        BSONObjBuilder objBuilder;
        objBuilder.append("ok", true);
        std::vector<BSONObj> files;

        BSONObjBuilder file0;
        file0.append("filename", "test.0");
        file0.append("fileSize", 10000);
        file0.append("blockSize", 1000);
        files.emplace_back(file0.obj());

        BSONObjBuilder file1;
        file1.append("filename", "index-1323-8358821207107156793.wt");
        file1.append("fileSize", 20480);
        file1.append("blockSize", 64 * 1024);
        files.emplace_back(file1.obj());

        objBuilder.append("files", files);

        BSONObj obj = objBuilder.obj();
        DataBuilder copy(obj.objsize());
        uassertStatusOK(copy.write(ConstDataRange(obj.objdata(), obj.objsize())));

        return copy;
    }

    // Ignore client configs.
    virtual void allowInsecureHTTP(bool) {}
    virtual void setHeaders(const std::vector<std::string>& headers) {}
    virtual void setConnectTimeout(Seconds timeout) {}
    virtual void setTimeout(Seconds timeout) {}
};

class LargeListDir : public MockedHttpClient {
public:
    virtual DataBuilder get(StringData) const {
        BSONObjBuilder builder;
        builder.append("ok", true);

        const long long GB = 1024 * 1024 * 1024;

        std::uint64_t bsonSize = 0;
        std::vector<BSONObj> files;
        while (bsonSize < 50 * 1024 * 1024) {
            BSONObjBuilder file;
            file.append("filename", "index-1323-8358821207107156793.wt");
            file.append("fileSize", 3 * GB);
            file.append("blockSize", 16 * 1024 * 1024);

            bsonSize += file.len();
            files.emplace_back(file.obj());
        }

        _numFilesInResponse = files.size();

        builder.append("files", files);
        BSONObj obj = builder.obj<BSONObj::LargeSizeTrait>();
        DataBuilder copy(obj.objsize());
        _numBytesInResponse = obj.objsize();
        uassertStatusOK(copy.write(ConstDataRange(obj.objdata(), obj.objsize())));

        return copy;
    }

    virtual std::size_t getNumFilesInResponse() {
        return _numFilesInResponse;
    }

    virtual std::size_t getNumBytesInResponse() {
        return _numBytesInResponse;
    }

private:
    mutable std::size_t _numFilesInResponse = 0;
    mutable std::size_t _numBytesInResponse = 0;
};

TEST_F(ListDirTest, ListDirs) {
    queryable::BlockstoreHTTP blockstore("", mongo::OID(), std::make_unique<MockedHttpClient>());
    auto swFiles = listDirectory(blockstore);
    ASSERT_TRUE(swFiles.isOK());
    ASSERT_EQ(static_cast<std::size_t>(2), swFiles.getValue().size());
    struct File file = swFiles.getValue()[0];
    ASSERT_EQ("test.0", file.filename);
    ASSERT_EQ(static_cast<int64_t>(10000), file.fileSize);
    ASSERT_EQ(static_cast<int32_t>(1000), file.blockSize);

    file = swFiles.getValue()[1];
    ASSERT_EQ("index-1323-8358821207107156793.wt", file.filename);
    ASSERT_EQ(static_cast<int64_t>(20480), file.fileSize);
    ASSERT_EQ(static_cast<int32_t>(64 * 1024), file.blockSize);
}

TEST_F(ListDirTest, LargeListDir) {
    auto httpClient = std::make_unique<LargeListDir>();
    LargeListDir* httpClientPtr = httpClient.get();

    queryable::BlockstoreHTTP blockstore("", mongo::OID(), std::move(httpClient));
    auto swFiles = listDirectory(blockstore);
    ASSERT_TRUE(swFiles.isOK()) << swFiles.getStatus();
    ASSERT_EQ(httpClientPtr->getNumFilesInResponse(), swFiles.getValue().size());
    ASSERT_GT(httpClientPtr->getNumBytesInResponse(), static_cast<std::size_t>(50 * 1024 * 1024));
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
