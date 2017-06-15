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

#include "../blockstore/http_client.h"
#include "../blockstore/list_dir.h"

namespace mongo {
namespace queryable {
namespace {

class ListDirTest : public unittest::Test {};

class MockedHttpClient final : public HttpClientInterface {
public:
    StatusWith<std::size_t> read(std::string path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const override {
        return 0;
    }

    StatusWith<DataBuilder> listDirectory() const override {
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
        copy.write(ConstDataRange(obj.objdata(), obj.objsize())).transitional_ignore();

        return {std::move(copy)};
    }
};

TEST_F(ListDirTest, ListDirs) {
    auto httpClient(stdx::make_unique<MockedHttpClient>());
    auto swFiles = listDirectory(httpClient.get());
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

}  // namespace
}  // namespace queryable
}  // namespace mongo
