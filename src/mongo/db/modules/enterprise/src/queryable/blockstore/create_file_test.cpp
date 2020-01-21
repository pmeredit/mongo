/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <memory>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"

#include "../blockstore/list_dir.h"

namespace mongo {
namespace queryable {
namespace {

class CreateFileTest : public unittest::Test {};

class MockedHttpClient : public HttpClient {
public:
    virtual DataBuilder post(StringData, ConstDataRange) const {
        MONGO_UNREACHABLE;
    }

    virtual DataBuilder get(StringData url) const {
        std::string urlStr(url);
        if (urlStr.find("/os_list") != std::string::npos) {
            BSONObjBuilder objBuilder;
            objBuilder.append("ok", true);
            objBuilder.append("files", _files);

            BSONObj obj = objBuilder.obj();
            DataBuilder copy(obj.objsize());
            uassertStatusOK(copy.write(ConstDataRange(obj.objdata(), obj.objsize())));

            return copy;
        } else if (urlStr.find("/os_wt_recovery_open_file") != std::string::npos) {
            // This would be better with a proper URI parser, but we're tightly coupled to
            // blockstore http so just pull out the filename the hard way.
            const char* filenameP = strstr(urlStr.c_str(), "&filename=");
            invariant(filenameP);
            const std::string filename(filenameP + strlen("&filename="));

            BSONObjBuilder file;
            file.append("filename", filename);
            file.append("fileSize", 10000);

            const BSONObj obj = file.done();
            DataBuilder copy(obj.objsize());
            uassertStatusOK(copy.write(ConstDataRange(obj.objdata(), obj.objsize())));

            _files.emplace_back(obj.getOwned());

            return copy;
        }

        MONGO_UNREACHABLE;
    }

    virtual DataBuilder put(StringData, ConstDataRange) const {
        invariant(false);
        return DataBuilder();
    }

    // Ignore client configs.
    virtual void allowInsecureHTTP(bool) {}
    virtual void setHeaders(const std::vector<std::string>& headers) {}
    virtual void setConnectTimeout(Seconds timeout) {}
    virtual void setTimeout(Seconds timeout) {}

private:
    mutable std::vector<BSONObj> _files;
};

TEST_F(CreateFileTest, CreateFile) {
    queryable::BlockstoreHTTP blockstore("", mongo::OID(), std::make_unique<MockedHttpClient>());
    auto swFiles = listDirectory(blockstore);
    ASSERT_TRUE(swFiles.isOK());
    ASSERT_EQ(static_cast<std::size_t>(0), swFiles.getValue().size());

    ASSERT_TRUE(blockstore.openFile("test_file").isOK());

    swFiles = listDirectory(blockstore);
    ASSERT_TRUE(swFiles.isOK());
    ASSERT_EQ(static_cast<std::size_t>(1), swFiles.getValue().size());

    struct File file = swFiles.getValue()[0];
    ASSERT_EQ("test_file", file.filename);
    ASSERT_EQ(static_cast<int64_t>(10000), file.fileSize);

    ASSERT_TRUE(blockstore.openFile("test_file_2").isOK());

    swFiles = listDirectory(blockstore);
    ASSERT_TRUE(swFiles.isOK());
    ASSERT_EQ(static_cast<std::size_t>(2), swFiles.getValue().size());

    file = swFiles.getValue()[1];
    ASSERT_EQ("test_file_2", file.filename);
    ASSERT_EQ(static_cast<int64_t>(10000), file.fileSize);
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
