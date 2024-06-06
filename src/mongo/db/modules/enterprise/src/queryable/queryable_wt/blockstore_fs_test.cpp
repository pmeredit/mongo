/**
 *  Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "blockstore_fs.h"
#include "mongo/platform/basic.h"

#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace queryable {
namespace {

class BlockstoreFSTest : public unittest::Test {};

TEST_F(BlockstoreFSTest, TestPathOperations) {
    BlockstoreFileSystem fs("mock", {}, "/data/db/mock/path", nullptr);

    std::string regularFile = "WiredTiger.lock";
    std::string journalFile = "journal/WiredTigerLog.00001";

    const std::string regularFilePath = fs.getFileAbsolutePathFromRelativePath(regularFile);
    const std::string journalFilePath = fs.getFileAbsolutePathFromRelativePath(journalFile);

    ASSERT_EQUALS("/data/db/mock/path/" + regularFile, regularFilePath);
    ASSERT_EQUALS("/data/db/mock/path/" + journalFile, journalFilePath);

    regularFile = fs.getFileRelativePath(regularFilePath);
    journalFile = fs.getFileRelativePath(journalFilePath);

    ASSERT_EQUALS("/data/db/mock/path/" + regularFile, regularFilePath);
    ASSERT_EQUALS("/data/db/mock/path/" + journalFile, journalFilePath);
}

TEST_F(BlockstoreFSTest, TestFileOperations) {
    BlockstoreFileSystem fs("mock", {}, "/data/db/mock/path", nullptr);

    File regularFile = {"WiredTiger.lock", 0};
    File logFile = {"journal/WiredTigerLog.00001", 0};

    fs.addFile(regularFile);
    fs.addFile(logFile);

    ASSERT_TRUE(fs.fileExists("/data/db/mock/path/WiredTiger.lock"));
    ASSERT_TRUE(fs.fileExists("/data/db/mock/path/journal/WiredTigerLog.00001"));
    ASSERT_FALSE(fs.fileExists("/data/db/mock/path/journal/WiredTigerLog.00002"));

    File retrievedRegularFile = fs.getFile("/data/db/mock/path/WiredTiger.lock");
    File retrievedlogFile = fs.getFile("/data/db/mock/path/journal/WiredTigerLog.00001");

    ASSERT_EQUALS(retrievedRegularFile.filename, "WiredTiger.lock");
    ASSERT_EQUALS(retrievedRegularFile.fileSize, 0);

    ASSERT_EQUALS(retrievedlogFile.filename, "journal/WiredTigerLog.00001");
    ASSERT_EQUALS(retrievedlogFile.fileSize, 0);

    ASSERT_EQUALS(fs.getFileSize("WiredTiger.lock"), 0);
    ASSERT_EQUALS(fs.getFileSize("journal/WiredTigerLog.00001"), 0);

    fs.addToFileSize("WiredTiger.lock", 1000);
    ASSERT_EQUALS(fs.getFileSize("WiredTiger.lock"), 1000);

    ASSERT_THROWS_CODE(fs.getFile("/data/db/mock/path/journal/WiredTigerLog.00002"),
                       DBException,
                       ErrorCodes::NoSuchKey);
}

TEST_F(BlockstoreFSTest, TestListDir) {
    BlockstoreFileSystem fs("mock", {}, "/data/db/mock/path", nullptr);

    File regularFile = {"WiredTiger.lock", 0};
    File logFile = {"journal/WiredTigerLog.00001", 0};

    fs.addFile(regularFile);
    fs.addFile(logFile);

    auto matchedFiles = fs.getFiles("/data", "");
    ASSERT_EQUALS(matchedFiles.size(), 0);

    matchedFiles = fs.getFiles("/data/db", "");
    ASSERT_EQUALS(matchedFiles.size(), 0);

    matchedFiles = fs.getFiles("/data/db/mock", "");
    ASSERT_EQUALS(matchedFiles.size(), 0);

    matchedFiles = fs.getFiles("/data/db/mock/path", "");
    ASSERT_EQUALS(matchedFiles.size(), 2);
    ASSERT_TRUE(std::find(matchedFiles.begin(), matchedFiles.end(), "journal") !=
                matchedFiles.end());
    ASSERT_TRUE(std::find(matchedFiles.begin(), matchedFiles.end(), "WiredTiger.lock") !=
                matchedFiles.end());

    matchedFiles = fs.getFiles("/data/db/mock/path", "jou");
    ASSERT_EQUALS(matchedFiles.size(), 1);
    ASSERT_EQUALS(matchedFiles.at(0), "journal");

    matchedFiles = fs.getFiles("/data/db/mock/path", "journal");
    ASSERT_EQUALS(matchedFiles.size(), 1);
    ASSERT_EQUALS(matchedFiles.at(0), "journal");

    matchedFiles = fs.getFiles("/data/db/mock/path", "journall");
    ASSERT_EQUALS(matchedFiles.size(), 0);

    matchedFiles = fs.getFiles("/data/db/mock/path", "WiredTiger.");
    ASSERT_EQUALS(matchedFiles.size(), 1);
    ASSERT_EQUALS(matchedFiles.at(0), "WiredTiger.lock");

    matchedFiles = fs.getFiles("/data/db/mock/path/journal", "");
    ASSERT_EQUALS(matchedFiles.size(), 1);
    ASSERT_EQUALS(matchedFiles.at(0), "WiredTigerLog.00001");


    matchedFiles = fs.getFiles("/data/db/mock/path/journal", "WiredTigerLog.00001");
    ASSERT_EQUALS(matchedFiles.size(), 1);
    ASSERT_EQUALS(matchedFiles.at(0), "WiredTigerLog.00001");
}

TEST_F(BlockstoreFSTest, TestAllocationDeallocationForListDir) {
    BlockstoreFileSystem fs("mock", {}, "/data/db/mock/path", nullptr);

    WT_FILE_SYSTEM* wtFs = (WT_FILE_SYSTEM*)&fs;
    char** dirList;
    uint32_t count;
    int ret = queryableWtFsDirectoryList(wtFs, nullptr, "/data/db/mock/path", "", &dirList, &count);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(count, 0);

    File regularFile = {"WiredTiger.lock", 0};
    File logFile = {"journal/WiredTigerLog.00001", 0};

    fs.addFile(regularFile);
    fs.addFile(logFile);

    ret = queryableWtFsDirectoryList(wtFs, nullptr, "/data/db/mock/path", "", &dirList, &count);
    ASSERT_EQ(ret, 0);

    ASSERT_EQUALS(count, 2);

    std::vector<std::string> fileNames = {"journal", "WiredTiger.lock"};
    ASSERT_TRUE(std::find(fileNames.begin(), fileNames.end(), dirList[0]) != fileNames.end());
    ASSERT_TRUE(std::find(fileNames.begin(), fileNames.end(), dirList[1]) != fileNames.end());

    ret = queryableWtFsDirectoryListFree(wtFs, nullptr, dirList, count);
    ASSERT_EQ(ret, 0);

    ret = queryableWtFsDirectoryList(wtFs, nullptr, "/data/db/mock/path", "jour", &dirList, &count);
    ASSERT_EQ(ret, 0);

    ASSERT_EQUALS(count, 1);

    fileNames = {"journal"};
    ASSERT_TRUE(std::find(fileNames.begin(), fileNames.end(), dirList[0]) != fileNames.end());

    ret = queryableWtFsDirectoryListFree(wtFs, nullptr, dirList, count);
    ASSERT_EQ(ret, 0);

    ret = queryableWtFsDirectoryList(
        wtFs, nullptr, "/data/db/mock/path", "journal", &dirList, &count);
    ASSERT_EQ(ret, 0);

    ASSERT_EQUALS(count, 1);

    fileNames = {"journal"};
    ASSERT_TRUE(std::find(fileNames.begin(), fileNames.end(), dirList[0]) != fileNames.end());

    ret = queryableWtFsDirectoryListFree(wtFs, nullptr, dirList, count);
    ASSERT_EQ(ret, 0);

    ret = queryableWtFsDirectoryList(
        wtFs, nullptr, "/data/db/mock/path", "journall", &dirList, &count);
    ASSERT_EQ(ret, 0);
    ASSERT_EQUALS(count, 0);
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
