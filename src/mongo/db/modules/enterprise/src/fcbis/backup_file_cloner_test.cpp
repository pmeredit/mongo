/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include <vector>

#include "backup_file_cloner.h"
#include "initial_sync_file_mover.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/repl/initial_sync_cloner_test_fixture.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/dbtests/mock/mock_dbclient_connection.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/concurrency/thread_pool.h"

namespace mongo {
namespace repl {

class MockCallbackState final : public mongo::executor::TaskExecutor::CallbackState {
public:
    MockCallbackState() = default;
    void cancel() override {}
    void waitForCompletion() override {}
    bool isCanceled() const override {
        return false;
    }
};

class BackupFileClonerTest : public InitialSyncClonerTestFixture {
public:
    BackupFileClonerTest() : _backupId(UUID::gen()) {}

protected:
    void setUp() override {
        InitialSyncClonerTestFixture::setUp();
        _initialSyncPath = boost::filesystem::path(storageGlobalParams.dbpath);
        _initialSyncPath.append(InitialSyncFileMover::kInitialSyncDir.toString());
    }
    std::unique_ptr<BackupFileCloner> makeBackupFileCloner(const std::string& remoteFileName,
                                                           const std::string& relativePath,
                                                           size_t remoteFileSize) {
        return std::make_unique<BackupFileCloner>(_backupId,
                                                  remoteFileName,
                                                  remoteFileSize,
                                                  relativePath,
                                                  getSharedData(),
                                                  _source,
                                                  _mockClient.get(),
                                                  &_storageInterface,
                                                  _dbWorkThreadPool.get());
    }

    ProgressMeter& getProgressMeter(BackupFileCloner* cloner) {
        return cloner->_progressMeter;
    }

protected:
    const UUID _backupId;
    boost::filesystem::path _initialSyncPath;

private:
    unittest::MinimumLoggedSeverityGuard replLogSeverityGuard{
        logv2::LogComponent::kReplicationInitialSync, logv2::LogSeverity::Debug(3)};
};

TEST_F(BackupFileClonerTest, RelativePathIsnt) {
    // current_path() is specified to be absolute; using it avoids the need to hardcode a path
    // which would need to be different between Windows and Unix.
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner =
        makeBackupFileCloner("/path/to/backupfile", absolutePath.generic_string(), 0);
    ASSERT_EQ(backupFileCloner->run().code(), 5781700);
}

TEST_F(BackupFileClonerTest, RelativePathEscapes) {
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "../escapee", 0);
    ASSERT_EQ(backupFileCloner->run().code(), 5781701);
}

TEST_F(BackupFileClonerTest, CantOpenFile) {
    // file can't be opened because it's actually a directory
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "dir/badfile", 0);
    boost::filesystem::create_directory(_initialSyncPath);
    auto badfilePath = _initialSyncPath;
    badfilePath.append("dir/badfile");
    boost::filesystem::create_directories(badfilePath);
    ASSERT_EQ(backupFileCloner->run().code(), ErrorCodes::FileOpenFailed);
}

TEST_F(BackupFileClonerTest, CantCreateDirectory) {
    // directory can't be created because it's a file already.
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "baddir/file", 0);
    boost::filesystem::create_directory(_initialSyncPath);
    auto baddirPath = _initialSyncPath;
    baddirPath.append("baddir");
    std::ofstream baddirFile(baddirPath, std::ios_base::out | std::ios_base::trunc);
    ASSERT_EQ(backupFileCloner->run().code(), 5781703);
}

TEST_F(BackupFileClonerTest, PreStageSuccess) {
    // First test with a file and directory which don't exist.
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "dir/file", 0);
    backupFileCloner->setStopAfterStage_forTest("preStage");
    boost::filesystem::create_directory(_initialSyncPath);
    auto filePath = _initialSyncPath;
    filePath.append("dir/file");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(0, boost::filesystem::file_size(filePath));

    // Now that it exists, test that it is truncated if we try again.
    std::ofstream file(filePath);
    file.write("stuff", 5);
    file.close();
    ASSERT_EQ(5, boost::filesystem::file_size(filePath));

    backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "dir/file", 5);
    backupFileCloner->setStopAfterStage_forTest("preStage");

    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    // File should be truncated.
    ASSERT_EQ(0, boost::filesystem::file_size(filePath));
}

TEST_F(BackupFileClonerTest, EmptyFile) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    CursorResponse response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        0 /* cursorId */,
        {BSON("byteOffset" << 0 << "endOfFile" << true << "data" << BSONBinData())});
    _mockServer->setCommandReply("aggregate", response.toBSONAsInitialResponse());
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(0, boost::filesystem::file_size(filePath));
}

TEST_F(BackupFileClonerTest, NoEOF) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    CursorResponse response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        0 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << BSONBinData())});
    _mockServer->setCommandReply("aggregate", response.toBSONAsInitialResponse());
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_EQ(backupFileCloner->run().code(), 5781710);
}

TEST_F(BackupFileClonerTest, SingleBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "The slow green fox\n takes\r a nap\0 next to the lazy dog.";
    auto bindata = BSONBinData(fileData.data(), fileData.size(), BinDataGeneral);
    CursorResponse response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        0 /* cursorId */,
        {BSON("byteOffset" << 0 << "endOfFile" << true << "data" << bindata)});
    _mockServer->setCommandReply("aggregate", response.toBSONAsInitialResponse());
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(fileData.size(), boost::filesystem::file_size(filePath));
    std::string actualFileData(fileData.size(), 0);
    std::ifstream checkStream(filePath.string(), std::ios_base::in | std::ios_base::binary);
    checkStream.read(actualFileData.data(), fileData.size());
    ASSERT_EQ(fileData, actualFileData);
}

TEST_F(BackupFileClonerTest, Multibatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    CursorResponse batch2response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        0 /* cursorId */,
        {BSON("byteOffset" << 20 << "endOfFile" << true << "data" << batch2bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {batch1response.toBSONAsInitialResponse(),
         batch2response.toBSON(CursorResponse::ResponseType::SubsequentResponse),
         Status(ErrorCodes::UnknownError, "This should never be seen")});
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(fileData.size(), boost::filesystem::file_size(filePath));
    std::string actualFileData(fileData.size(), 0);
    std::ifstream checkStream(filePath.string(), std::ios_base::in | std::ios_base::binary);
    checkStream.read(actualFileData.data(), fileData.size());
    ASSERT_EQ(fileData, actualFileData);
}

TEST_F(BackupFileClonerTest, RetryOnFirstBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "The slow green fox\n takes\r a nap\0 next to the lazy dog.";
    auto bindata = BSONBinData(fileData.data(), fileData.size(), BinDataGeneral);
    CursorResponse response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        0 /* cursorId */,
        {BSON("byteOffset" << 0 << "endOfFile" << true << "data" << bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {Status(ErrorCodes::HostUnreachable, "Retryable Error on first batch"),
         response.toBSONAsInitialResponse()});
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(fileData.size(), boost::filesystem::file_size(filePath));
    std::string actualFileData(fileData.size(), 0);
    std::ifstream checkStream(filePath.string(), std::ios_base::in | std::ios_base::binary);
    checkStream.read(actualFileData.data(), fileData.size());
    ASSERT_EQ(fileData, actualFileData);
}

TEST_F(BackupFileClonerTest, RetryOnSubsequentBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    CursorResponse batch2response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        0 /* cursorId */,
        {BSON("byteOffset" << 20 << "endOfFile" << true << "data" << batch2bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {batch1response.toBSONAsInitialResponse(),
         Status(ErrorCodes::HostUnreachable, "Retryable Error on second batch"),
         batch2response.toBSONAsInitialResponse(),
         Status(ErrorCodes::UnknownError, "This should never be seen")});
    boost::filesystem::create_directory(_initialSyncPath);
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(fileData.size(), boost::filesystem::file_size(filePath));
    std::string actualFileData(fileData.size(), 0);
    std::ifstream checkStream(filePath.string(), std::ios_base::in | std::ios_base::binary);
    checkStream.read(actualFileData.data(), fileData.size());
    ASSERT_EQ(fileData, actualFileData);
}

TEST_F(BackupFileClonerTest, NonRetryableErrorFirstBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    _mockServer->setCommandReply(
        "aggregate", Status(ErrorCodes::IllegalOperation, "Non-retryable Error on first batch"));
    ASSERT_EQ(backupFileCloner->run().code(), ErrorCodes::IllegalOperation);
}

TEST_F(BackupFileClonerTest, NonRetryableErrorSubsequentBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {batch1response.toBSONAsInitialResponse(),
         Status(ErrorCodes::IllegalOperation, "Non-retryable Error on second batch"),
         Status(ErrorCodes::UnknownError, "This should never be seen")});
    ASSERT_EQ(backupFileCloner->run().code(), ErrorCodes::IllegalOperation);
}

TEST_F(BackupFileClonerTest, NonRetryableErrorFollowsRetryableError) {
    // This scenario is expected when a sync source restarts and thus loses its backup cursor.
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {batch1response.toBSONAsInitialResponse(),
         Status(ErrorCodes::HostUnreachable, "Retryable Error on second batch"),
         Status(ErrorCodes::IllegalOperation, "Non-retryable Error on retry"),
         Status(ErrorCodes::UnknownError, "This should never be seen")});
    ASSERT_EQ(backupFileCloner->run().code(), ErrorCodes::IllegalOperation);
}

}  // namespace repl
}  // namespace mongo
