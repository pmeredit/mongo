/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <vector>

#include "backup_file_cloner.h"
#include "initial_sync_file_mover.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/repl/initial_sync_cloner_test_fixture.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/db/storage/storage_options.h"
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
                                                           size_t remoteFileSize,
                                                           int extensionNumber = 0) {
        return std::make_unique<BackupFileCloner>(_backupId,
                                                  remoteFileName,
                                                  remoteFileSize,
                                                  relativePath,
                                                  extensionNumber,
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
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), 5781700) << status;
}

TEST_F(BackupFileClonerTest, RelativePathEscapes) {
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "../escapee", 0);
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), 5781701) << status;
}

TEST_F(BackupFileClonerTest, CantOpenFile) {
    // file can't be opened because it's actually a directory
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "dir/badfile", 0);
    boost::filesystem::create_directory(_initialSyncPath);
    auto badfilePath = _initialSyncPath;
    badfilePath.append("dir/badfile");
    boost::filesystem::create_directories(badfilePath);
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), ErrorCodes::FileOpenFailed) << status;
}

TEST_F(BackupFileClonerTest, CantCreateDirectory) {
    // directory can't be created because it's a file already.
    auto backupFileCloner = makeBackupFileCloner("/path/to/backupfile", "baddir/file", 0);
    boost::filesystem::create_directory(_initialSyncPath);
    auto baddirPath = _initialSyncPath;
    baddirPath.append("baddir");
    std::ofstream baddirFile(baddirPath.native(), std::ios_base::out | std::ios_base::trunc);
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), 5781703) << status;
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
    std::ofstream file(filePath.native());
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
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        0 /* cursorId */,
        {BSON("byteOffset" << 0 << "endOfFile" << true << "data" << BSONBinData())});
    _mockServer->setCommandReply("aggregate", response.toBSONAsInitialResponse());
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());
    ASSERT(boost::filesystem::exists(filePath));
    ASSERT_EQ(0, boost::filesystem::file_size(filePath));

    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT(stats["fileSize"].isNumber());
    ASSERT(stats["bytesCopied"].isNumber());
    ASSERT_EQ(0, stats["fileSize"].numberLong());
    ASSERT_EQ(0, stats["bytesCopied"].numberLong());
    // The empty batch counts as a batch.
    ASSERT_EQ(1, stats["receivedBatches"].numberLong());
    ASSERT_EQ(1, stats["writtenBatches"].numberLong());
}

TEST_F(BackupFileClonerTest, NoEOF) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    CursorResponse response(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                            0 /* cursorId */,
                            {BSON("byteOffset" << 0 << "data" << BSONBinData())});
    _mockServer->setCommandReply("aggregate", response.toBSONAsInitialResponse());
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), 5781710) << status;
}

TEST_F(BackupFileClonerTest, SingleBatch) {
    auto absolutePath = boost::filesystem::current_path();
    std::string fileData = "The slow green fox\n takes\r a nap\0 next to the lazy dog.";
    auto backupFileCloner =
        makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", fileData.size());
    auto bindata = BSONBinData(fileData.data(), fileData.size(), BinDataGeneral);
    CursorResponse response(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
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

    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
    ASSERT_EQ(fileData.size(), stats["bytesCopied"].numberLong());
    ASSERT_EQ(1, stats["receivedBatches"].numberLong());
    ASSERT_EQ(1, stats["writtenBatches"].numberLong());
}

TEST_F(BackupFileClonerTest, Multibatch) {
    auto absolutePath = boost::filesystem::current_path();
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto backupFileCloner =
        makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", fileData.size());
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    CursorResponse batch2response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
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

    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
    ASSERT_EQ(fileData.size(), stats["bytesCopied"].numberLong());
    ASSERT_EQ(2, stats["receivedBatches"].numberLong());
    ASSERT_EQ(2, stats["writtenBatches"].numberLong());
}

TEST_F(BackupFileClonerTest, RetryOnFirstBatch) {
    auto absolutePath = boost::filesystem::current_path();
    std::string fileData = "The slow green fox\n takes\r a nap\0 next to the lazy dog.";
    auto backupFileCloner =
        makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", fileData.size());
    auto bindata = BSONBinData(fileData.data(), fileData.size(), BinDataGeneral);
    CursorResponse response(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
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

    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
    ASSERT_EQ(fileData.size(), stats["bytesCopied"].numberLong());
    ASSERT_EQ(1, stats["receivedBatches"].numberLong());
    ASSERT_EQ(1, stats["writtenBatches"].numberLong());
}

TEST_F(BackupFileClonerTest, RetryOnSubsequentBatch) {
    auto absolutePath = boost::filesystem::current_path();
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto backupFileCloner =
        makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", fileData.size());
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    CursorResponse batch2response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
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

    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
    ASSERT_EQ(fileData.size(), stats["bytesCopied"].numberLong());
    ASSERT_EQ(2, stats["receivedBatches"].numberLong());
    ASSERT_EQ(2, stats["writtenBatches"].numberLong());
}

TEST_F(BackupFileClonerTest, NonRetryableErrorFirstBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    _mockServer->setCommandReply(
        "aggregate", Status(ErrorCodes::IllegalOperation, "Non-retryable Error on first batch"));
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), ErrorCodes::IllegalOperation) << status;
}

TEST_F(BackupFileClonerTest, NonRetryableErrorSubsequentBatch) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {batch1response.toBSONAsInitialResponse(),
         Status(ErrorCodes::IllegalOperation, "Non-retryable Error on second batch"),
         Status(ErrorCodes::UnknownError, "This should never be seen")});
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), ErrorCodes::IllegalOperation) << status;
}

TEST_F(BackupFileClonerTest, NonRetryableErrorFollowsRetryableError) {
    // This scenario is expected when a sync source restarts and thus loses its backup cursor.
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", 0);
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    _mockServer->setCommandReply(
        "aggregate",
        {batch1response.toBSONAsInitialResponse(),
         Status(ErrorCodes::HostUnreachable, "Retryable Error on second batch"),
         Status(ErrorCodes::IllegalOperation, "Non-retryable Error on retry"),
         Status(ErrorCodes::UnknownError, "This should never be seen")});
    auto status = backupFileCloner->run();
    ASSERT_EQ(status.code(), ErrorCodes::IllegalOperation) << status;
}

TEST_F(BackupFileClonerTest, InProgressStats) {
    auto absolutePath = boost::filesystem::current_path();
    std::string fileData = "ABCDEFGHJIKLMNOPQRST0123456789";
    auto backupFileCloner =
        makeBackupFileCloner("/path/dir/backupfile", "dir/backupfile", fileData.size());
    auto batch1bindata = BSONBinData(fileData.data(), 20, BinDataGeneral);
    auto batch2bindata = BSONBinData(fileData.data() + 20, fileData.size() - 20, BinDataGeneral);
    CursorResponse batch1response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        1 /* cursorId */,
        {BSON("byteOffset" << 0 << "data" << batch1bindata)});
    CursorResponse batch2response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        0 /* cursorId */,
        {BSON("byteOffset" << 20 << "endOfFile" << true << "data" << batch2bindata)});
    _mockServer->setCommandReply("aggregate",
                                 {batch1response.toBSONAsInitialResponse(),
                                  batch2response.toBSONAsInitialResponse(),
                                  Status(ErrorCodes::UnknownError, "This should never be seen")});
    boost::filesystem::create_directory(_initialSyncPath);
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");

    // Stats before running
    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
    ASSERT(stats["bytesCopied"].isNumber());
    ASSERT(stats["receivedBatches"].isNumber());
    ASSERT(stats["writtenBatches"].isNumber());
    ASSERT_EQ(0, stats["bytesCopied"].numberLong());
    ASSERT_EQ(0, stats["receivedBatches"].numberLong());
    ASSERT_EQ(0, stats["writtenBatches"].numberLong());
    ASSERT(stats["start"].eoo());
    ASSERT(stats["end"].eoo());
    ASSERT(stats["extensionNumber"].eoo());

    stdx::thread backupFileClonerThread;
    {
        // Pause both network and file-writing threads
        auto networkFailpoint = globalFailPointRegistry().find(
            "initialSyncHangBackupFileClonerAfterHandlingBatchResponse");
        auto fileWritingFailpoint =
            globalFailPointRegistry().find("initialSyncHangDuringBackupFileClone");
        auto fileWritingFailpointCount = fileWritingFailpoint->setMode(FailPoint::alwaysOn);
        auto networkFailpointCount = networkFailpoint->setMode(FailPoint::alwaysOn);

        // Stop after the query stage for one more stats check.
        FailPointEnableBlock clonerFailpoint("hangAfterClonerStage",
                                             BSON("cloner"
                                                  << "BackupFileCloner"
                                                  << "stage"
                                                  << "query"));
        // Run the cloner in another thread.
        backupFileClonerThread = stdx::thread([&] {
            Client::initThread("BackupFileClonerRunner", getGlobalServiceContext()->getService());
            ASSERT_OK(backupFileCloner->run());
        });
        fileWritingFailpoint->waitForTimesEntered(Interruptible::notInterruptible(),
                                                  fileWritingFailpointCount + 1);
        networkFailpoint->waitForTimesEntered(Interruptible::notInterruptible(),
                                              networkFailpointCount + 1);

        // Stats after first batch.
        stats = backupFileCloner->getStats().toBSON();

        ASSERT_EQ("dir/backupfile", stats["filePath"].str());
        ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
        ASSERT_EQ(20, stats["bytesCopied"].numberLong());
        ASSERT_EQ(1, stats["receivedBatches"].numberLong());
        ASSERT_EQ(1, stats["writtenBatches"].numberLong());
        ASSERT_EQ(Date, stats["start"].type());
        ASSERT(stats["end"].eoo());
        ASSERT(stats["extensionNumber"].eoo());

        fileWritingFailpoint->setMode(FailPoint::off);
        networkFailpoint->setMode(FailPoint::off);
        clonerFailpoint->waitForTimesEntered(Interruptible::notInterruptible(),
                                             clonerFailpoint.initialTimesEntered() + 1);

        // Stats after second batch.
        stats = backupFileCloner->getStats().toBSON();

        ASSERT_EQ("dir/backupfile", stats["filePath"].str());
        ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
        ASSERT_EQ(fileData.size(), stats["bytesCopied"].numberLong());
        ASSERT_EQ(2, stats["receivedBatches"].numberLong());
        ASSERT_EQ(2, stats["writtenBatches"].numberLong());
        ASSERT_EQ(Date, stats["start"].type());
        ASSERT(stats["end"].eoo());
        ASSERT(stats["extensionNumber"].eoo());
    }

    backupFileClonerThread.join();

    // Final stats.
    stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT_EQ(fileData.size(), stats["fileSize"].numberLong());
    ASSERT_EQ(fileData.size(), stats["bytesCopied"].numberLong());
    ASSERT_EQ(2, stats["receivedBatches"].numberLong());
    ASSERT_EQ(2, stats["writtenBatches"].numberLong());
    ASSERT_EQ(Date, stats["start"].type());
    ASSERT_EQ(Date, stats["end"].type());
    ASSERT(stats["extensionNumber"].eoo());
}

TEST_F(BackupFileClonerTest, ExtensionStats) {
    auto absolutePath = boost::filesystem::current_path();
    auto backupFileCloner = makeBackupFileCloner("/path/dir/backupfile",
                                                 "dir/backupfile",
                                                 0,
                                                 /* extensionNumber */ 2);
    CursorResponse response(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
        0 /* cursorId */,
        {BSON("byteOffset" << 0 << "endOfFile" << true << "data" << BSONBinData())});
    _mockServer->setCommandReply("aggregate", response.toBSONAsInitialResponse());
    auto filePath = _initialSyncPath;
    filePath.append("dir/backupfile");
    ASSERT_OK(backupFileCloner->run());

    BSONObj stats = backupFileCloner->getStats().toBSON();

    ASSERT_EQ("dir/backupfile", stats["filePath"].str());
    ASSERT(stats["fileSize"].isNumber());
    ASSERT(stats["bytesCopied"].isNumber());
    ASSERT_EQ(0, stats["fileSize"].numberLong());
    ASSERT_EQ(0, stats["bytesCopied"].numberLong());
    ASSERT_EQ(1, stats["receivedBatches"].numberLong());
    ASSERT_EQ(1, stats["writtenBatches"].numberLong());
    ASSERT_EQ(2, stats["extensionNumber"].numberLong());
}

}  // namespace repl
}  // namespace mongo
