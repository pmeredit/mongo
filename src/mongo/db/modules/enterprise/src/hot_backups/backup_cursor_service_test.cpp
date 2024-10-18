/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "backup_cursor_service.h"

#include "mongo/db/client.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/devnull/devnull_kv_engine.h"
#include "mongo/db/storage/storage_engine_impl.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {

const int kBlockSizeMB = 16;

class BackupCursorServiceTest : public ServiceContextMongoDTest {
public:
    BackupCursorServiceTest()
        : ServiceContextMongoDTest(Options{}.engine("devnull")),
          _opCtx(cc().makeOperationContext()),
          _backupCursorService(std::make_unique<BackupCursorService>()) {
        repl::ReplicationCoordinator::set(
            getServiceContext(),
            std::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
        repl::StorageInterface::set(getServiceContext(),
                                    std::make_unique<repl::StorageInterfaceMock>());
    }

protected:
    ServiceContext::UniqueOperationContext _opCtx;
    std::unique_ptr<BackupCursorService> _backupCursorService;
    boost::filesystem::path backupRootPath = storageGlobalParams.dbpath;
};

TEST_F(BackupCursorServiceTest, TestTypicalFsyncLifetime) {
    _backupCursorService->fsyncLock(_opCtx.get());
    _backupCursorService->fsyncUnlock(_opCtx.get());

    _backupCursorService->fsyncLock(_opCtx.get());
    _backupCursorService->fsyncUnlock(_opCtx.get());
}

TEST_F(BackupCursorServiceTest, TestDoubleLock) {
    _backupCursorService->fsyncLock(_opCtx.get());
    ASSERT_THROWS_WHAT(_backupCursorService->fsyncLock(_opCtx.get()),
                       DBException,
                       "The node is already fsyncLocked.");
    _backupCursorService->fsyncUnlock(_opCtx.get());
}

TEST_F(BackupCursorServiceTest, TestDoubleUnlock) {
    ASSERT_THROWS_WHAT(_backupCursorService->fsyncUnlock(_opCtx.get()),
                       DBException,
                       "The node is not fsyncLocked.");

    _backupCursorService->fsyncLock(_opCtx.get());
    _backupCursorService->fsyncUnlock(_opCtx.get());
    ASSERT_THROWS_WHAT(_backupCursorService->fsyncUnlock(_opCtx.get()),
                       DBException,
                       "The node is not fsyncLocked.");
}

TEST_F(BackupCursorServiceTest, TestTypicalCursorLifetime) {
    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    std::deque<BackupBlock> backupBlocks =
        uassertStatusOK(backupCursorState.streamingCursor.get()->getNextBatch(1 /* batchSize */));
    ASSERT_EQUALS(1u, backupBlocks.size());
    ASSERT_EQUALS(backupRootPath / "testFile.txt", backupBlocks.front().filePath());
    ASSERT_EQUALS(0, backupBlocks.front().offset());
    ASSERT_EQUALS(0, backupBlocks.front().length());
    ASSERT_EQUALS(0, backupBlocks.front().fileSize());

    _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId);

    backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    backupBlocks =
        uassertStatusOK(backupCursorState.streamingCursor.get()->getNextBatch(1 /* batchSize */));
    ASSERT_EQUALS(1u, backupBlocks.size());
    ASSERT_EQUALS(backupRootPath / "testFile.txt", backupBlocks.front().filePath());
    ASSERT_EQUALS(0, backupBlocks.front().offset());
    ASSERT_EQUALS(0, backupBlocks.front().length());
    ASSERT_EQUALS(0, backupBlocks.front().fileSize());

    _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId);
}

TEST_F(BackupCursorServiceTest, TestDoubleOpenCursor) {
    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    ASSERT_THROWS_WHAT(
        _backupCursorService->openBackupCursor(
            _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none}),
        DBException,
        "The existing backup cursor must be closed before $backupCursor can succeed.");
    _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId);
}

TEST_F(BackupCursorServiceTest, TestDoubleCloseCursor) {
    ASSERT_THROWS_WHAT(_backupCursorService->closeBackupCursor(_opCtx.get(), UUID::gen()),
                       DBException,
                       "There is no backup cursor to close.");

    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId);
    ASSERT_THROWS_WHAT(
        _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId),
        DBException,
        "There is no backup cursor to close.");
}

TEST_F(BackupCursorServiceTest, TestCloseWrongCursor) {
    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});

    auto wrongCursor = UUID::gen();
    ASSERT_THROWS_WITH_CHECK(_backupCursorService->closeBackupCursor(_opCtx.get(), wrongCursor),
                             DBException,
                             [](const DBException& exc) {
                                 ASSERT_STRING_CONTAINS(
                                     exc.what(), "Can only close the running backup cursor.");
                             });

    _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId);
}

TEST_F(BackupCursorServiceTest, TestMixingFsyncAndCursors) {
    _backupCursorService->fsyncLock(_opCtx.get());
    ASSERT_THROWS_WHAT(_backupCursorService->openBackupCursor(
                           _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none}),
                       DBException,
                       "The node is currently fsyncLocked.");
    ASSERT_THROWS_WHAT(_backupCursorService->closeBackupCursor(_opCtx.get(), UUID::gen()),
                       DBException,
                       "There is no backup cursor to close.");
    _backupCursorService->fsyncUnlock(_opCtx.get());

    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    ASSERT_THROWS_WHAT(_backupCursorService->fsyncLock(_opCtx.get()),
                       DBException,
                       "The existing backup cursor must be closed before fsyncLock can succeed.");
    ASSERT_THROWS_WHAT(_backupCursorService->fsyncUnlock(_opCtx.get()),
                       DBException,
                       "The node is not fsyncLocked.");
    _backupCursorService->closeBackupCursor(_opCtx.get(), backupCursorState.backupId);
}

TEST_F(BackupCursorServiceTest, TestSuccessfulExtend) {
    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    auto backupId = backupCursorState.backupId;
    auto extendTo = Timestamp(100, 1);
    auto backupCursorExtendState =
        _backupCursorService->extendBackupCursor(_opCtx.get(), backupId, extendTo);
    ASSERT_EQUALS(1u, backupCursorExtendState.filePaths.size());
    ASSERT_EQUALS((backupRootPath / "journal" / "WiredTigerLog.999").string(),
                  backupCursorExtendState.filePaths[0]);

    _backupCursorService->closeBackupCursor(_opCtx.get(), backupId);
}

TEST_F(BackupCursorServiceTest, TestExtendNonExistentBackupCursor) {
    auto backupId = UUID::gen();
    auto extendTo = Timestamp(100, 1);
    ASSERT_THROWS_WITH_CHECK(
        _backupCursorService->extendBackupCursor(_opCtx.get(), backupId, extendTo),
        DBException,
        [](const DBException& exc) {
            ASSERT_STRING_CONTAINS(exc.what(),
                                   "Cannot extend backup cursor, backupId was not found.");
        });
}

TEST_F(BackupCursorServiceTest, TestFilenameCheckWithExtend) {
    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    auto backupId = backupCursorState.backupId;
    auto extendTo = Timestamp(100, 1);
    _backupCursorService->extendBackupCursor(_opCtx.get(), backupId, extendTo);

    ASSERT(_backupCursorService->isFileReturnedByCursor(
        backupId, (backupRootPath / "journal" / "WiredTigerLog.999").string()));

    // Check incorrect filename.
    ASSERT_FALSE(_backupCursorService->isFileReturnedByCursor(
        backupId, (backupRootPath / "journal" / "NotWiredTigerLog.999").string()));

    // Check incorrect backupId.
    auto wrongBackupId = UUID::gen();
    ASSERT_FALSE(_backupCursorService->isFileReturnedByCursor(
        wrongBackupId, (backupRootPath / "journal" / "WiredTigerLog.999").string()));

    _backupCursorService->closeBackupCursor(_opCtx.get(), backupId);
}

TEST_F(BackupCursorServiceTest, TestFilenamesClearedOnClose) {
    auto backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    auto backupId = backupCursorState.backupId;
    auto extendTo = Timestamp(100, 1);
    _backupCursorService->extendBackupCursor(_opCtx.get(), backupId, extendTo);

    ASSERT(_backupCursorService->isFileReturnedByCursor(
        backupId, (backupRootPath / "journal" / "WiredTigerLog.999").string()));

    // Closing the backup cursor should clear the tracked filenames.
    _backupCursorService->closeBackupCursor(_opCtx.get(), backupId);
    ASSERT(!_backupCursorService->isFileReturnedByCursor(
        backupId, (backupRootPath / "journal" / "WiredTigerLog.999").string()));

    // Opening another backup cursor should still track filenames.
    backupCursorState = _backupCursorService->openBackupCursor(
        _opCtx.get(), {false, false, kBlockSizeMB, boost::none, boost::none});
    backupId = backupCursorState.backupId;
    extendTo = Timestamp(100, 1);
    _backupCursorService->extendBackupCursor(_opCtx.get(), backupId, extendTo);

    ASSERT(_backupCursorService->isFileReturnedByCursor(
        backupId, (backupRootPath / "journal" / "WiredTigerLog.999").string()));

    _backupCursorService->closeBackupCursor(_opCtx.get(), backupId);
}

}  // namespace
}  // namespace mongo
