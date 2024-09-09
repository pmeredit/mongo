/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <vector>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/storage/backup_cursor_hooks.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/concurrency/with_lock.h"
#include "mongo/util/uuid.h"

namespace mongo {

class OperationContext;
class ServiceContext;
class StorageEngine;

/**
 * MongoDB exposes two separate client APIs for locking down files on disk for a file-system based
 * backup that require only one pass of the data to complete. First is using the `fsync` command
 * with `lock: true`. The second is using the `$backupCursor` aggregation stage. It does not make
 * sense for both techniques to be concurrently engaged. This class will manage access to these
 * resources such that their lifetimes do not overlap. More specifically, an `fsyncLock` mode can
 * only be freed by `fsyncUnlock` and `openBackupCursor` by `closeBackupCursor`.
 *
 * Failure to comply is expected to be due to user error and this class is best situated to detect
 * such errors. For this reason, callers should assume all method calls can only fail with
 * uassert exceptions.
 */
class BackupCursorService : public BackupCursorHooks {
    BackupCursorService(const BackupCursorService&) = delete;
    BackupCursorService& operator=(const BackupCursorService&) = delete;

public:
    BackupCursorService() {}

    bool enabled() const override {
        return true;
    }

    /**
     * This method will uassert if `_state` is not `kInactive`. Otherwise, the method forwards to
     * `StorageEngine::beginBackup`.
     */
    void fsyncLock(OperationContext* opCtx) override;

    /**
     * This method will uassert if `_state` is not `kFsyncLocked`. Otherwise, the method forwards
     * to `StorageEngine::endBackup`.
     */
    void fsyncUnlock(OperationContext* opCtx) override;

    /**
     * This method will uassert if `_state` is not `kInactive`. Otherwise, the method forwards to
     * `StorageEngine::beginNonBlockingBackup`.
     *
     * Returns a BackupCursorState. The structure's `backupId` must be passed to
     * `closeBackupCursor` to successfully exit this backup mode. `filenames` is a list of files
     * relative to the server's `dbpath` that must be copied by the application to complete a
     * backup.
     */
    BackupCursorState openBackupCursor(OperationContext* opCtx,
                                       const StorageEngine::BackupOptions& options) override;

    /**
     * This method will uassert if `_state` is not `kBackupCursorOpened`, or the `backupId` input
     * is not the active backup cursor open known to `_activeBackupId`.
     */
    void closeBackupCursor(OperationContext* opCtx, const UUID& backupId) override;

    /**
     * Blocks until oplog with `extendTo` is majority committed and persistent, and returns a list
     * of journal files covering a timeframe between the timestamp backed up by 'backupId' and the
     * provided 'extendTo' timestamp. This function may block for an arbitrarily long time so
     * callers should be responsible for setting timeout to interrupt the operation if needed.
     *
     * This method will uassert if a backup cursor is not currently open associated with 'backupId'.
     */
    BackupCursorExtendState extendBackupCursor(OperationContext* opCtx,
                                               const UUID& backupId,
                                               const Timestamp& extendTo) override;

    bool isFileReturnedByCursor(const UUID& backupId, boost::filesystem::path filePath) override;

    void addFile(const UUID& backupId, boost::filesystem::path filePath) override;

    bool isBackupCursorOpen() const override {
        return _state.load() == State::kBackupCursorOpened;
    }

    UUID getBackupId() {
        return _activeBackupId.get();
    }

private:
    void _closeBackupCursor(OperationContext* opCtx, const UUID& backupId, WithLock);

    enum State { kInactive, kFsyncLocked, kBackupCursorOpened };
    AtomicWord<State> _state{kInactive};

    mutable Mutex _mutex = MONGO_MAKE_LATCH("BackupCursorService::_mutex");
    // When state is `kBackupCursorOpened`, _activeBackupId contains an UUID which uniquely
    // identifies the active backup cursor. Otherwise it is boost::none.
    boost::optional<UUID> _activeBackupId = boost::none;

    // Tracks the filenames returned by the open backup cursor or backup cursor extension.
    stdx::unordered_set<std::string> _returnedFilePaths;
};

}  // namespace mongo
