/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <vector>

#include "mongo/db/pipeline/document.h"
#include "mongo/db/storage/backup_cursor_hooks.h"
#include "mongo/stdx/mutex.h"
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
    BackupCursorService(StorageEngine* storageEngine) : _storageEngine(storageEngine) {}

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
    BackupCursorState openBackupCursor(OperationContext* opCtx) override;

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

    bool isBackupCursorOpen() const override;

private:
    void _closeBackupCursor(OperationContext* opCtx, const UUID& backupId, WithLock);

    StorageEngine* _storageEngine;

    enum State { kInactive, kFsyncLocked, kBackupCursorOpened };

    // This mutex serializes all access into this class.
    mutable stdx::mutex _mutex;
    State _state = kInactive;
    // When state is `kBackupCursorOpened`, _activeBackupId contains an UUID which uniquely
    // identifies the active backup cursor. Otherwise it is boost::none.
    boost::optional<UUID> _activeBackupId = boost::none;
    std::set<std::string> _backupFiles;
};

}  // namespace mongo
