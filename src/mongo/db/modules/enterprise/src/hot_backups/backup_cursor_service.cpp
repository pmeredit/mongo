/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "backup_cursor_service.h"

#include <boost/filesystem.hpp>
#include <fmt/format.h>

#include "mongo/db/concurrency/replication_state_transition_lock_guard.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(backupCursorErrorAfterOpen);

std::unique_ptr<BackupCursorService> constructBackupCursorService(StorageEngine* storageEngine) {
    return std::make_unique<BackupCursorService>(storageEngine);
}

ServiceContext::ConstructorActionRegisterer registerBackupCursorHooks{
    "CreateBackupCursorHooks", [](ServiceContext* serviceContext) {
        BackupCursorHooks::registerInitializer(constructBackupCursorService);
    }};

}  // namespace

void BackupCursorService::fsyncLock(OperationContext* opCtx) {
    stdx::lock_guard<Latch> lk(_mutex);
    uassert(50885, "The node is already fsyncLocked.", _state != kFsyncLocked);
    uassert(50884,
            "The existing backup cursor must be closed before fsyncLock can succeed.",
            _state != kBackupCursorOpened);
    uassertStatusOK(_storageEngine->beginBackup(opCtx));
    _state = kFsyncLocked;
}

void BackupCursorService::fsyncUnlock(OperationContext* opCtx) {
    stdx::lock_guard<Latch> lk(_mutex);
    uassert(50888, "The node is not fsyncLocked.", _state == kFsyncLocked);
    _storageEngine->endBackup(opCtx);
    _state = kInactive;
}

BackupCursorState BackupCursorService::openBackupCursor(
    OperationContext* opCtx, const StorageEngine::BackupOptions& options) {
    // Prevent rollback
    repl::ReplicationStateTransitionLockGuard rstl(opCtx, MODE_IX);

    // Replica sets must also return the opTime's of the earliest and latest oplog entry. The
    // range represented by the oplog start/end values must exist in the backup copy, but are not
    // expected to be exact.
    repl::OpTime oplogStart;
    repl::OpTime oplogEnd;

    // If replication is enabled, get the optime of the last document in the oplog (using the last
    // applied as a proxy) before opening the backup cursor. This value will be checked again
    // after the cursor is established to guarantee it still exists (and was not truncated before
    // the backup cursor was established).
    //
    // This procedure can block, do it before acquiring the mutex to allow fsyncLock requests to
    // succeed.
    auto replCoord = repl::ReplicationCoordinator::get(opCtx);
    bool isReplSet = replCoord &&
        replCoord->getReplicationMode() == repl::ReplicationCoordinator::Mode::modeReplSet;

    if (isReplSet) {
        oplogEnd = replCoord->getMyLastAppliedOpTime();
        // If this is a primary, there may be oplog holes. The oplog range being returned must be
        // contiguous.
        auto storageInterface = repl::StorageInterface::get(opCtx);
        storageInterface->waitForAllEarlierOplogWritesToBeVisible(opCtx);
    }

    stdx::lock_guard<Latch> lk(_mutex);
    uassert(50887, "The node is currently fsyncLocked.", _state != kFsyncLocked);
    uassert(50886,
            "The existing backup cursor must be closed before $backupCursor can succeed.",
            _state != kBackupCursorOpened);

    // Capture the checkpointTimestamp before and after opening a cursor. If it hasn't moved, the
    // checkpointTimestamp is known to be exact. If it has moved, uassert and have the user retry.
    boost::optional<Timestamp> checkpointTimestamp;
    if (isReplSet) {
        checkpointTimestamp = _storageEngine->getLastStableRecoveryTimestamp();
    };

    std::unique_ptr<StorageEngine::StreamingCursor> streamingCursor;
    if (options.disableIncrementalBackup) {
        uassertStatusOK(_storageEngine->disableIncrementalBackup(opCtx));
    } else {
        streamingCursor = uassertStatusOK(_storageEngine->beginNonBlockingBackup(opCtx, options));
    }

    _state = kBackupCursorOpened;
    _activeBackupId = UUID::gen();
    _replTermOfActiveBackup = replCoord->getTerm();
    LOGV2(24200,
          "Opened backup cursor. ID: {activeBackupId} Term: {replTermOfActiveBackup}",
          "activeBackupId"_attr = *_activeBackupId,
          "replTermOfActiveBackup"_attr = *_replTermOfActiveBackup);

    // A backup cursor is open. Any exception code path must leave the BackupCursorService in an
    // inactive state.
    auto closeCursorGuard =
        makeGuard([this, opCtx, &lk] { _closeBackupCursor(opCtx, *_activeBackupId, lk); });

    uassert(50919,
            "Failpoint hit after opening the backup cursor.",
            !backupCursorErrorAfterOpen.shouldFail());

    // Ensure the checkpointTimestamp hasn't moved. A subtle case to catch is the first stable
    // checkpoint coming out of initial sync racing with opening the backup cursor.
    if (checkpointTimestamp) {
        auto requeriedCheckpointTimestamp = _storageEngine->getLastStableRecoveryTimestamp();
        if (!requeriedCheckpointTimestamp ||
            requeriedCheckpointTimestamp.get() < checkpointTimestamp.get()) {
            LOGV2_FATAL(50916,
                        "The checkpoint timestamp went backwards. Original: "
                        "{checkpointTimestamp_get} Found: {requeriedCheckpointTimestamp}",
                        "checkpointTimestamp_get"_attr = checkpointTimestamp.get(),
                        "requeriedCheckpointTimestamp"_attr = requeriedCheckpointTimestamp);
        }

        uassert(50915,
                str::stream() << "A checkpoint took place while opening a backup cursor.",
                checkpointTimestamp == requeriedCheckpointTimestamp);
    };

    // If the oplog exists, capture the first oplog entry after opening the backup cursor. Ensure
    // it is before the `oplogEnd` value.
    if (!oplogEnd.isNull()) {
        BSONObj firstEntry;
        uassert(50912,
                str::stream() << "No oplog records were found.",
                Helpers::getSingleton(
                    opCtx, NamespaceString::kRsOplogNamespace.ns().c_str(), firstEntry));
        auto oplogEntry = fassertNoTrace(50918, repl::OplogEntry::parse(firstEntry));
        oplogStart = oplogEntry.getOpTime();
        uassert(50917,
                str::stream() << "Oplog rolled over while establishing the backup cursor.",
                oplogStart < oplogEnd);
    }

    std::vector<StorageEngine::BackupBlock> eseBackupBlocks;
    auto encHooks = EncryptionHooks::get(opCtx->getServiceContext());
    if (encHooks->enabled() && !options.disableIncrementalBackup) {
        std::vector<std::string> eseFiles = uassertStatusOK(encHooks->beginNonBlockingBackup());
        for (std::string& filename : eseFiles) {
            boost::system::error_code errorCode;
            const std::uint64_t fileSize = boost::filesystem::file_size(filename, errorCode);

            using namespace fmt::literals;
            uassert(31318,
                    "Failed to get a file's size. Filename: {} Error: {}"_format(
                        filename, errorCode.message()),
                    !errorCode);

            // The database instance backing the encryption at rest data simply returns filenames
            // that need to be copied whole. The assumption is these files are small so the cost is
            // negligible.
            eseBackupBlocks.push_back({filename, 0 /* offset */, fileSize, fileSize});
        }
    }

    BSONObjBuilder builder;
    builder << "backupId" << *_activeBackupId;
    builder << "dbpath" << storageGlobalParams.dbpath;
    if (!oplogStart.isNull()) {
        builder << "oplogStart" << oplogStart.toBSON();
        builder << "oplogEnd" << oplogEnd.toBSON();
    }

    if (options.disableIncrementalBackup) {
        builder << "message"
                << "Close the cursor to release all incremental information and resources.";
    }

    // Notably during initial sync, a node may have an oplog without a stable checkpoint.
    if (checkpointTimestamp) {
        builder << "checkpointTimestamp" << checkpointTimestamp.get();
    }

    builder << "disableIncrementalBackup" << options.disableIncrementalBackup;
    builder << "incrementalBackup" << options.incrementalBackup;
    builder << "blockSize" << options.blockSizeMB;

    if (options.thisBackupName) {
        builder << "thisBackupName" << *options.thisBackupName;
    }

    if (options.srcBackupName) {
        builder << "srcBackupName" << *options.srcBackupName;
    }

    Document preamble{{"metadata", builder.obj()}};

    closeCursorGuard.dismiss();
    return {*_activeBackupId, preamble, std::move(streamingCursor), eseBackupBlocks};
}

void BackupCursorService::closeBackupCursor(OperationContext* opCtx, const UUID& backupId) {
    stdx::lock_guard<Latch> lk(_mutex);
    _closeBackupCursor(opCtx, backupId, lk);
}

BackupCursorExtendState BackupCursorService::extendBackupCursor(OperationContext* opCtx,
                                                                const UUID& backupId,
                                                                const Timestamp& extendTo) {
    stdx::lock_guard<Latch> lk(_mutex);
    uassert(51011,
            str::stream() << "Cannot extend backup cursor, backupId was not found. BackupId: "
                          << backupId,
            _activeBackupId == backupId);

    repl::ReplicationCoordinator* const replCoord = repl::ReplicationCoordinator::get(opCtx);
    uassert(51016,
            "Cannot extend backup cursor without replication enabled",
            replCoord->isReplEnabled());

    // This waiting can block for an arbitrarily long time. Clients making an `$backupCursorExtend`
    // call are recommended to pass in a `maxTimeMS`, which is obeyed in this waiting logic.
    LOGV2(24201,
          "Extending backup cursor. backupId: {backupId} extendingTo: {extendTo}",
          "backupId"_attr = backupId,
          "extendTo"_attr = extendTo);

    // Wait 1: This wait guarantees that the local lastApplied timestamp has reached at least the
    // `extendTo` timestamp.
    uassertStatusOK(replCoord->waitUntilOpTimeForRead(
        opCtx,
        repl::ReadConcernArgs(LogicalTime(extendTo), repl::ReadConcernLevel::kLocalReadConcern)));

    // Wait 2: This wait ensures that this node's majority committed timestamp is >= `extendTo`.
    // After Wait 1 and 2 complete we should be guaranteed that this node's lastApplied operation is
    // both majority committed and has a timestamp >= `extendTo`.
    uassertStatusOK(replCoord->awaitTimestampCommitted(opCtx, extendTo));

    // Wait 3: Force a journal flush because having opTime `extendTo` available for read does not
    // guarantee the persistency of the oplog with timestamp `extendTo`.
    opCtx->recoveryUnit()->waitUntilDurable(opCtx);

    std::vector<std::string> filesToBackup =
        uassertStatusOK(_storageEngine->extendBackupCursor(opCtx));
    LOGV2(24202,
          "Backup cursor has been extended. backupId: {backupId} extendedTo: {extendTo}",
          "backupId"_attr = backupId,
          "extendTo"_attr = extendTo);

    if (!_storageEngine->supportsReadConcernMajority()) {
        auto currentTerm = replCoord->getTerm();
        uassert(31055,
                "Term has been changed since opening backup cursor. This is problematic with "
                "enableMajorityReadConcern=off because it indicates the checkpoint might be rolled "
                "back. Restart the sharded backup process please.",
                currentTerm == _replTermOfActiveBackup);
    }

    return {filesToBackup};
}

bool BackupCursorService::isBackupCursorOpen() const {
    stdx::lock_guard<Latch> lk(_mutex);
    return _state == State::kBackupCursorOpened;
}

void BackupCursorService::_closeBackupCursor(OperationContext* opCtx,
                                             const UUID& backupId,
                                             WithLock) {
    uassert(50880, "There is no backup cursor to close.", _state == kBackupCursorOpened);
    uassert(50879,
            str::stream() << "Can only close the running backup cursor. To close: " << backupId
                          << " Running: " << *_activeBackupId,
            backupId == *_activeBackupId);
    _storageEngine->endNonBlockingBackup(opCtx);
    auto encHooks = EncryptionHooks::get(opCtx->getServiceContext());
    if (encHooks->enabled()) {
        fassert(50934, encHooks->endNonBlockingBackup());
    }
    LOGV2(24203, "Closed backup cursor. ID: {backupId}", "backupId"_attr = backupId);
    _state = kInactive;
    _activeBackupId = boost::none;
    _replTermOfActiveBackup = boost::none;
}
}  // namespace mongo
