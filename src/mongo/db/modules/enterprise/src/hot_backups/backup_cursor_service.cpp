/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "backup_cursor_service.h"

#include <boost/filesystem.hpp>
#include <fmt/format.h>

#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/replication_state_transition_lock_guard.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/control/journal_flusher.h"
#include "mongo/db/storage/durable_catalog.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/logv2/log.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {

MONGO_FAIL_POINT_DEFINE(backupCursorErrorAfterOpen);
MONGO_FAIL_POINT_DEFINE(backupCursorHangAfterOpen);
MONGO_FAIL_POINT_DEFINE(backupCursorForceCheckpointConflict);

namespace {

std::unique_ptr<BackupCursorService> constructBackupCursorService() {
    return std::make_unique<BackupCursorService>();
}

ServiceContext::ConstructorActionRegisterer registerBackupCursorHooks{
    "CreateBackupCursorHooks", [](ServiceContext* serviceContext) {
        BackupCursorHooks::registerInitializer(constructBackupCursorService);
    }};

/**
 * Normalizes ident names with and without 'directoryPerDb' and 'wiredTigerDirectoryForIndexes'
 * mode.
 *
 * The durable catalog can return idents in four forms:
 *  - <db_name>/<collection|index>/<ident_identifier>
 *    - directoryPerDb + wiredTigerDirectoryForIndexes
 *  - <db_name>/<ident_name>
 *    - directoryPerDb
 *  - <collection|index>/<ident_identifier>
 *    - wiredTigerDirectoryForIndexes
 *  - <ident_name>
 *    - default, no options enabled
 *
 * ident_identifier: <counter>-<random number>
 * ident_name: <collection|index>-<ident_identifier>
 *
 * This function trims the leading directory names leaving only the ident's unique identifier.
 */
inline std::string getIdentStem(const std::string& ident) {
    boost::filesystem::path identPath(ident);
    return identPath.stem().string();
}

void populateMetadataFromCursor(
    OperationContext* opCtx,
    std::unique_ptr<SeekableRecordCursor> catalogCursor,
    stdx::unordered_map<std::string, std::pair<NamespaceString, UUID>>& identsToNsAndUUID) {
    while (auto record = catalogCursor->next()) {
        boost::optional<DurableCatalogEntry> entry =
            DurableCatalog::get(opCtx)->parseCatalogEntry(record->id, record->data.releaseToBson());
        if (!entry) {
            // If the record is the feature document, this will be boost::none.
            continue;
        }

        NamespaceString nss = entry->metadata->nss;

        // Remove "system.buckets." from time-series collection namespaces since it is an internal
        // detail that is not intended to be visible externally.
        if (nss.isTimeseriesBucketsCollection()) {
            nss = nss.getTimeseriesViewNamespace();
        }

        const UUID uuid = *entry->metadata->options.uuid;
        std::string collectionIdent = getIdentStem(entry->ident);

        // Add the trimmed collection ident to the map. Do not overwrite if it already exists.
        if (identsToNsAndUUID.find(collectionIdent) == identsToNsAndUUID.end()) {
            identsToNsAndUUID.emplace(collectionIdent, std::make_pair(nss, uuid));
        }

        // Add the trimmed index idents to the map. Do not overwrite if it already exists.
        for (const BSONElement& indexIdentElem : entry->indexIdents) {
            std::string indexIdent = getIdentStem(indexIdentElem.String());
            if (identsToNsAndUUID.find(indexIdent) == identsToNsAndUUID.end()) {
                identsToNsAndUUID.emplace(indexIdent, std::make_pair(nss, uuid));
            }
        }
    }
}

}  // namespace

void BackupCursorService::fsyncLock(OperationContext* opCtx) {
    stdx::lock_guard<Latch> lk(_mutex);
    uassert(50885, "The node is already fsyncLocked.", _state != kFsyncLocked);
    uassert(50884,
            "The existing backup cursor must be closed before fsyncLock can succeed.",
            _state != kBackupCursorOpened);
    uassertStatusOK(opCtx->getServiceContext()->getStorageEngine()->beginBackup(opCtx));
    _state = kFsyncLocked;
}

void BackupCursorService::fsyncUnlock(OperationContext* opCtx) {
    stdx::lock_guard<Latch> lk(_mutex);
    uassert(50888, "The node is not fsyncLocked.", _state == kFsyncLocked);
    opCtx->getServiceContext()->getStorageEngine()->endBackup(opCtx);
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
    bool isReplSet = replCoord->getSettings().isReplSet();

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

    // Open a checkpoint cursor on the catalog.
    std::unique_ptr<SeekableRecordCursor> catalogCursor;
    ReadSourceScope scope(opCtx, RecoveryUnit::ReadSource::kCheckpoint);
    try {
        catalogCursor =
            DurableCatalog::get(opCtx)->getRecordStore()->getCursor(opCtx, /*forward=*/true);
    } catch (const ExceptionFor<ErrorCodes::CursorNotFound>&) {
        // The catalog was not part of a checkpoint yet, do nothing.
    }

    auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
    boost::optional<Timestamp> checkpointTimestamp;
    if (isReplSet) {
        checkpointTimestamp = storageEngine->getLastStableRecoveryTimestamp();
    };

    std::unique_ptr<StorageEngine::StreamingCursor> streamingCursor;
    if (options.disableIncrementalBackup) {
        uassertStatusOK(storageEngine->disableIncrementalBackup(opCtx));
    } else {
        streamingCursor = uassertStatusOK(storageEngine->beginNonBlockingBackup(opCtx, options));
    }

    _state = kBackupCursorOpened;
    _activeBackupId = UUID::gen();
    _replTermOfActiveBackup = replCoord->getTerm();
    LOGV2(6858300,
          "Opened backup cursor",
          "backupId"_attr = *_activeBackupId,
          "term"_attr = *_replTermOfActiveBackup,
          "stableCheckpoint"_attr = checkpointTimestamp,
          "checkpointId"_attr =
              catalogCursor ? boost::make_optional(catalogCursor->getCheckpointId()) : boost::none,
          "lsid"_attr = opCtx->getLogicalSessionId());

    // A backup cursor is open. Any exception code path must leave the BackupCursorService in an
    // inactive state.
    ScopeGuard closeCursorGuard = [this, opCtx, &lk] {
        _closeBackupCursor(opCtx, *_activeBackupId, lk);
    };

    uassert(50919,
            "Failpoint hit after opening the backup cursor.",
            !backupCursorErrorAfterOpen.shouldFail());

    backupCursorHangAfterOpen.pauseWhileSet(opCtx);

    // Ensure the checkpoint id hasn't changed. If it has changed, fail the operation and have the
    // user retry. A subtle case to catch is the first stable checkpoint coming out of initial sync
    // racing with opening the backup cursor.
    if (!options.disableIncrementalBackup && catalogCursor) {
        // Open a new checkpoint cursor, with the previous ReadSourceScope still effective.
        auto checkpointId =
            DurableCatalog::get(opCtx)->getRecordStore()->getCursor(opCtx, true)->getCheckpointId();
        auto checkpointTimestampAfter = storageEngine->getLastStableRecoveryTimestamp();
        if (catalogCursor->getCheckpointId() != checkpointId ||
            checkpointTimestamp != checkpointTimestampAfter) {
            LOGV2(8412100,
                  "A checkpoint took place while opening a backup cursor.",
                  "checkpointIdBefore"_attr = catalogCursor->getCheckpointId(),
                  "checkpointIdAfter"_attr = checkpointId,
                  "checkpointTimestampBefore"_attr = checkpointTimestamp,
                  "checkpointTimestampAfter"_attr = checkpointTimestampAfter);
            uassert(ErrorCodes::BackupCursorOpenConflictWithCheckpoint,
                    "A checkpoint took place while opening a backup cursor.",
                    false);
        }
    }

    // If the oplog exists, capture the first oplog entry after opening the backup cursor. Ensure
    // it is before the `oplogEnd` value.
    if (!oplogEnd.isNull()) {
        BSONObj firstEntry;
        uassert(50912,
                str::stream() << "No oplog records were found.",
                Helpers::getSingleton(opCtx, NamespaceString::kRsOplogNamespace, firstEntry));
        oplogStart = repl::OpTime::parse(firstEntry);
        uassert(50917,
                str::stream() << "Oplog rolled over while establishing the backup cursor."
                              << " First entry:" << oplogStart.toString()
                              << ". Last entry:" << oplogEnd.toString() << ".",
                oplogStart < oplogEnd);
    }

    stdx::unordered_map<std::string, std::pair<NamespaceString, UUID>> identsToNsAndUUID;

    // Populate the metadata from the checkpoint cursor, if it's open.
    if (catalogCursor) {
        populateMetadataFromCursor(opCtx, std::move(catalogCursor), identsToNsAndUUID);
    }

    // Collections created later than the latest checkpoint will still be reported by the backup
    // cursor, so fetch the namespace from the latest catalog as a best guess.
    {
        ReadSourceScope scope(opCtx, RecoveryUnit::ReadSource::kNoTimestamp);
        catalogCursor =
            DurableCatalog::get(opCtx)->getRecordStore()->getCursor(opCtx, /*forward=*/true);
        populateMetadataFromCursor(opCtx, std::move(catalogCursor), identsToNsAndUUID);
    }

    if (streamingCursor) {
        streamingCursor->setCatalogEntries(identsToNsAndUUID);
    }

    std::deque<BackupBlock> eseBackupBlocks;
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
            eseBackupBlocks.push_back(BackupBlock(opCtx,
                                                  boost::none /* nss */,
                                                  boost::none /* uuid */,
                                                  filename,
                                                  0 /* offset */,
                                                  fileSize,
                                                  fileSize));
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
        builder << "checkpointTimestamp" << checkpointTimestamp.value();
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
    return {*_activeBackupId, preamble, std::move(streamingCursor), std::move(eseBackupBlocks)};
}

void BackupCursorService::closeBackupCursor(OperationContext* opCtx, const UUID& backupId) {
    stdx::lock_guard<Latch> lk(_mutex);
    _closeBackupCursor(opCtx, backupId, lk);
}

void BackupCursorService::addFilename(const UUID& backupId, std::string filename) {
    stdx::lock_guard<Latch> lk(_mutex);
    tassert(57807, "_activeBackupId should equal backupId", _activeBackupId == backupId);
    _returnedFilenames.insert(filename);
}

bool BackupCursorService::isFileReturnedByCursor(const UUID& backupId, std::string filename) {
    stdx::lock_guard<Latch> lk(_mutex);
    return _activeBackupId == backupId && _returnedFilenames.contains(filename);
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
            replCoord->getSettings().isReplSet());

    auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
    boost::optional<Timestamp> checkpointTimestamp;
    if (replCoord->getSettings().isReplSet()) {
        checkpointTimestamp = storageEngine->getLastStableRecoveryTimestamp();
    };

    // This waiting can block for an arbitrarily long time. Clients making an `$backupCursorExtend`
    // call are recommended to pass in a `maxTimeMS`, which is obeyed in this waiting logic.
    LOGV2(6858301,
          "Extending backup cursor",
          "backupId"_attr = backupId,
          "extendTo"_attr = extendTo,
          "stableCheckpoint"_attr = checkpointTimestamp);

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
    JournalFlusher::get(opCtx)->waitForJournalFlush();

    std::deque<std::string> filesToBackup =
        uassertStatusOK(storageEngine->extendBackupCursor(opCtx));
    LOGV2(24202,
          "Backup cursor has been extended",
          "backupId"_attr = backupId,
          "extendTo"_attr = extendTo);

    if (!storageEngine->supportsReadConcernMajority()) {
        auto currentTerm = replCoord->getTerm();
        uassert(31055,
                "Term has been changed since opening backup cursor. This is problematic with "
                "enableMajorityReadConcern=off because it indicates the checkpoint might be rolled "
                "back. Restart the sharded backup process please.",
                currentTerm == _replTermOfActiveBackup);
    }

    // Copy filenames from `filesToBackup` to `returnedFilenames`.
    std::copy(filesToBackup.begin(),
              filesToBackup.end(),
              std::inserter(_returnedFilenames, _returnedFilenames.end()));

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
    opCtx->getServiceContext()->getStorageEngine()->endNonBlockingBackup(opCtx);
    auto encHooks = EncryptionHooks::get(opCtx->getServiceContext());
    if (encHooks->enabled()) {
        fassert(50934, encHooks->endNonBlockingBackup());
    }
    LOGV2(24203, "Closed backup cursor", "backupId"_attr = backupId);
    _state = kInactive;
    _activeBackupId = boost::none;
    _replTermOfActiveBackup = boost::none;
    _returnedFilenames.clear();
}
}  // namespace mongo
