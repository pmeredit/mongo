/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "import_collection.h"

#include "live_import/collection_properties_gen.h"
#include "live_import/import_collection_coordinator.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/audit.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/uncommitted_collections.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/storage/durable_catalog.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/logv2/log.h"
#include "mongo/util/fail_point.h"

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(failImportCollectionApplication);
MONGO_FAIL_POINT_DEFINE(hangBeforeWaitingForImportDryRunVotes);
MONGO_FAIL_POINT_DEFINE(noopImportCollectionCommand);

Status applyImportCollectionNoThrow(OperationContext* opCtx,
                                    const UUID& importUUID,
                                    const NamespaceString& nss,
                                    long long numRecords,
                                    long long dataSize,
                                    const BSONObj& catalogEntry,
                                    const BSONObj& storageMetadata,
                                    bool isDryRun) noexcept {
    auto status = [&]() {
        try {
            if (MONGO_unlikely(failImportCollectionApplication.shouldFail())) {
                return Status(ErrorCodes::OperationFailed,
                              "failImportCollectionApplication fail point enabled");
            }
            importCollection(opCtx,
                             importUUID,
                             nss,
                             numRecords,
                             dataSize,
                             catalogEntry,
                             storageMetadata,
                             isDryRun);
            return Status::OK();
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }();
    LOGV2(5106000,
          "Finished applying importCollection",
          "importUUID"_attr = importUUID,
          "nss"_attr = nss,
          "numRecords"_attr = numRecords,
          "dataSize"_attr = dataSize,
          "catalogEntry"_attr = redact(catalogEntry),
          "storageMetadata"_attr = redact(storageMetadata),
          "isDryRun"_attr = isDryRun,
          "status"_attr = status);
    return status;
}

void applyImportCollection(OperationContext* opCtx,
                           const UUID& importUUID,
                           const NamespaceString& nss,
                           long long numRecords,
                           long long dataSize,
                           const BSONObj& catalogEntry,
                           const BSONObj& storageMetadata,
                           bool isDryRun,
                           repl::OplogApplication::Mode mode) {
    // Skip applying dry run unless we are in steady state replication.
    if (isDryRun && mode != repl::OplogApplication::Mode::kSecondary) {
        return;
    }

    if (isDryRun) {
        // Apply the dry run asynchronously to avoid blocking replication because it could take a
        // long time to dropIdent after the dry run if a checkpoint is running. And voting also
        // takes time.
        stdx::thread([=,
                      catalogEntry = catalogEntry.getOwned(),
                      storageMetadata = storageMetadata.getOwned()] {
            Client::initThread("ImportDryRun");
            auto client = Client::getCurrent();
            {
                stdx::lock_guard<Client> lk(*client);
                // Mark this system client killable by replication state transitions.
                client->setSystemOperationKillableByStepdown(lk);
            }
            auto opCtx = cc().makeOperationContext();
            opCtx->setShouldParticipateInFlowControl(false);
            repl::UnreplicatedWritesBlock uwb(opCtx.get());
            ShouldNotConflictWithSecondaryBatchApplicationBlock shouldNotConflictBlock(
                opCtx->lockState());

            auto status = applyImportCollectionNoThrow(opCtx.get(),
                                                       importUUID,
                                                       nss,
                                                       numRecords,
                                                       dataSize,
                                                       catalogEntry,
                                                       storageMetadata,
                                                       isDryRun);

            // Vote for the import.
            auto replCoord = repl::ReplicationCoordinator::get(opCtx.get());
            auto const voteCmdRequest =
                BSON("voteCommitImportCollection"
                     << importUUID << "from" << replCoord->getMyHostAndPort().toString()
                     << "dryRunSuccess" << status.isOK() << "reason" << status.reason());
            try {
                auto onRemoteCmdScheduled = [](executor::TaskExecutor::CallbackHandle handle) {};
                auto onRemoteCmdComplete = [](executor::TaskExecutor::CallbackHandle handle) {};
                auto voteCmdResponse =
                    replCoord->runCmdOnPrimaryAndAwaitResponse(opCtx.get(),
                                                               "admin",
                                                               voteCmdRequest,
                                                               onRemoteCmdScheduled,
                                                               onRemoteCmdComplete);
                LOGV2(5106001,
                      "Voted for importCollection",
                      "voteCmdRequest"_attr = voteCmdRequest,
                      "voteCmdResponse"_attr = voteCmdResponse);
            } catch (const DBException& e) {
                // As this is a best-effort vote, ignore all errors.
                LOGV2_ERROR(5106002,
                            "Failed to run voteCommitImportCollection against the primary",
                            "error"_attr = e.toStatus());
            }
        })
            .detach();
    } else {
        // Not a dry run.
        uassertStatusOK(applyImportCollectionNoThrow(
            opCtx, importUUID, nss, numRecords, dataSize, catalogEntry, storageMetadata, isDryRun));
    }
}

ServiceContext::ConstructorActionRegisterer registerApplyImportCollectionFn{
    "ApplyImportCollection", [](ServiceContext* serviceContext) {
        repl::registerApplyImportCollectionFn(applyImportCollection);
    }};

class CountsChange : public RecoveryUnit::Change {
public:
    CountsChange(WiredTigerRecordStore* rs, long long numRecords, long long dataSize)
        : _rs(rs), _numRecords(numRecords), _dataSize(dataSize) {}
    void commit(boost::optional<Timestamp>) {
        _rs->setNumRecords(_numRecords);
        _rs->setDataSize(_dataSize);
    }
    void rollback() {}

private:
    WiredTigerRecordStore* _rs;
    long long _numRecords;
    long long _dataSize;
};

}  // namespace

void importCollection(OperationContext* opCtx,
                      const UUID& importUUID,
                      const NamespaceString& nss,
                      long long numRecords,
                      long long dataSize,
                      const BSONObj& catalogEntry,
                      const BSONObj& storageMetadata,
                      bool isDryRun) {
    LOGV2(5095100,
          "import collection",
          "importUUID"_attr = importUUID,
          "nss"_attr = nss,
          "numRecords"_attr = numRecords,
          "dataSize"_attr = dataSize,
          "catalogEntry"_attr = redact(catalogEntry),
          "storageMetadata"_attr = storageMetadata,
          "isDryRun"_attr = isDryRun);

    // Since a global IX lock is taken during the import, the opCtx is already guaranteed to be
    // killed during stepDown/stepUp. But it is better to make this explicit for clarity.
    opCtx->setAlwaysInterruptAtStepDownOrUp();

    return writeConflictRetry(opCtx, "importCollection", nss.ns(), [&] {
        AutoGetOrCreateDb autoDb(opCtx, nss.db(), MODE_IX);
        // Since we do not need to support running importCollection as part of multi-document
        // transactions and there is very little value allowing multiple imports of the same
        // namespace to run in parallel, we can take a MODE_X lock here to simplify the concurrency
        // control of importCollection.
        Lock::CollectionLock collLock(opCtx, nss, MODE_X);

        uassert(ErrorCodes::NotWritablePrimary,
                str::stream() << "Not primary while importing collection " << nss,
                !opCtx->writesAreReplicated() ||
                    repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, nss));

        uassert(ErrorCodes::DatabaseDropPending,
                str::stream() << "The database is in the process of being dropped " << nss.db(),
                !autoDb.getDb()->isDropPending(opCtx));

        uassert(ErrorCodes::NamespaceExists,
                str::stream() << "Collection already exists. NS: " << nss,
                !CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, nss));

        uassert(ErrorCodes::NamespaceExists,
                str::stream() << "A view already exists. NS: " << nss,
                !ViewCatalog::get(autoDb.getDb())->lookup(opCtx, nss.ns()));

        WriteUnitOfWork wunit(opCtx);

        AutoStatsTracker statsTracker(
            opCtx,
            nss,
            Top::LockType::NotLocked,
            AutoStatsTracker::LogMode::kUpdateTopAndCurOp,
            CollectionCatalog::get(opCtx).getDatabaseProfileLevel(nss.db()));

        // If the collection creation rolls back, ensure that the Top entry created for the
        // collection is deleted.
        opCtx->recoveryUnit()->onRollback([nss, serviceContext = opCtx->getServiceContext()]() {
            Top::get(serviceContext).collectionDropped(nss);
        });

        // In order to make the storage timestamp for the collection import always correct even when
        // other operations are present in the same storage transaction, we reserve an opTime before
        // the collection import, then pass it to the opObserver. Reserving the optime automatically
        // sets the storage timestamp for future writes.
        // TODO SERVER-51254: Support or ban importing unreplicated collections.
        OplogSlot importOplogSlot;
        auto replCoord = repl::ReplicationCoordinator::get(opCtx);
        if (opCtx->writesAreReplicated() && !replCoord->isOplogDisabledFor(opCtx, nss)) {
            importOplogSlot = repl::getNextOpTime(opCtx);
        }

        if (!isDryRun) {
            audit::logImportCollection(&cc(), nss.ns());
        }

        // If this is from secondary application, we keep the collection UUID in the
        // catalog entry. If this is a dryRun, we don't bother generating a new UUID (which requires
        // correcting the catalog entry metadata). Otherwise, we generate a new collection UUID for
        // the import as a primary.
        DurableCatalog::ImportCollectionUUIDOption uuidOption = importOplogSlot.isNull() || isDryRun
            ? DurableCatalog::ImportCollectionUUIDOption::kKeepOld
            : DurableCatalog::ImportCollectionUUIDOption::kGenerateNew;

        // Create Collection object
        auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
        auto importResult = uassertStatusOK(storageEngine->getCatalog()->importCollection(
            opCtx, nss, catalogEntry, storageMetadata, uuidOption));
        std::shared_ptr<Collection> ownedCollection = Collection::Factory::get(opCtx)->make(
            opCtx, nss, importResult.catalogId, importResult.uuid, std::move(importResult.rs));
        ownedCollection->init(opCtx);
        ownedCollection->setCommitted(false);

        // Update the number of records and data size appropriately on commit.
        opCtx->recoveryUnit()->registerChange(std::make_unique<CountsChange>(
            static_cast<WiredTigerRecordStore*>(ownedCollection->getRecordStore()),
            numRecords,
            dataSize));

        UncommittedCollections::addToTxn(opCtx, std::move(ownedCollection));

        if (isDryRun) {
            // This aborts the WUOW and rolls back the import.
            return;
        }

        // Fetch the catalog entry for the imported collection and log an oplog entry.
        auto importedCatalogEntry =
            storageEngine->getCatalog()->getCatalogEntry(opCtx, importResult.catalogId);
        opCtx->getServiceContext()->getOpObserver()->onImportCollection(opCtx,
                                                                        importUUID,
                                                                        nss,
                                                                        numRecords,
                                                                        dataSize,
                                                                        importedCatalogEntry,
                                                                        storageMetadata,
                                                                        /*dryRun=*/false);

        wunit.commit();
    });
}

void runImportCollectionCommand(OperationContext* opCtx,
                                const NamespaceString& nss,
                                const BSONObj& collectionPropertiesObj,
                                bool force) {
    // TODO SERVER-51143: Additional sanity check on the input if necessary.

    auto collectionProperties = CollectionProperties::parse(
        IDLParserErrorContext("collectionProperties"), collectionPropertiesObj);

    uassert(ErrorCodes::BadValue,
            "Namespace in collectionProperties doesn't match with the import namesapce",
            nss == collectionProperties.getNs());

    auto importUUID = UUID::gen();

    if (MONGO_unlikely(noopImportCollectionCommand.shouldFail())) {
        return;
    }

    if (!force) {
        // Run dryRun and wait for commit votes.
        importCollection(opCtx,
                         importUUID,
                         collectionProperties.getNs(),
                         collectionProperties.getNumRecords(),
                         collectionProperties.getDataSize(),
                         collectionProperties.getMetadata(),
                         collectionProperties.getStorageMetadata(),
                         /*dryRun=*/true);

        auto importCoord = ImportCollectionCoordinator::get(opCtx);

        // Register the current import.
        auto future = importCoord->registerImport(importUUID);
        ON_BLOCK_EXIT([&] { importCoord->unregisterImport(importUUID); });

        // Vote success for this node itself.
        importCoord->voteForImport(importUUID,
                                   repl::ReplicationCoordinator::get(opCtx)->getMyHostAndPort(),
                                   /*success=*/true);

        // Write an import oplog entry for the dryRun.
        invariant(!opCtx->lockState()->inAWriteUnitOfWork());
        writeConflictRetry(opCtx, "onImportCollection", nss.ns(), [&] {
            Lock::GlobalLock lk(opCtx, MODE_IX);
            WriteUnitOfWork wunit(opCtx);
            opCtx->getServiceContext()->getOpObserver()->onImportCollection(
                opCtx,
                importUUID,
                nss,
                collectionProperties.getNumRecords(),
                collectionProperties.getDataSize(),
                collectionProperties.getMetadata(),
                collectionProperties.getStorageMetadata(),
                /*dryRun=*/true);
            wunit.commit();
        });

        hangBeforeWaitingForImportDryRunVotes.pauseWhileSet();

        // Wait for votes of the dryRun.
        uassertStatusOK(future.getNoThrow(opCtx));
    }

    // Run the actual import.
    importCollection(opCtx,
                     importUUID,
                     collectionProperties.getNs(),
                     collectionProperties.getNumRecords(),
                     collectionProperties.getDataSize(),
                     collectionProperties.getMetadata(),
                     collectionProperties.getStorageMetadata(),
                     /*dryRun=*/false);
}

}  // namespace mongo
