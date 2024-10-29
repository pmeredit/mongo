/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "import_collection.h"

#include "audit/audit_manager.h"
#include "live_import/import_collection_coordinator.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/audit.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/op_observer/op_observer.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/profile_settings.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/storage/durable_catalog.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/idl/cluster_parameter_synchronization_helpers.h"
#include "mongo/logv2/log.h"
#include "mongo/util/fail_point.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(failImportCollectionApplication);
MONGO_FAIL_POINT_DEFINE(hangAfterImportDryRun);
MONGO_FAIL_POINT_DEFINE(hangBeforeWaitingForImportDryRunVotes);
MONGO_FAIL_POINT_DEFINE(noopImportCollectionCommand);
MONGO_FAIL_POINT_DEFINE(abortImportAfterOpObserver);

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
            Client::initThread("ImportDryRun",
                               getGlobalServiceContext()->getService(ClusterRole::ShardServer));
            auto opCtx = cc().makeOperationContext();
            repl::UnreplicatedWritesBlock uwb(opCtx.get());

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
                                                               DatabaseName::kAdmin,
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
        }).detach();
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
    opCtx->setAlwaysInterruptAtStepDownOrUp_UNSAFE();

    return writeConflictRetry(opCtx, "importCollection", nss, [&] {
        AutoGetDb autoDb(opCtx, nss.dbName(), MODE_IX);
        // Since we do not need to support running importCollection as part of multi-document
        // transactions and there is very little value allowing multiple imports of the same
        // namespace to run in parallel, we can take a MODE_X lock here to simplify the concurrency
        // control of importCollection.
        Lock::CollectionLock collLock(opCtx, nss, MODE_X);

        auto catalog = CollectionCatalog::get(opCtx);
        uassert(ErrorCodes::NotWritablePrimary,
                str::stream() << "Not primary while importing collection "
                              << nss.toStringForErrorMsg(),
                !opCtx->writesAreReplicated() ||
                    repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, nss));

        autoDb.ensureDbExists(opCtx);
        uassert(ErrorCodes::DatabaseDropPending,
                str::stream() << "The database is in the process of being dropped "
                              << nss.dbName().toStringForErrorMsg(),
                !catalog->isDropPending(nss.dbName()));

        uassert(ErrorCodes::NamespaceExists,
                str::stream() << "Collection already exists. NS: " << nss.toStringForErrorMsg(),
                !catalog->lookupCollectionByNamespace(opCtx, nss));

        uassert(ErrorCodes::NamespaceExists,
                str::stream() << "A view already exists. NS: " << nss.toStringForErrorMsg(),
                !catalog->lookupView(opCtx, nss));
        {
            WriteUnitOfWork wunit(opCtx);

            AutoStatsTracker statsTracker(opCtx,
                                          nss,
                                          Top::LockType::NotLocked,
                                          AutoStatsTracker::LogMode::kUpdateTopAndCurOp,
                                          DatabaseProfileSettings::get(opCtx->getServiceContext())
                                              .getDatabaseProfileLevel(nss.dbName()));

            // If the collection creation rolls back, ensure that the Top entry created for the
            // collection is deleted.
            shard_role_details::getRecoveryUnit(opCtx)->onRollback(
                [nss, serviceContext = opCtx->getServiceContext()](OperationContext*) {
                    Top::get(serviceContext).collectionDropped(nss);
                });

            // In order to make the storage timestamp for the collection import always correct even
            // when other operations are present in the same storage transaction, we reserve an
            // opTime before the collection import, then pass it to the opObserver. Reserving the
            // optime automatically sets the storage timestamp for future writes.
            OplogSlot importOplogSlot;
            auto replCoord = repl::ReplicationCoordinator::get(opCtx);
            if (opCtx->writesAreReplicated()) {
                uassert(ErrorCodes::CommandNotSupported,
                        "Importing unreplicated collections is not supported",
                        !replCoord->isOplogDisabledFor(opCtx, nss));
                importOplogSlot = repl::getNextOpTime(opCtx);
            }

            if (!isDryRun) {
                audit::logImportCollection(&cc(), nss);
            }

            // Skip the actual import for the noopImportCollectionCommand fail point.
            if (MONGO_unlikely(noopImportCollectionCommand.shouldFail())) {
                return;
            }

            // If this is from secondary application, we keep the collection UUID in the
            // catalog entry. If this is a dryRun, we don't bother generating a new UUID (which
            // requires correcting the catalog entry metadata). Otherwise, we generate a new
            // collection UUID for the import as a primary.
            ImportOptions::ImportCollectionUUIDOption uuidOption =
                importOplogSlot.isNull() || isDryRun
                ? ImportOptions::ImportCollectionUUIDOption::kKeepOld
                : ImportOptions::ImportCollectionUUIDOption::kGenerateNew;

            uassert(ErrorCodes::NamespaceExists,
                    str::stream() << "Collection already exists. NS: " << nss.toStringForErrorMsg(),
                    !CollectionCatalog::get(opCtx)->lookupCollectionByNamespace(opCtx, nss));

            // Create Collection object
            auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
            auto durableCatalog = storageEngine->getCatalog();
            auto opts = ImportOptions(uuidOption);
            // Panic on dry run so it fails early, if it fails on the real thing we want to retry
            // with repair
            opts.panicOnCorruptWtMetadata = isDryRun;

            auto status =
                durableCatalog->importCollection(opCtx, nss, catalogEntry, storageMetadata, opts);
            if (!status.isOK()) {
                LOGV2_ERROR(9616600,
                            "importCollection failed",
                            "error"_attr = status.getStatus().toString());

                if (!isDryRun) {
                    opts.panicOnCorruptWtMetadata = true;
                    opts.repair = true;
                    status = durableCatalog->importCollection(
                        opCtx, nss, catalogEntry, storageMetadata, opts);
                }
                uassertStatusOK(status.getStatus());
            }
            auto importResult = std::move(status.getValue());

            const auto catalogEntry =
                durableCatalog->getParsedCatalogEntry(opCtx, importResult.catalogId);
            const auto md = catalogEntry->metadata;
            std::shared_ptr<Collection> ownedCollection = Collection::Factory::get(opCtx)->make(
                opCtx, nss, importResult.catalogId, md, std::move(importResult.rs));

            {
                // Validate index spec.
                StringMap<bool> seenIndex;
                for (const auto& index : md->indexes) {
                    uassert(ErrorCodes::BadValue, "Cannot import non-ready indexes", index.ready);
                    auto swValidatedSpec = ownedCollection->getIndexCatalog()->prepareSpecForCreate(
                        opCtx, CollectionPtr(ownedCollection.get()), index.spec, boost::none);
                    if (!swValidatedSpec.isOK()) {
                        uasserted(ErrorCodes::BadValue, swValidatedSpec.getStatus().reason());
                    }
                    auto validatedSpec = swValidatedSpec.getValue();
                    uassert(ErrorCodes::BadValue,
                            str::stream() << "Validated index spec " << validatedSpec
                                          << " does not match original " << index.spec,
                            index.spec.woCompare(validatedSpec,
                                                 /*ordering=*/BSONObj(),
                                                 BSONObj::ComparisonRules::kIgnoreFieldOrder) == 0);
                    uassert(ErrorCodes::BadValue,
                            str::stream() << "Duplicate index name found in spec list: "
                                          << md->toBSON()["indexes"],
                            !seenIndex[index.nameStringData()]);
                    seenIndex[index.nameStringData()] = true;
                }
            }

            ownedCollection->init(opCtx);

            // Update the number of records and data size appropriately on commit.
            shard_role_details::getRecoveryUnit(opCtx)->onCommit(
                [numRecords,
                 dataSize,
                 rs = static_cast<WiredTigerRecordStore*>(ownedCollection->getRecordStore())](
                    OperationContext*, boost::optional<Timestamp>) {
                    rs->setNumRecords(numRecords);
                    rs->setDataSize(dataSize);
                });

            // don't std::move, we will need access to the records later for auditing
            CollectionCatalog::get(opCtx)->onCreateCollection(opCtx, ownedCollection);

            if (isDryRun) {
                // Force a checkpoint to ensure rollback with remove_files=false doesn't hang on
                // EBUSY (SERVER-95921)
                opCtx->getServiceContext()->getStorageEngine()->checkpoint();
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

            if (audit::getGlobalAuditManager()->isEnabled() && nss.isPrivilegeCollection()) {
                const auto cursor = ownedCollection->getCursor(opCtx);
                while (const auto record = cursor->next()) {
                    audit::logInsertOperation(opCtx->getClient(), nss, record->data.toBson());
                }
            }

            if (MONGO_unlikely(abortImportAfterOpObserver.shouldFail())) {
                uasserted(ErrorCodes::OperationFailed,
                          "Aborting import due to failpoint, no commit!");
            }
            wunit.commit();

            if (numRecords > 0 &&
                nss == NamespaceString::makeClusterParametersNSS(nss.tenantId())) {
                cluster_parameters::initializeAllTenantParametersFromCollection(opCtx,
                                                                                *ownedCollection);
            }
        }
    });
}

void runImportCollectionCommand(OperationContext* opCtx,
                                const CollectionProperties& collectionProperties,
                                bool force) {
    uassert(ErrorCodes::InvalidOptions,
            "This collection was exported from a system with a different directoryPerDB setting",
            collectionProperties.getDirectoryPerDB() == storageGlobalParams.directoryperdb);
    uassert(
        ErrorCodes::InvalidOptions,
        "This collection was exported from a system with a different directoryForIndexes setting",
        collectionProperties.getDirectoryForIndexes() ==
            wiredTigerGlobalOptions.directoryForIndexes);

    auto importUUID = UUID::gen();

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
        ScopeGuard unregisterGuard = [&] { importCoord->unregisterImport(importUUID); };

        // Vote success for this node itself.
        importCoord->voteForImport(importUUID,
                                   repl::ReplicationCoordinator::get(opCtx)->getMyHostAndPort(),
                                   /*success=*/true);

        // Write an import oplog entry for the dryRun.
        invariant(!shard_role_details::getLocker(opCtx)->inAWriteUnitOfWork());
        writeConflictRetry(opCtx, "onImportCollection", collectionProperties.getNs(), [&] {
            Lock::GlobalLock lk(opCtx, MODE_IX);
            WriteUnitOfWork wunit(opCtx);
            opCtx->getServiceContext()->getOpObserver()->onImportCollection(
                opCtx,
                importUUID,
                collectionProperties.getNs(),
                collectionProperties.getNumRecords(),
                collectionProperties.getDataSize(),
                collectionProperties.getMetadata(),
                collectionProperties.getStorageMetadata(),
                /*dryRun=*/true);
            wunit.commit();
        });
        // We intentionally do not update in-memory cluster parameter state because this is a dry
        // run.

        hangBeforeWaitingForImportDryRunVotes.pauseWhileSet();

        // Wait for votes of the dryRun.
        uassertStatusOK(future.getNoThrow(opCtx));

        hangAfterImportDryRun.pauseWhileSet();
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
