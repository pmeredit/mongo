/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "import_collection.h"

#include "live_import/collection_properties_gen.h"
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
#include "mongo/db/views/view_catalog.h"
#include "mongo/logv2/log.h"
#include "mongo/util/fail_point.h"

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(noopImportCollectionCommand);

ServiceContext::ConstructorActionRegisterer registerApplyImportCollectionFn{
    "ApplyImportCollection", [](ServiceContext* serviceContext) {
        repl::registerApplyImportCollectionFn(importCollection);
    }};

}  // namespace

void importCollection(OperationContext* opCtx,
                      const UUID& importUUID,
                      const NamespaceString& nss,
                      long long numRecords,
                      long long dataSize,
                      const BSONObj& catalogEntry,
                      bool isDryRun) {
    LOGV2(5095100,
          "import collection",
          "importUUID"_attr = importUUID,
          "nss"_attr = nss,
          "numRecords"_attr = numRecords,
          "dataSize"_attr = dataSize,
          "catalogEntry"_attr = redact(catalogEntry),
          "isDryRun"_attr = isDryRun);

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

        // TODO SERVER-51140: Investigate how importCollection interacts with movePrimary.
        // TODO SERVER-51241: Audit importCollection.

        // If this is from secondary application, we keep the collection UUID in the
        // catalog entry. If this is a dryRun, we don't bother generating a new UUID (which requires
        // correcting the catalog entry metadata). Otherwise, we generate a new collection UUID for
        // the import as a primary.
        DurableCatalog::ImportCollectionUUIDOption uuidOption = importOplogSlot.isNull() || isDryRun
            ? DurableCatalog::ImportCollectionUUIDOption::kKeepOld
            : DurableCatalog::ImportCollectionUUIDOption::kGenerateNew;

        // Create Collection object
        auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
        auto importResult = uassertStatusOK(
            storageEngine->getCatalog()->importCollection(opCtx, nss, catalogEntry, uuidOption));
        std::shared_ptr<Collection> ownedCollection = Collection::Factory::get(opCtx)->make(
            opCtx, nss, importResult.catalogId, importResult.uuid, std::move(importResult.rs));
        ownedCollection->init(opCtx);
        ownedCollection->setCommitted(false);
        UncommittedCollections::addToTxn(opCtx, std::move(ownedCollection));

        // TODO SERVER-51141: Make use of numRecords and dataSize.

        if (isDryRun) {
            // This aborts the WUOW and rolls back the import.
            return;
        }

        // Fetch the catalog entry for the imported collection and log an oplog entry.
        auto importedCatalogEntry =
            storageEngine->getCatalog()->getCatalogEntry(opCtx, importResult.catalogId);
        opCtx->getServiceContext()->getOpObserver()->onImportCollection(
            opCtx, importUUID, nss, numRecords, dataSize, importedCatalogEntry, /*dryRun=*/false);

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
                         /*dryRun=*/true);

        // TODO SERVER-51060: Register an import dryRun waiter with the importUUID.

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
                /*dryRun=*/true);
            wunit.commit();
        });

        // TODO SERVER-51060: Implement voteCommitImportCollection command and wait for commit
        // votes.
    }

    // Run the actual import.
    importCollection(opCtx,
                     importUUID,
                     collectionProperties.getNs(),
                     collectionProperties.getNumRecords(),
                     collectionProperties.getDataSize(),
                     collectionProperties.getMetadata(),
                     /*dryRun=*/false);
}

}  // namespace mongo
