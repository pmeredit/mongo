/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem.hpp>
#include <boost/optional/optional.hpp>
#include <string>
// IWYU pragma: no_include "cxxabi.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/modules/enterprise/src/hot_backups/backup_cursor_service.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_impl.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/durable_catalog.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/db/storage/write_unit_of_work.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace mongo {

namespace {

const NamespaceString kNss = NamespaceString::createNamespaceString_forTest("db", "collection");

}  // namespace

/**
 * A test fixture to unit test backup cursor functionalities.
 */
class BackupCursorServiceTest : public ServiceContextMongoDTest {
public:
    // Disable table logging. When table logging is enabled, timestamps are discarded by
    // WiredTiger. Don't use in-memory mode. We can't open backup cursors on in-memory
    // WiredTiger.
    BackupCursorServiceTest()
        : ServiceContextMongoDTest(Options{}.forceDisableTableLogging().ephemeral(false)) {}

    void setUp() override {
        ServiceContextMongoDTest::setUp();
        opCtx = ServiceContextTest::makeOperationContext();
        backupCursorService = std::make_unique<BackupCursorService>();

        storageEngine = opCtx->getServiceContext()->getStorageEngine();
        storageEngine->setInitialDataTimestamp(Timestamp(1, 1));

        repl::ReplSettings settings;
        settings.setReplSetString("mock");
        auto replCoord = std::make_unique<repl::ReplicationCoordinatorMock>(
            opCtx->getServiceContext(), settings);
        ASSERT_OK(replCoord->setFollowerMode(repl::MemberState::RS_PRIMARY));

        repl::ReplicationCoordinator::set(opCtx->getServiceContext(), std::move(replCoord));
    }

    void tearDown() override {
        ServiceContextMongoDTest::tearDown();

        wiredTigerGlobalOptions.directoryForIndexes = false;
        storageGlobalParams.directoryperdb = false;
    }

    UUID createCollection(OperationContext* opCtx,
                          const NamespaceString& nss,
                          Timestamp timestamp) {
        _setupDDLOperation(opCtx, timestamp);
        WriteUnitOfWork wuow(opCtx);
        UUID uuid = _createCollection(opCtx, nss, boost::none);
        wuow.commit();
        storageEngine->setStableTimestamp(timestamp);
        return uuid;
    }

    void dropCollection(OperationContext* opCtx, const NamespaceString& nss, Timestamp timestamp) {
        _setupDDLOperation(opCtx, timestamp);
        WriteUnitOfWork wuow(opCtx);
        _dropCollection(opCtx, nss, timestamp);
        wuow.commit();
        storageEngine->setStableTimestamp(timestamp);
    }

    void renameCollection(OperationContext* opCtx,
                          const NamespaceString& from,
                          const NamespaceString& to,
                          Timestamp timestamp) {
        invariant(from.db_forTest() == to.db_forTest());
        _setupDDLOperation(opCtx, timestamp);
        WriteUnitOfWork wuow(opCtx);
        _renameCollection(opCtx, from, to, timestamp);
        wuow.commit();
        storageEngine->setStableTimestamp(timestamp);
    }

    void createIndex(OperationContext* opCtx,
                     const NamespaceString& nss,
                     BSONObj indexSpec,
                     Timestamp timestamp) {
        _setupDDLOperation(opCtx, timestamp);
        WriteUnitOfWork wuow(opCtx);
        _createIndex(opCtx, nss, indexSpec);
        wuow.commit();
        storageEngine->setStableTimestamp(timestamp);
    }

    void dropIndex(OperationContext* opCtx,
                   const NamespaceString& nss,
                   const std::string& indexName,
                   Timestamp timestamp) {
        _setupDDLOperation(opCtx, timestamp);
        WriteUnitOfWork wuow(opCtx);
        _dropIndex(opCtx, nss, indexName);
        wuow.commit();
        storageEngine->setStableTimestamp(timestamp);
    }

    BackupCursorState openBackupCursor(StorageEngine::BackupOptions backupOptions = {}) {
        return backupCursorService->openBackupCursor(opCtx.get(), backupOptions);
    }

    std::string getFileName(const std::string& filePath) {
        return boost::filesystem::path(filePath).filename().string();
    }

    boost::optional<BackupBlock> getNext(StorageEngine::StreamingCursor& cursor) {
        auto swBatch = cursor.getNextBatch(/*kBatchSize=*/1);
        if (!swBatch.isOK()) {
            return boost::none;
        }
        auto& batch = swBatch.getValue();
        if (batch.size() == 0) {
            // EOF case.
            return boost::none;
        }
        ASSERT(batch.size() == 1);
        return batch.front();
    };

    // Verify the format of the backup cursor fields.
    void validateBackupCursorFields(const BackupBlock& doc) {
        auto filename = getFileName(doc.filePath());
        auto ns = doc.ns();
        auto uuid = doc.uuid();
        auto required = doc.isRequired();

        // Verify that the correct WiredTiger files are marked as required.
        if (filename == "WiredTiger" || filename == "WiredTiger.backup" ||
            filename == "WiredTigerHS.wt" || filename.starts_with("WiredTigerLog.")) {
            ASSERT(!ns);
            ASSERT(required);
            ASSERT(!uuid);  // Empty
            return;
        }

        // Verify that the correct MongoDB files are marked as required.
        if (filename == "_mdb_catalog.wt" || filename == "sizeStorer.wt") {
            ASSERT(!ns);    // Empty
            ASSERT(!uuid);  // Empty
            return;
        }

        ASSERT(ns);

        std::string nss = ns->toStringForErrorMsg();

        ASSERT(filename.starts_with("collection-") || filename.starts_with("index-"));
        ASSERT(!required);
        ASSERT_EQ(kNss, ns);
        ASSERT(uuid);
    }

    // Verify the format of fields in the document.
    void validateDocumentFields(StorageEngine::StreamingCursor& cursor,
                                mongo::BackupCursorState&& state,
                                bool incremental) {
        size_t numSeen = 0;

        // Assert the metadata file exists and check the validity of the format.
        ASSERT(state.preamble);
        auto metadataFileBSON = state.preamble->toBson();
        auto metadataBSON = metadataFileBSON.getObjectField("metadata");
        ASSERT_EQ(metadataBSON.getBoolField("disableIncrementalBackup"), false);
        ASSERT_EQ(metadataBSON.getBoolField("incrementalBackup"), incremental);
        ASSERT_EQ(metadataBSON.getIntField("blockSize"), 16);

        if (incremental) {
            ASSERT(metadataBSON.hasField("thisBackupName"));
            ASSERT(metadataBSON.hasField("srcBackupName"));
        } else {
            ASSERT(!metadataBSON.hasField("thisBackupName"));
            ASSERT(!metadataBSON.hasField("srcBackupName"));
        }

        // Verify valid format of namespace and UUID fields for each doc in the file.
        while (auto doc = getNext(cursor)) {
            numSeen++;
            ASSERT(doc);
            validateBackupCursorFields(doc.value());
        }
        ASSERT(numSeen > 0);
    }

private:
    void _setupDDLOperation(OperationContext* opCtx, Timestamp timestamp) {
        RecoveryUnit* recoveryUnit = shard_role_details::getRecoveryUnit(opCtx);

        recoveryUnit->setTimestampReadSource(RecoveryUnit::ReadSource::kNoTimestamp);
        recoveryUnit->abandonSnapshot();

        if (!recoveryUnit->getCommitTimestamp().isNull()) {
            recoveryUnit->clearCommitTimestamp();
        }
        recoveryUnit->setCommitTimestamp(timestamp);
    }

    UUID _createCollection(OperationContext* opCtx,
                           const NamespaceString& nss,
                           boost::optional<UUID> uuid = boost::none) {
        AutoGetDb databaseWriteGuard(opCtx, nss.dbName(), MODE_IX);
        auto db = databaseWriteGuard.ensureDbExists(opCtx);
        ASSERT(db);

        Lock::CollectionLock lk(opCtx, nss, MODE_IX);

        CollectionOptions options;
        if (uuid) {
            options.uuid.emplace(*uuid);
        } else {
            options.uuid.emplace(UUID::gen());
        }

        // Adds the collection to the durable catalog.
        auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
        std::pair<RecordId, std::unique_ptr<RecordStore>> catalogIdRecordStorePair =
            uassertStatusOK(storageEngine->getCatalog()->createCollection(
                opCtx, nss, options, /*allocateDefaultSpace=*/true));
        auto& catalogId = catalogIdRecordStorePair.first;
        auto catalogEntry = DurableCatalog::get(opCtx)->getParsedCatalogEntry(opCtx, catalogId);
        auto metadata = catalogEntry->metadata;
        std::shared_ptr<Collection> ownedCollection = Collection::Factory::get(opCtx)->make(
            opCtx, nss, catalogId, metadata, std::move(catalogIdRecordStorePair.second));
        ownedCollection->init(opCtx);
        invariant(ownedCollection->getSharedDecorations());

        // Adds the collection to the in-memory catalog.
        CollectionCatalog::get(opCtx)->onCreateCollection(opCtx, std::move(ownedCollection));
        return *options.uuid;
    }

    void _dropCollection(OperationContext* opCtx, const NamespaceString& nss, Timestamp timestamp) {
        Lock::DBLock dbLk(opCtx, nss.dbName(), MODE_IX);
        Lock::CollectionLock collLk(opCtx, nss, MODE_X);
        CollectionWriter collection(opCtx, nss);

        Collection* writableCollection = collection.getWritableCollection(opCtx);

        // Drop all remaining indexes before dropping the collection.
        std::vector<std::string> indexNames;
        writableCollection->getAllIndexes(&indexNames);
        for (const auto& indexName : indexNames) {
            IndexCatalog* indexCatalog = writableCollection->getIndexCatalog();
            auto writableEntry = indexCatalog->getWritableEntryByName(
                opCtx, indexName, IndexCatalog::InclusionPolicy::kReady);

            // This also adds the index ident to the drop-pending reaper.
            ASSERT_OK(indexCatalog->dropIndexEntry(opCtx, writableCollection, writableEntry));
        }

        // Add the collection ident to the drop-pending reaper.
        opCtx->getServiceContext()->getStorageEngine()->addDropPendingIdent(
            timestamp, collection->getRecordStore()->getSharedIdent());

        // Drops the collection from the durable catalog.
        auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
        uassertStatusOK(
            storageEngine->getCatalog()->dropCollection(opCtx, writableCollection->getCatalogId()));

        // Drops the collection from the in-memory catalog.
        CollectionCatalog::get(opCtx)->dropCollection(
            opCtx, writableCollection, /*isDropPending=*/true);
    }

    void _renameCollection(OperationContext* opCtx,
                           const NamespaceString& from,
                           const NamespaceString& to,
                           Timestamp timestamp) {
        Lock::DBLock dbLk(opCtx, from.dbName(), MODE_IX);
        Lock::CollectionLock fromLk(opCtx, from, MODE_X);
        Lock::CollectionLock toLk(opCtx, to, MODE_X);

        // Drop the collection if it exists. This triggers the same behavior as renaming with
        // dropTarget=true.

        if (CollectionCatalog::get(opCtx)->lookupCollectionByNamespace(opCtx, to)) {
            _dropCollection(opCtx, to, timestamp);
        }
        CollectionWriter collection(opCtx, from);
        ASSERT_OK(collection.getWritableCollection(opCtx)->rename(opCtx, to, false));
        CollectionCatalog::get(opCtx)->onCollectionRename(
            opCtx, collection.getWritableCollection(opCtx), from);
    }

    void _createIndex(OperationContext* opCtx, const NamespaceString& nss, BSONObj indexSpec) {
        AutoGetCollection autoColl(opCtx, nss, MODE_X);
        CollectionWriter collection(opCtx, nss);
        IndexBuildsCoordinator::get(opCtx)->createIndexesOnEmptyCollection(
            opCtx, collection, {indexSpec}, /*fromMigrate=*/false);
    }

    void _dropIndex(OperationContext* opCtx,
                    const NamespaceString& nss,
                    const std::string& indexName) {
        AutoGetCollection autoColl(opCtx, nss, MODE_X);
        CollectionWriter collection(opCtx, nss);
        Collection* writableCollection = collection.getWritableCollection(opCtx);

        IndexCatalog* indexCatalog = writableCollection->getIndexCatalog();
        auto writableEntry = indexCatalog->getWritableEntryByName(
            opCtx, indexName, IndexCatalog::InclusionPolicy::kReady);

        // This also adds the index ident to the drop-pending reaper.
        ASSERT_OK(indexCatalog->dropIndexEntry(opCtx, writableCollection, writableEntry));
    }

protected:
    ServiceContext::UniqueOperationContext opCtx;
    std::unique_ptr<BackupCursorService> backupCursorService;
    StorageEngine* storageEngine;
    NamespaceString nss;
};

TEST_F(BackupCursorServiceTest, OpenBackupCursor) {
    const Timestamp createCollectionTs = Timestamp(10, 10);
    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();
    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();
    size_t numSeen = 0;

    while (auto backupBlock = getNext(*cursor)) {
        LOGV2(9355401,
              "Backup block",
              "file"_attr = backupBlock->filePath(),
              "ns"_attr = backupBlock->ns());
        if (!backupBlock->ns()) {
            continue;
        }
        numSeen++;
        ASSERT_EQ(backupBlock->ns(), kNss);
    }
    ASSERT_EQ(numSeen, 1);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, NsUUIDCheck) {
    // NS and UUID check on a full backup
    const Timestamp createCollectionTs = Timestamp(10, 10);
    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, NsUUIDCheckDirectoryPerDb) {
    // NS and UUID check on a full backup with directoryperdb = True
    const Timestamp createCollectionTs = Timestamp(10, 10);

    storageGlobalParams.directoryperdb = true;

    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    // Open a backup cursor
    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, NsUUIDCheckWTDirectoryForIndexes) {
    // NS and UUID check on a full backup with directoryForIndexes = True
    const Timestamp createCollectionTs = Timestamp(10, 10);

    wiredTigerGlobalOptions.directoryForIndexes = true;

    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}


TEST_F(BackupCursorServiceTest, NsUUIDCheckDirectories) {
    // NS and UUID check on a full backup with both directoryperdb and directoryForIndexes
    const Timestamp createCollectionTs = Timestamp(10, 10);

    wiredTigerGlobalOptions.directoryForIndexes = true;
    storageGlobalParams.directoryperdb = true;

    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}


TEST_F(BackupCursorServiceTest, FullBackupCursorFormat) {
    // Testing non-incremental backup document format
    const Timestamp createCollectionTs = Timestamp(10, 10);

    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();
    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, IncBackupCursorFormat) {
    // Testing incremental backup document format.
    const Timestamp createCollectionTs = Timestamp(15, 15);
    auto storage = std::make_unique<repl::StorageInterfaceImpl>();

    [[maybe_unused]] auto uuid = createCollection(opCtx.get(), kNss, createCollectionTs);

    auto backupCursorState =
        openBackupCursor({.incrementalBackup = true, .thisBackupName = std::string("a")});
    auto cursor = backupCursorState.streamingCursor.get();
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());

    // Insert documents to create changes which the incremental backup will make us copy.
    shard_role_details::getRecoveryUnit(opCtx.get())->clearCommitTimestamp();
    for (auto i = 15; i < 30; i++) {
        ASSERT_OK(
            storage->insertDocument(opCtx.get(), kNss, {BSON("_id" << i), Timestamp(20, 20)}, 0));
    }

    storageEngine->checkpoint();

    // Check the validity of incremental backup fields.
    backupCursorState = openBackupCursor({.incrementalBackup = true,
                                          .thisBackupName = std::string("b"),
                                          .srcBackupName = std::string("a")});
    cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/true);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

}  // namespace mongo
