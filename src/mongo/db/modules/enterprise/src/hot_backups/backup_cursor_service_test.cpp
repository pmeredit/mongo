/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem.hpp>
#include <boost/optional/optional.hpp>
#include <cstddef>
#include <random>
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
#include "mongo/db/timeseries/timeseries_collmod.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/md5.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace mongo {

namespace {

const Timestamp createCollectionTs = Timestamp(10, 10);

const NamespaceString kAdmin =
    NamespaceString::createNamespaceString_forTest("admin", "collection");
const NamespaceString kConfig =
    NamespaceString::createNamespaceString_forTest("config", "collection");
const NamespaceString kLocal =
    NamespaceString::createNamespaceString_forTest("local", "collection");
const NamespaceString kNss = NamespaceString::createNamespaceString_forTest("db", "collection");
const NamespaceString kTs =
    NamespaceString::createNamespaceString_forTest("timeseries-test", "collection");

std::string md5sumFile(const std::string& filename) {
    std::stringstream ss;
    FILE* f = fopen(filename.c_str(), "rb");
    uassert(9705700, str::stream() << "couldn't open file " << filename, f);
    ON_BLOCK_EXIT([&] { fclose(f); });

    md5digest d;
    md5_state_t st;
    md5_init_state(&st);

    enum { BUFLEN = 4 * 1024 };
    char buffer[BUFLEN];
    int bytes_read;
    while ((bytes_read = fread(buffer, 1, BUFLEN, f))) {
        md5_append(&st, (const md5_byte_t*)(buffer), bytes_read);
    }

    md5_finish(&st, d);
    return digestToString(d);
}

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
                          Timestamp timestamp,
                          const CollectionOptions& options = CollectionOptions()) {
        _setupDDLOperation(opCtx, timestamp);
        WriteUnitOfWork wuow(opCtx);
        UUID uuid = _createCollection(opCtx, nss, options);
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

    void populateData(OperationContext* opCtx,
                      const NamespaceString& nss,
                      repl::StorageInterfaceImpl& storage,
                      int iteration) {
        constexpr int kMinLen = 0.25 * 1024 * 1024;  // 0.25MB.
        constexpr int kMaxLen = 1 * 1024 * 1024;     // 1MB.

        auto generateData = [&kMinLen, &kMaxLen]() {
            std::random_device rd;
            unsigned int num = rd();
            int len = num % (kMaxLen - kMinLen) + kMinLen;
            std::vector<int> buf(len);

            for (int i = 0; i < len; ++i) {
                buf[i] = num % 256;
            }
            return buf;
        };
        for (auto i = 0; i < 25; i++) {
            ASSERT_OK(storage.insertDocument(
                opCtx,
                nss,
                {BSON("_id" << (i * iteration) << "x" << generateData()), Timestamp(20, 20)},
                0));
        }
    }

    BackupCursorState openBackupCursor(StorageEngine::BackupOptions backupOptions = {}) {
        return backupCursorService->openBackupCursor(opCtx.get(), backupOptions);
    }

    BackupCursorExtendState extendBackupCursor(const UUID& backupId, const Timestamp& extendTo) {
        return {backupCursorService->extendBackupCursor(opCtx.get(), backupId, extendTo)};
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

    /**
     *  Verify the format of the backup cursor fields.
     */
    void validateBackupCursorFields(const BackupBlock& doc) {
        auto filename = getFileName(doc.filePath());
        auto ns = doc.ns();
        auto uuid = doc.uuid();
        auto required = doc.isRequired();

        // If a file is unchanged in a subsequent incremental backup, a single block's offset
        // and length will remain at zero.
        if (doc.length() != 0 && doc.offset() != 0) {

            // If the fileSize is the same as the length, then WT is asking us to take a full
            // backup of this file.
            if (doc.fileSize() != doc.length()) {

                // The length must be strictly less than or equal to 16MB, which was the
                // specified block size during the full backup.
                ASSERT_LTE(doc.length(), 16 * 1024 * 1024);
                ASSERT_GTE(doc.offset(), 0);
            } else {
                ASSERT_EQ(doc.offset(), 0);
            }
        }

        // Verify that the correct WiredTiger and MongoDB files are marked as required.
        if (filename == "WiredTiger" || filename == "WiredTiger.backup" ||
            filename == "WiredTigerHS.wt" || filename.starts_with("WiredTigerLog.") ||
            filename == "_mdb_catalog.wt" || filename == "sizeStorer.wt") {
            ASSERT(required);
            ASSERT(!ns);    // Empty
            ASSERT(!uuid);  // Empty
            return;
        }

        // Denylisting internal files that don't need to have ns/uuid set. Denylisting known
        // patterns will help catch subtle API changes if new filename patterns are added that
        // don't generate ns/uuid.
        if (required &&
            (!filename.starts_with("Wired") && !filename.starts_with("_") &&
             !filename.starts_with("size"))) {
            ASSERT(ns);
            ASSERT(uuid);
        }

        std::string nss = ns->toString_forTest();

        if (filename.starts_with("collection-") || filename.starts_with("index-")) {
            ASSERT(ns);
            ASSERT(uuid);

            // Verify time-series collection has internal "system.buckets." string removed
            if (nss.find("timeseries-test") != std::string::npos) {
                ASSERT(!ns->isTimeseriesBucketsCollection());
                ASSERT(!required);
                return;
            } else if (nss.starts_with("local") || nss.starts_with("admin") ||
                       nss.starts_with("config") ||
                       nss.find("test.system.views") != std::string::npos) {

                // Verify that internal database and system.views files are marked required.
                ASSERT(required);
                return;
            }
        }

        // Everything else should not be marked as required.
        ASSERT(!required);
        ASSERT(uuid);
        ASSERT(ns);
    }

    /**
     * Verify the format of fields in the document.
     */
    void validateDocumentFields(StorageEngine::StreamingCursor& cursor,
                                mongo::BackupCursorState&& state,
                                bool incremental) {
        size_t numSeen = 0;

        // Assert the metadata file exists and check the validity of the format.
        ASSERT(state.preamble);
        auto metadataFileBSON = state.preamble->toBson();
        auto metadataBSON = metadataFileBSON.getObjectField("metadata");
        ASSERT_EQ(metadataBSON.getBoolField("disableIncrementalBackup"), false);
        ASSERT_EQ(metadataBSON.getIntField("blockSize"), 16);

        // These fields only exist if we are doing incremental backup.
        ASSERT_EQ(metadataBSON.getBoolField("incrementalBackup"), incremental);
        ASSERT_EQ(metadataBSON.hasField("thisBackupName"), incremental);

        ASSERT(!metadataBSON.hasField("filename"));
        ASSERT(!metadataBSON.hasField("fileSize"));
        ASSERT(!metadataBSON.hasField("offset"));
        ASSERT(!metadataBSON.hasField("length"));

        // Verify valid format of namespace and UUID fields for each doc in the file.
        while (auto doc = getNext(cursor)) {
            numSeen++;
            ASSERT(doc);
            validateBackupCursorFields(doc.value());
        }
        ASSERT(numSeen > 0);
    }

    /**
     * Expect to read a new log file from 'extendBackupCursor' calls and old backup log files to
     * remain unchanged. After extending the backupCursor, we will have an extra log file. We save
     * the log file in 'backupLogFileCheckSums' along with the checkSum of the file to ensure that
     * future calls to extendBackupCursor leave the logs unchanged.
     */
    void assertBackupCursorExtends(UUID backupId,
                                   Timestamp extendTo,
                                   std::map<std::string, std::string>& backupLogFileCheckSums) {
        auto backupCursorExtendState = extendBackupCursor(backupId, extendTo);
        const auto extendedFiles = backupCursorExtendState.filePaths;

        size_t newLogFileCount = 0;
        for (const auto& extendFile : extendedFiles) {
            if (backupLogFileCheckSums.find(extendFile) == backupLogFileCheckSums.end()) {
                const auto sumFile = md5sumFile(extendFile);
                backupLogFileCheckSums[extendFile] = sumFile;
                newLogFileCount += 1;
            } else {
                ASSERT_EQ(backupLogFileCheckSums[extendFile], md5sumFile(extendFile));
            }
        }
        ASSERT(newLogFileCount > 0);
    }

    /**
     *  Verify the backup files are in ascending order according to their offset.
     */
    void validateDocumentOrder(StorageEngine::StreamingCursor& cursor) {
        std::set<std::string> seenFiles;
        std::string lastSeenFile;
        auto lastSeenOffset = 0;

        while (auto doc = getNext(cursor)) {
            auto filename = getFileName(doc.value().filePath());

            if (lastSeenFile.empty()) {
                // First file.
                lastSeenFile = filename;
                lastSeenOffset = doc.value().offset();
            } else if (lastSeenFile != filename) {
                // New file.
                seenFiles.emplace(lastSeenFile);
                lastSeenFile = filename;
                lastSeenOffset = doc.value().offset();
            } else {
                // Same file. Checks blocks within a batch are ordered properly and that the
                // offset is ascending.
                ASSERT_GT(doc.value().offset(), lastSeenOffset);
                lastSeenOffset = doc.value().offset();
            }

            // If we're copying a new file, check we haven't copied the file before. If we have
            // copied the file before, then the batch pre-fetch has caused blocks of different
            // files to be given out of order. Checks ordering at the boundaries between
            // batches.
            ASSERT_FALSE(seenFiles.contains(filename));
        }
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
                           const CollectionOptions& options) {
        AutoGetDb databaseWriteGuard(opCtx, nss.dbName(), MODE_IX);
        auto db = databaseWriteGuard.ensureDbExists(opCtx);
        ASSERT(db);

        Lock::CollectionLock lk(opCtx, nss, MODE_IX);

        CollectionOptions newOptions = options;
        newOptions.uuid.emplace(UUID::gen());

        // Adds the collection to the durable catalog.
        auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
        std::pair<RecordId, std::unique_ptr<RecordStore>> catalogIdRecordStorePair =
            uassertStatusOK(storageEngine->getCatalog()->createCollection(
                opCtx, nss, newOptions, /*allocateDefaultSpace=*/true));
        auto& catalogId = catalogIdRecordStorePair.first;
        auto catalogEntry = DurableCatalog::get(opCtx)->getParsedCatalogEntry(opCtx, catalogId);
        auto metadata = catalogEntry->metadata;
        std::shared_ptr<Collection> ownedCollection = Collection::Factory::get(opCtx)->make(
            opCtx, nss, catalogId, metadata, std::move(catalogIdRecordStorePair.second));
        ownedCollection->init(opCtx);
        invariant(ownedCollection->getSharedDecorations());

        // Adds the collection to the in-memory catalog.
        CollectionCatalog::get(opCtx)->onCreateCollection(opCtx, std::move(ownedCollection));
        return *newOptions.uuid;
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
    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, NsUUIDCheckDirectoryPerDb) {
    // NS and UUID check on a full backup with directoryperdb = True
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
    createCollection(opCtx.get(), kNss, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();
    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, FullBackupCursorFormatAdmin) {
    // Testing non-incremental backup document format for admin database
    createCollection(opCtx.get(), kAdmin, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, FullBackupCursorFormatConfig) {
    // Testing non-incremental backup document format for config database
    createCollection(opCtx.get(), kConfig, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, FullBackupCursorFormatLocal) {
    // Testing non-incremental backup document format for local database
    createCollection(opCtx.get(), kLocal, createCollectionTs);
    storageEngine->checkpoint();

    auto backupCursorState = openBackupCursor();
    auto cursor = backupCursorState.streamingCursor.get();

    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/false);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, IncBackupCursorFormat) {
    // Testing incremental backup document format.
    auto storage = std::make_unique<repl::StorageInterfaceImpl>();

    [[maybe_unused]] auto uuid = createCollection(opCtx.get(), kNss, createCollectionTs);

    auto backupCursorState =
        openBackupCursor({.incrementalBackup = true, .thisBackupName = std::string("a")});
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
    auto cursor = backupCursorState.streamingCursor.get();
    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/true);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, IncBackupCursorFormatTimeSeries) {
    // Testing incremental backup document format with time-series.
    auto storage = std::make_unique<repl::StorageInterfaceImpl>();

    CollectionOptions options;
    options.timeseries = TimeseriesOptions(/*timeField=*/"time");
    createCollection(opCtx.get(), kTs, createCollectionTs, options);

    auto backupCursorState =
        openBackupCursor({.incrementalBackup = true, .thisBackupName = std::string("a")});
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());

    // Insert documents to create changes which the incremental backup will make us copy.
    shard_role_details::getRecoveryUnit(opCtx.get())->clearCommitTimestamp();
    for (auto i = 25; i < 50; i++) {
        ASSERT_OK(storage->insertDocument(
            opCtx.get(), kTs, {BSON("_id" << i << "time" << Date_t::now()), Timestamp(30, 30)}, 0));
    }

    // Check the validity of incremental backup fields.
    backupCursorState = openBackupCursor({.incrementalBackup = true,
                                          .thisBackupName = std::string("b"),
                                          .srcBackupName = std::string("a")});
    auto cursor = backupCursorState.streamingCursor.get();
    validateDocumentFields(*cursor, std::move(backupCursorState), /*incremental=*/true);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, MultipleBackupCursorExtend) {
    // Testing 'extendBackupCursor' calls result in new log files.
    const Timestamp createCollectionTs = Timestamp(15, 15);
    auto storage = std::make_unique<repl::StorageInterfaceImpl>();

    createCollection(opCtx.get(), kNss, createCollectionTs);
    auto backupCursorState =
        openBackupCursor({.incrementalBackup = true, .thisBackupName = std::string("a")});
    auto backupId = backupCursorState.backupId;

    // We insert new documents and call 'extendBackupCursor' multiple times and expect to read
    // an extra log file from the cursor and leave the older log files unchanged.
    std::map<std::string, std::string> backupLogFileCheckSums = {};
    shard_role_details::getRecoveryUnit(opCtx.get())->clearCommitTimestamp();
    for (auto i = 20; i < 25; i++) {
        ASSERT_OK(
            storage->insertDocument(opCtx.get(), kNss, {BSON("_id" << i), Timestamp(i, i)}, 0));
        assertBackupCursorExtends(backupId, Timestamp(i, i), backupLogFileCheckSums);
    }

    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

TEST_F(BackupCursorServiceTest, BackupCursorOutOfOrder) {
    // Testing backup blocks are returned in the same ascending file offset order as WT returns
    // them.
    auto storage = std::make_unique<repl::StorageInterfaceImpl>();
    createCollection(opCtx.get(), kNss, createCollectionTs);

    // Add initial data and checkpoint
    size_t iteration = 1;
    shard_role_details::getRecoveryUnit(opCtx.get())->clearCommitTimestamp();
    populateData(opCtx.get(), kNss, *storage, iteration);
    storageEngine->checkpoint();

    // Backup
    auto backupCursorState = openBackupCursor({
        .incrementalBackup = true,
        .blockSizeMB = 1,
        .thisBackupName = std::string("a"),
    });

    // Add more data and checkpoint
    populateData(opCtx.get(), kNss, *storage, ++iteration);
    storageEngine->checkpoint();
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());

    // Backup again
    backupCursorState = openBackupCursor({
        .incrementalBackup = true,
        .blockSizeMB = 1,
        .thisBackupName = std::string("b"),
        .srcBackupName = std::string("a"),
    });

    // Validate backup files are in ascending offset order.
    auto cursor = backupCursorState.streamingCursor.get();
    validateDocumentOrder(*cursor);
    backupCursorService->closeBackupCursor(opCtx.get(), backupCursorService->getBackupId());
}

}  // namespace mongo
