/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "fcbis/initial_sync_file_mover.h"
#include "file_copy_based_initial_syncer.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/json.h"
#include "mongo/db/client.h"
#include "mongo/db/index_builds_coordinator_mongod.h"
#include "mongo/db/query/client_cursor/cursor_manager.h"
#include "mongo/db/query/client_cursor/cursor_response.h"
#include "mongo/db/repl/data_replicator_external_state_mock.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_consistency_markers_mock.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/replication_recovery_mock.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/repl/sync_source_selector.h"
#include "mongo/db/repl/sync_source_selector_mock.h"
#include "mongo/db/repl/task_executor_mock.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/backup_cursor_hooks.h"
#include "mongo/db/storage/devnull/devnull_kv_engine.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/wire_version.h"
#include "mongo/dbtests/mock/mock_dbclient_connection.h"
#include "mongo/dbtests/mock/mock_remote_db_server.h"
#include "mongo/executor/mock_network_fixture.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/executor/thread_pool_task_executor_test_fixture.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/unittest/log_test.h"
#include "mongo/unittest/thread_assertion_monitor.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/scopeguard.h"
#include "mongo/watchdog/watchdog.h"
#include "mongo/watchdog/watchdog_mock.h"
#include <boost/filesystem.hpp>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace mongo {
namespace {

using namespace repl;
using namespace test::mock;

using executor::NetworkInterfaceMock;
using executor::RemoteCommandResponse;
using LockGuard = stdx::lock_guard<stdx::mutex>;

struct CollectionCloneInfo {
    std::shared_ptr<CollectionMockStats> stats = std::make_shared<CollectionMockStats>();
    CollectionBulkLoaderMock* loader = nullptr;
    Status status{ErrorCodes::NotYetInitialized, ""};
};

class FileCopyBasedInitialSyncerTest : public ServiceContextMongoDTest, public SyncSourceSelector {
public:
    FileCopyBasedInitialSyncerTest()
        : ServiceContextMongoDTest(std::make_unique<MongoDScopedGlobalServiceContextForTest>(
              ServiceContext::make(std::make_unique<ClockSourceMock>(),
                                   std::make_unique<ClockSourceMock>()),
              Options{}.engine("devnull"))) {}

    executor::ThreadPoolMock::Options makeThreadPoolMockOptions() const;

    /**
     * clear/reset state
     */
    void reset() {
        _setMyLastOptime = [this](const OpTimeAndWallTime& opTimeAndWallTime) {
            _myLastOpTime = opTimeAndWallTime.opTime;
            _myLastWallTime = opTimeAndWallTime.wallTime;
        };
        _myLastOpTime = OpTime();
        _myLastWallTime = Date_t();
        _syncSourceSelector = std::make_unique<SyncSourceSelectorMock>();
    }

    // SyncSourceSelector
    void clearSyncSourceDenylist() override {
        _syncSourceSelector->clearSyncSourceDenylist();
    }
    HostAndPort chooseNewSyncSource(const OpTime& ot) override {
        return _syncSourceSelector->chooseNewSyncSource(ot);
    }
    void denylistSyncSource(const HostAndPort& host, Date_t until) override {
        _syncSourceSelector->denylistSyncSource(host, until);
    }
    ChangeSyncSourceAction shouldChangeSyncSource(const HostAndPort& currentSource,
                                                  const rpc::ReplSetMetadata& replMetadata,
                                                  const rpc::OplogQueryMetadata& oqMetadata,
                                                  const OpTime& previousOpTimeFetched,
                                                  const OpTime& lastOpTimeFetched) const override {
        return _syncSourceSelector->shouldChangeSyncSource(
            currentSource, replMetadata, oqMetadata, previousOpTimeFetched, lastOpTimeFetched);
    }
    ChangeSyncSourceAction shouldChangeSyncSourceOnError(
        const HostAndPort& currentSource, const OpTime& lastOpTimeFetched) const override {
        return _syncSourceSelector->shouldChangeSyncSourceOnError(currentSource, lastOpTimeFetched);
    }

    FileCopyBasedInitialSyncer* getFileCopyBasedInitialSyncer() {
        return _fileCopyBasedInitialSyncer.get();
    }

    DataReplicatorExternalStateMock* getExternalState() {
        return _externalState;
    }

    StorageInterface& getStorage() {
        return *_storageInterface;
    }

    executor::ThreadPoolTaskExecutor& getExecutor() {
        return *_threadPoolExecutor;
    }

    executor::NetworkInterfaceMock* getNet() {
        return _net;
    }

protected:
    struct StorageInterfaceResults {
        bool createOplogCalled = false;
        bool truncateCalled = false;
        bool insertedOplogEntries = false;
        int oplogEntriesInserted = 0;
        bool droppedUserDBs = false;
        std::vector<std::string> droppedCollections;
        int documentsInsertedCount = 0;
    };

    // protects _storageInterfaceWorkDone.
    stdx::mutex _storageInterfaceWorkDoneMutex;
    StorageInterfaceResults _storageInterfaceWorkDone;

    void setUp() override {
        ServiceContextMongoDTest::setUp();

        auto* service = getGlobalServiceContext();
        WatchdogMonitorInterface::set(service, std::make_unique<WatchdogMonitorMock>());

        auto network = std::make_unique<executor::NetworkInterfaceMock>();
        _net = network.get();
        _threadPoolExecutor =
            makeThreadPoolTestExecutor(std::move(network), makeThreadPoolMockOptions());
        _threadPoolExecutor->startup();

        _storageInterface = std::make_unique<StorageInterfaceMock>();
        _storageInterface->createOplogFn = [this](OperationContext* opCtx,
                                                  const NamespaceString& nss) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            _storageInterfaceWorkDone.createOplogCalled = true;
            return Status::OK();
        };
        _storageInterface->truncateCollFn = [this](OperationContext* opCtx,
                                                   const NamespaceString& nss) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            _storageInterfaceWorkDone.truncateCalled = true;
            return Status::OK();
        };
        _storageInterface->insertDocumentFn = [this](OperationContext* opCtx,
                                                     const NamespaceStringOrUUID& nsOrUUID,
                                                     const TimestampedBSONObj& doc,
                                                     long long term) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            ++_storageInterfaceWorkDone.documentsInsertedCount;
            return Status::OK();
        };
        _storageInterface->insertDocumentsFn = [this](OperationContext* opCtx,
                                                      const NamespaceStringOrUUID& nsOrUUID,
                                                      const std::vector<InsertStatement>& ops) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            _storageInterfaceWorkDone.insertedOplogEntries = true;
            ++_storageInterfaceWorkDone.oplogEntriesInserted;
            return Status::OK();
        };
        _storageInterface->dropCollFn = [this](OperationContext* opCtx,
                                               const NamespaceString& nss) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            _storageInterfaceWorkDone.droppedCollections.push_back(nss.toString_forTest());
            return Status::OK();
        };
        _storageInterface->dropUserDBsFn = [this](OperationContext* opCtx) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            _storageInterfaceWorkDone.droppedUserDBs = true;
            return Status::OK();
        };
        _storageInterface->createCollectionForBulkFn =
            [this](const NamespaceString& nss,
                   const CollectionOptions& options,
                   const BSONObj idIndexSpec,
                   const std::vector<BSONObj>& secondaryIndexSpecs)
            -> StatusWith<std::unique_ptr<CollectionBulkLoaderMock>> {
            // Get collection info from map.
            const auto collInfo = &_collections[nss];
            if (collInfo->stats->initCalled) {
                LOGV2(5781905,
                      "reusing collection during test which may cause problems",
                      "nss"_attr = nss);
            }
            auto localLoader = std::make_unique<CollectionBulkLoaderMock>(collInfo->stats);
            auto status = localLoader->init(secondaryIndexSpecs);
            if (!status.isOK())
                return status;
            collInfo->loader = localLoader.get();

            return std::move(localLoader);
        };
        _storageInterface->putSingletonFn = [this](OperationContext* opCtx,
                                                   const NamespaceString& nss,
                                                   const TimestampedBSONObj& update) {
            LockGuard lock(_storageInterfaceWorkDoneMutex);
            return Status::OK();
        };

        auto cursorManager = CursorManager::get(service);
        cursorManager->setPreciseClockSource(service->getPreciseClockSource());

        repl::StorageInterface::set(service, std::make_unique<repl::StorageInterfaceMock>());
        ReplicationCoordinator::set(service, std::make_unique<ReplicationCoordinatorMock>(service));
        BackupCursorHooks::initialize(service);
        repl::createOplog(cc().makeOperationContext().get());

        ThreadPool::Options dbThreadPoolOptions;
        dbThreadPoolOptions.poolName = "dbthread";
        dbThreadPoolOptions.minThreads = 1U;
        dbThreadPoolOptions.maxThreads = 1U;
        dbThreadPoolOptions.onCreateThread = [](const std::string& threadName) {
            Client::initThread(threadName.c_str(), getGlobalServiceContext()->getService());
        };
        _dbWorkThreadPool = std::make_unique<ThreadPool>(dbThreadPoolOptions);
        _dbWorkThreadPool->startup();

        _target = HostAndPort{"localhost:12346"};
        _mockServer = std::make_unique<MockRemoteDBServer>(_target.toString());
        _mock = std::make_unique<MockNetwork>(getNet());

        reset();

        _replicationProcess = std::make_unique<ReplicationProcess>(
            _storageInterface.get(),
            std::make_unique<ReplicationConsistencyMarkersMock>(),
            std::make_unique<ReplicationRecoveryMock>());

        _executorProxy = std::make_shared<TaskExecutorMock>(&getExecutor());

        _myLastOpTime = OpTime({3, 0}, 1);

        InitialSyncerInterface::Options options;
        options.initialSyncRetryWait = Milliseconds(1);
        options.getMyLastOptime = [this]() { return _myLastOpTime; };
        options.setMyLastOptime = [this](const OpTimeAndWallTime& opTimeAndWallTime) {
            _setMyLastOptime(opTimeAndWallTime);
        };
        options.resetOptimes = [this]() { _myLastOpTime = OpTime(); };
        options.syncSourceSelector = this;

        _options = options;

        auto dataReplicatorExternalState = std::make_unique<DataReplicatorExternalStateMock>();
        dataReplicatorExternalState->taskExecutor = _executorProxy;
        dataReplicatorExternalState->setCurrentTerm(1LL);
        dataReplicatorExternalState->setLastCommittedOpTime(_myLastOpTime);
        {
            ReplSetConfig config(
                ReplSetConfig::parse(BSON("_id"
                                          << "myset"
                                          << "version" << 1 << "protocolVersion" << 1 << "members"
                                          << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                                   << "localhost:12345"))
                                          << "settings"
                                          << BSON("electionTimeoutMillis" << 10000))));
            dataReplicatorExternalState->setReplSetConfigResult(config);
        }
        _externalState = dataReplicatorExternalState.get();

        storageGlobalParams.engine = "wiredTiger";
        _lastApplied = {ErrorCodes::InternalError, "Initial test error status"};
        _onCompletion = [this](const StatusWith<OpTimeAndWallTime>& lastApplied) {
            _lastApplied = lastApplied;
        };

        try {
            _fileCopyBasedInitialSyncer = std::make_shared<FileCopyBasedInitialSyncer>(
                options,
                std::move(dataReplicatorExternalState),
                _dbWorkThreadPool.get(),
                _storageInterface.get(),
                _replicationProcess.get(),
                [this](const StatusWith<OpTimeAndWallTime>& lastApplied) {
                    _onCompletion(lastApplied);
                });
            _fileCopyBasedInitialSyncer->setCreateClientFn_forTest([this]() {
                return std::unique_ptr<DBClientConnection>(
                    new MockDBClientConnection(_mockServer.get()));
            });
            globalFailPointRegistry()
                .find("fCBISSkipSyncingFilesPhase")
                ->setMode(FailPoint::alwaysOn);
            globalFailPointRegistry()
                .find("fCBISSkipMovingFilesPhase")
                ->setMode(FailPoint::alwaysOn);
            globalFailPointRegistry()
                .find("fCBISSkipSwitchingStorage")
                ->setMode(FailPoint::alwaysOn);
            globalFailPointRegistry()
                .find("fCBISSkipUpdatingLastApplied")
                ->setMode(FailPoint::alwaysOn);
            globalFailPointRegistry()
                .find("fCBISAllowGettingProgressAfterInitialSyncCompletes")
                ->setMode(FailPoint::alwaysOn);
        } catch (...) {
            ASSERT_OK(exceptionToStatus());
        }

        cursorDataMock.dbpath = boost::filesystem::path(storageGlobalParams.dbpath);
        cursorDataMock.currentFileMover =
            std::make_unique<InitialSyncFileMover>(cursorDataMock.dbpath.string());
    }

    void tearDownExecutorThread() {
        if (_executorThreadShutdownComplete) {
            return;
        }
        getExecutor().shutdown();
        _mock->runUntilIdle();
        getExecutor().join();
        _executorThreadShutdownComplete = true;
    }

    void tearDown() override {
        _fileCopyBasedInitialSyncer.reset();
        tearDownExecutorThread();
        _dbWorkThreadPool.reset();
        _replicationProcess.reset();
        _storageInterface.reset();
        _mock.reset();
    }

    std::shared_ptr<TaskExecutorMock> _executorProxy;

    InitialSyncerInterface::Options _options;
    InitialSyncerInterface::Options::SetMyLastOptimeFn _setMyLastOptime;
    OpTime _myLastOpTime;
    Date_t _myLastWallTime;
    std::unique_ptr<SyncSourceSelectorMock> _syncSourceSelector;
    std::unique_ptr<StorageInterfaceMock> _storageInterface;
    HostAndPort _target;
    std::unique_ptr<MockRemoteDBServer> _mockServer;
    std::unique_ptr<MockNetwork> _mock;
    std::unique_ptr<ReplicationProcess> _replicationProcess;
    std::unique_ptr<ThreadPool> _dbWorkThreadPool;
    std::map<NamespaceString, CollectionCloneInfo> _collections;

    executor::NetworkInterfaceMock* _net = nullptr;
    std::shared_ptr<executor::ThreadPoolTaskExecutor> _threadPoolExecutor;

    StatusWith<OpTimeAndWallTime> _lastApplied = Status(ErrorCodes::NotYetInitialized, "");
    InitialSyncerInterface::OnCompletionFn _onCompletion;

    struct cursorDataMock {
        const UUID backupId =
            UUID(uassertStatusOK(UUID::parse(("2b068e03-5961-4d8e-b47a-d1c8cbd4b835"))));
        const Timestamp checkpointTimestamp = Timestamp(1, 0);
        const CursorId backupCursorId = 3703253128214665235ll;
        const NamespaceString nss =
            NamespaceString::createNamespaceString_forTest("admin.$cmd.aggregate");
        const std::vector<std::vector<std::string>> backupCursorFiles{
            {"/data/db/job0/mongorunner/test-1/WiredTiger",
             "/data/db/job0/mongorunner/test-1/WiredTiger.backup",
             "/data/db/job0/mongorunner/test-1/sizeStorer.wt",
             "/data/db/job0/mongorunner/test-1/index-1--3853645825680686061.wt",
             "/data/db/job0/mongorunner/test-1/collection-0--3853645825680686061.wt",
             "/data/db/job0/mongorunner/test-1/_mdb_catalog.wt",
             "/data/db/job0/mongorunner/test-1/WiredTigerHS.wt",
             "/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000001"},
            {"/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000002",
             "/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000003"}};
        const std::vector<std::vector<int>> backupCursorFileSizes{
            {47, 77050, 20480, 20480, 20480, 36864, 4096, 104857600}, {104857600, 104857600}};

        const std::vector<std::vector<std::string>> extendedCursorFiles{
            {"/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000004",
             "/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000005"},
            {"/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000006",
             "/data/db/job0/mongorunner/test-1/journal/WiredTigerLog.0000000007"}};

        const std::vector<std::string> moveMarkerFilesList{"WiredTiger",
                                                           "WiredTiger.backup",
                                                           "WiredTigerHS.wt",
                                                           "_mdb_catalog.wt",
                                                           "collection-0--3853645825680686061.wt",
                                                           "index-1--3853645825680686061.wt",
                                                           "journal",
                                                           "sizeStorer.wt"};

        const std::vector<std::string> deleteMarkerFilesList{"WiredTiger",
                                                             "WiredTiger.backup",
                                                             "WiredTigerHS.wt",
                                                             "_mdb_catalog.wt",
                                                             "collection-0--3853645825680686061.wt",
                                                             "index-1--3853645825680686061.wt",
                                                             "journal/WiredTigerLog.0000000001",
                                                             "journal/WiredTigerLog.0000000002",
                                                             "journal/WiredTigerLog.0000000003",
                                                             "journal/WiredTigerLog.0000000004",
                                                             "journal/WiredTigerLog.0000000005",
                                                             "journal/WiredTigerLog.0000000006",
                                                             "journal/WiredTigerLog.0000000007",
                                                             "sizeStorer.wt"};

        StringData remoteDbPath = "/data/db/job0/mongorunner/test-1";

        boost::filesystem::path dbpath;
        std::unique_ptr<InitialSyncFileMover> currentFileMover;

        BSONObj getBackupCursorBatches(int batchId) {
            // Empty last batch.
            if (batchId == -1) {
                return BSON("cursor" << BSON("nextBatch" << BSONArray() << "id" << backupCursorId
                                                         << "ns" << nss.ns_forTest())
                                     << "ok" << 1.0);
            }

            BSONObjBuilder cursor;
            BSONArrayBuilder batch(
                cursor.subarrayStart((batchId == 0) ? "firstBatch" : "nextBatch"));

            // First batch.
            if (batchId == 0) {
                auto metaData = BSON("backupId" << backupId << "checkpointTimestamp"
                                                << checkpointTimestamp << "dbpath" << remoteDbPath);
                batch.append(BSON("metadata" << metaData));
            }
            for (int i = 0; i < int(backupCursorFiles[batchId].size()); i++) {
                batch.append(BSON("filename" << backupCursorFiles[batchId][i] << "fileSize"
                                             << backupCursorFileSizes[batchId][i]));
            }
            batch.done();
            cursor.append("id", backupCursorId);
            cursor.append("ns", nss.ns_forTest());
            BSONObjBuilder backupCursorReply;
            backupCursorReply.append("cursor", cursor.obj());
            backupCursorReply.append("ok", 1.0);
            return backupCursorReply.obj();
        }

        BSONObj getExtendedCursorBatches(int cursorIndex) {
            BSONObjBuilder cursor;
            BSONArrayBuilder batch(cursor.subarrayStart("firstBatch"));
            for (const auto& filename : extendedCursorFiles[cursorIndex]) {
                batch.append(BSON("filename" << filename));
            }
            batch.done();
            cursor.append("id", 0ll);
            cursor.append("ns", nss.ns_forTest());
            BSONObjBuilder extendedCursorReply;
            extendedCursorReply.append("cursor", cursor.obj());
            extendedCursorReply.append("ok", 1.0);
            return extendedCursorReply.obj();
        }

        std::vector<std::string> getAllRemoteFilesRelativePath() {
            std::vector<std::string> allFiles;
            for (int i = 0; i < 2; i++) {
                for (const auto& file : backupCursorFiles[i]) {
                    allFiles.push_back(
                        FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(file, remoteDbPath));
                }

                for (const auto& file : extendedCursorFiles[i]) {
                    allFiles.push_back(
                        FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(file, remoteDbPath));
                }
            }

            return allFiles;
        }

        void writeFile(StringData fileRelativePath,
                       StringData contents,
                       bool insideInitialSyncDir) {
            boost::filesystem::path filepath(dbpath);
            if (insideInitialSyncDir) {
                filepath.append(InitialSyncFileMover::kInitialSyncDir.toString());
            }
            filepath.append(fileRelativePath.toString());
            boost::filesystem::create_directories(filepath.parent_path());
            std::ofstream writer(filepath.native());
            writer << contents;
        }

        void createStorageFiles(bool insideInitialSyncDir = false) {
            auto files = getAllRemoteFilesRelativePath();
            for (const auto& fileRelativePath : files) {
                writeFile(
                    fileRelativePath, "_DATA_" + fileRelativePath + "_DATA_", insideInitialSyncDir);
            }
        }

        void createInitialSyncDummyDirectory() {
            boost::filesystem::path filepath(dbpath);
            filepath.append(InitialSyncFileMover::kInitialSyncDir.toString());
            filepath.append(InitialSyncFileMover::kInitialSyncDummyDir.toString());
            filepath.append("dummyFile");
            boost::filesystem::create_directories(filepath.parent_path());
            std::ofstream writer(filepath.native());
            writer << "DummyFile_DATA";
        }

        void validateInitialSyncDummyDirectoryExistence(bool shouldExist) {
            boost::filesystem::path fileRelativePath(
                InitialSyncFileMover::kInitialSyncDummyDir.toString());
            fileRelativePath.append("dummyFile");
            if (!shouldExist) {
                assertNotExist(fileRelativePath.string(), true /* insideInitialSyncDir */);
            } else {
                assertExistsWithContents(
                    fileRelativePath.string(), "DummyFile_DATA", true /* insideInitialSyncDir */);
            }
        }

        void assertExistsWithContents(StringData fileRelativePath,
                                      StringData expected_contents,
                                      bool insideInitialSyncDir) {
            auto path = dbpath;
            if (insideInitialSyncDir) {
                path.append(InitialSyncFileMover::kInitialSyncDir.toString());
            }

            path.append(fileRelativePath.toString());
            ASSERT_TRUE(boost::filesystem::exists(path))
                << "File " << fileRelativePath << " should exist but does not";
            std::ifstream reader(path.native());
            std::string contents;
            reader >> contents;
            ASSERT_EQ(contents, expected_contents)
                << "File " << fileRelativePath << " should have contents " << expected_contents
                << " but instead has " << contents;
        }

        void assertNotExist(StringData fileRelativePath, bool insideInitialSyncDir) {
            auto path = dbpath;
            if (insideInitialSyncDir) {
                path.append(InitialSyncFileMover::kInitialSyncDir.toString());
            }
            path.append(fileRelativePath.toString());
            ASSERT_FALSE(boost::filesystem::exists(path))
                << "File " << fileRelativePath << " shouldn't exist but does";
        }

        void validateFilesExistence(bool shouldExist, bool insideInitialSyncDir = false) {
            auto files = getAllRemoteFilesRelativePath();
            for (const auto& fileRelativePath : files) {
                if (!shouldExist) {
                    assertNotExist(fileRelativePath, insideInitialSyncDir);
                } else {
                    assertExistsWithContents(fileRelativePath,
                                             "_DATA_" + fileRelativePath + "_DATA_",
                                             insideInitialSyncDir);
                }
            }
        }

        void validateMarkerExistence(bool shouldExist, StringData markerName) {
            if (!shouldExist) {
                assertNotExist(markerName, false /*insideInitialSyncDir*/);
            } else {
                auto filesInMarker = currentFileMover->readListOfFiles(markerName);
                sort(filesInMarker.begin(), filesInMarker.end());
                if (markerName == InitialSyncFileMover::kMovingFilesMarker) {
                    ASSERT_TRUE(moveMarkerFilesList == filesInMarker);
                } else {
                    ASSERT_TRUE(deleteMarkerFilesList == filesInMarker);
                }
            }
        }

        void validateInitialSyncDirExistence(bool shouldExist) {
            auto path = dbpath;
            path.append(InitialSyncFileMover::kInitialSyncDir.toString());
            ASSERT_TRUE(boost::filesystem::exists(path) == shouldExist);
        }

    } cursorDataMock;

    bool verifyCursorFiles(const StringSet& returnedFiles,
                           const std::vector<std::vector<std::string>>& cursorFiles) {
        int numOfFiles = 0;
        for (int batchId = 0; batchId < int(cursorFiles.size()); batchId++) {
            for (const auto& file : cursorFiles[batchId]) {
                numOfFiles++;
                if (!returnedFiles.contains(file)) {
                    return false;
                }
            }
        }

        return numOfFiles == (int)returnedFiles.size();
    }

    bool verifyCursorFiles(
        const FileCopyBasedInitialSyncer::BackupFileMetadataCollection returnedFiles,
        const std::vector<std::vector<std::string>>& cursorFiles) {
        StringSet returnedFilenames;
        std::for_each(returnedFiles.begin(),
                      returnedFiles.end(),
                      [&returnedFilenames](const BSONObj& metadata) {
                          returnedFilenames.insert(metadata["filename"].str());
                      });
        return verifyCursorFiles(returnedFilenames, cursorFiles);
    }

    // Helper functions to test sending remote commands with the expected response.
    void expectHelloCommandWithResponse(bool isWritablePrimary,
                                        bool secondary,
                                        int maxWireVersion) {
        BSONObjBuilder helloResp;
        helloResp.append("isWritablePrimary", isWritablePrimary);
        helloResp.append("secondary", secondary);
        helloResp.append("maxWireVersion", maxWireVersion);
        helloResp.append("ok", 1);
        _mock
            ->expect(BSON("hello" << 1),
                     RemoteCommandResponse::make_forTest(helloResp.obj(), Milliseconds()))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulHelloCommand() {
        expectHelloCommandWithResponse(true, false, static_cast<int>(WireVersion::WIRE_VERSION_51));
    }

    void expectGetParameterCommandWithResponse(bool directoryperdb, bool directoryForIndexes) {
        BSONObjBuilder getParameterResp;
        getParameterResp.append("storageGlobalParams.directoryperdb", directoryperdb);
        getParameterResp.append("wiredTigerDirectoryForIndexes", directoryForIndexes);
        getParameterResp.append("ok", 1);

        // MockNetwork converts commands from BSON format to a MatchExpression, which doesn't handle
        // '.' in field names, so we manually write the command as a MatchExpression with the
        // $getField operator, which is able to handle '.' in field names.
        BSONObj getParameterReq = fromjson(
            "{$and:[{ getParameter: { $eq: 1 } },{$expr: { $eq: [{$getField: "
            "\"wiredTigerDirectoryForIndexes\"}, 1]}},{$expr: {$eq:[{$getField: "
            "\"storageGlobalParams.directoryperdb\"}, 1]}}]}");
        _mock->expect(getParameterReq,
                      RemoteCommandResponse::make_forTest(getParameterResp.obj(), Milliseconds()));
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulGetParameterCommand() {
        expectGetParameterCommandWithResponse(false, false);
    }

    void expectServerStatusCommandWithResponse(std::string storageEngine, bool encryptionEnabled) {
        _mock->expect(BSON("serverStatus" << 1),
                      RemoteCommandResponse::make_forTest(
                          BSON("storageEngine"
                               << BSON("name" << storageEngine) << "encryptionAtRest"
                               << BSON("encryptionEnabled" << encryptionEnabled) << "ok" << 1),
                          Milliseconds()));
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulSyncSourceValidation() {
        expectSuccessfulHelloCommand();
        expectSuccessfulGetParameterCommand();
        expectServerStatusCommandWithResponse("wiredTiger", false);
    }

    void expectSuccessfulBackupCursorCall() {
        auto backupCursorRequest =
            BSON("aggregate" << 1 << "pipeline" << BSON_ARRAY(BSON("$backupCursor" << BSONObj())));

        auto getMoreRequest = BSON("getMore" << cursorDataMock.backupCursorId << "collection"
                                             << cursorDataMock.nss.coll());
        // For keeping the backup cursor alive.
        _mock->defaultExpect(getMoreRequest,
                             RemoteCommandResponse::make_forTest(
                                 cursorDataMock.getBackupCursorBatches(-1), Milliseconds()));

        // The sync source satisfies requirements for FCBIS.
        expectSuccessfulSyncSourceValidation();

        {
            MockNetwork::InSequence seq(*_mock);
            _mock
                ->expect(backupCursorRequest,
                         RemoteCommandResponse::make_forTest(
                             cursorDataMock.getBackupCursorBatches(0), Milliseconds()))
                .times(1);
            _mock
                ->expect(getMoreRequest,
                         RemoteCommandResponse::make_forTest(
                             cursorDataMock.getBackupCursorBatches(1), Milliseconds()))
                .times(1);

            _mock
                ->expect(getMoreRequest,
                         RemoteCommandResponse::make_forTest(
                             cursorDataMock.getBackupCursorBatches(-1), Milliseconds()))
                .times(1);
        }

        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulBackupCursorEmptyGetMore() {
        auto getMoreRequest = BSON("getMore" << cursorDataMock.backupCursorId << "collection"
                                             << cursorDataMock.nss.coll());
        _mock
            ->expect(getMoreRequest,
                     RemoteCommandResponse::make_forTest(cursorDataMock.getBackupCursorBatches(-1),
                                                         Milliseconds()))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulExtendedBackupCursorCall(int cursorIndex, const Timestamp& extendTo) {
        auto extendedBackupCursorRequest =
            BSON("aggregate" << 1 << "pipeline"
                             << BSON_ARRAY(BSON("$backupCursorExtend"
                                                << BSON("backupId" << cursorDataMock.backupId
                                                                   << "timestamp" << extendTo))));
        _mock
            ->expect(extendedBackupCursorRequest,
                     RemoteCommandResponse::make_forTest(
                         cursorDataMock.getExtendedCursorBatches(cursorIndex), Milliseconds()))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulReplSetGetStatusCall(const Timestamp& appliedOpTime, bool run = true) {
        auto response =
            BSON("ok" << 1.0 << "optimes" << BSON("appliedOpTime" << BSON("ts" << appliedOpTime)));
        _mock
            ->expect(BSON("replSetGetStatus" << 1),
                     RemoteCommandResponse::make_forTest(response, Milliseconds()))
            .times(1);
        if (run)
            _mock->runUntilExpectationsSatisfied();
    }

    void expectFailedReplSetGetStatusCall() {
        // This should not be a retriable error.
        auto response = BSON("ok" << 0 << "code" << ErrorCodes::UnknownError);
        _mock
            ->expect(BSON("replSetGetStatus" << 1),
                     RemoteCommandResponse::make_forTest(response, Milliseconds()))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulKillBackupCursorCall() {
        auto request = BSON("killCursors" << cursorDataMock.nss.coll() << "cursors"
                                          << BSON_ARRAY(cursorDataMock.backupCursorId));
        auto response =
            BSON("ok" << 1.0 << "cursorsKilled" << BSON_ARRAY(cursorDataMock.backupCursorId));

        _mock->expect(request, RemoteCommandResponse::make_forTest(response, Milliseconds()))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
    }

    void expectSuccessfulFileCloning() {
        // This sets up the _mockServer to reply with an empty file for every file clone attempt.
        auto response =
            CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                           0 /* cursorId */,
                           {BSON("byteOffset" << 0 << "endOfFile" << true << "data"
                                              << BSONBinData(0, 0, BinDataGeneral))})
                .toBSONAsInitialResponse();
        _mockServer->setCommandReply("aggregate", response);
    }

    void populateBackupFiles(OperationContext* opCtx, const std::vector<std::string>& filenames) {
        std::deque<BackupBlock> backupBlocks;
        for (const auto& filename : filenames) {
            BackupBlock file = BackupBlock(/*nss=*/boost::none,
                                           /*uuid=*/boost::none,
                                           storageGlobalParams.dbpath + '/' + filename);
            backupBlocks.push_back(file);
        }

        auto devNullEngine = checked_cast<DevNullKVEngine*>(
            opCtx->getClient()->getServiceContext()->getStorageEngine()->getEngine());
        devNullEngine->setBackupBlocks_forTest({backupBlocks});
    }

    void mockBackupFileData(const std::vector<std::string>& backupFileData) {
        std::vector<StatusWith<BSONObj>> responses;
        for (const auto& data : backupFileData) {
            BSONBinData bindata(data.data(), data.size(), BinDataGeneral);
            responses.emplace_back(
                CursorResponse(
                    NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                    0 /* cursorId */,
                    {BSON("byteOffset" << 0 << "endOfFile" << true << "data" << bindata)})
                    .toBSONAsInitialResponse());
        }
        _mockServer->setCommandReply("aggregate", responses);
    }

    void checkFileData(const std::string& relativePath, StringData data) {
        boost::filesystem::path filePath(storageGlobalParams.dbpath);
        filePath.append(InitialSyncFileMover::kInitialSyncDir.toString());
        filePath.append(relativePath);
        ASSERT(boost::filesystem::exists(filePath))
            << "File " << filePath.string() << " is missing";
        ASSERT_EQ(data.size(), boost::filesystem::file_size(filePath))
            << "File " << filePath.string() << "has the wrong size";
        std::string actualFileData(data.size(), 0);
        std::ifstream checkStream(filePath.string(), std::ios_base::in | std::ios_base::binary);
        checkStream.read(actualFileData.data(), data.size());
        ASSERT_EQ(data, actualFileData) << "File " << filePath.string() << "has the wrong contents";
    }

private:
    DataReplicatorExternalStateMock* _externalState;
    std::shared_ptr<FileCopyBasedInitialSyncer> _fileCopyBasedInitialSyncer;
    bool _executorThreadShutdownComplete = false;
    unittest::MinimumLoggedSeverityGuard replLogSeverityGuard{
        logv2::LogComponent::kReplicationInitialSync, logv2::LogSeverity::Debug(3)};
};

executor::ThreadPoolMock::Options FileCopyBasedInitialSyncerTest::makeThreadPoolMockOptions()
    const {
    executor::ThreadPoolMock::Options options;
    options.onCreateThread = []() {
        Client::initThread("FileCopyBasedInitialSyncerTest",
                           getGlobalServiceContext()->getService());
    };
    return options;
}

void advanceClock(NetworkInterfaceMock* net, Milliseconds duration) {
    executor::NetworkInterfaceMock::InNetworkGuard guard(net);
    auto when = net->now() + duration;
    ASSERT_EQUALS(when, net->runUntil(when));
}

auto convertSecToMills(unsigned secs) {
    return Milliseconds(static_cast<Milliseconds::rep>(1000 * secs));
}

ServiceContext::UniqueOperationContext makeOpCtx() {
    return cc().makeOperationContext();
}


TEST_F(FileCopyBasedInitialSyncerTest, elementary_getInitialSyncProgress) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());

    // Before starting initial sync.
    BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
    ASSERT_EQUALS(prog.nFields(), 4) << prog;
    ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
    ASSERT(prog["failedInitialSyncAttempts"].isNumber()) << prog;
    ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
    ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), 0) << prog;
    ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());

    const std::uint32_t maxAttempts = 1U;
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    // After running initial sync.
    prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
    ASSERT_EQUALS(prog.nFields(), 11) << prog;
    ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
    ASSERT(prog["failedInitialSyncAttempts"].isNumber()) << prog;
    ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
    ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
    ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
    ASSERT_EQUALS(prog["totalInitialSyncElapsedMillis"].type(), NumberLong) << prog;
    ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());
    ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 0) << prog;
    ASSERT_EQUALS(prog.getIntField("approxTotalBytesCopied"), 0) << prog;
    ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 0) << prog;
    ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
    ASSERT_BSONOBJ_EQ(prog.getObjectField("files"), BSONObj());
}


TEST_F(FileCopyBasedInitialSyncerTest, InvalidConstruction) {
    InitialSyncerInterface::Options options;
    options.getMyLastOptime = []() { return OpTime(); };
    options.setMyLastOptime = [](const OpTimeAndWallTime&) {};
    options.resetOptimes = []() {};
    options.syncSourceSelector = this;
    auto callback = [](const StatusWith<OpTimeAndWallTime>&) {};

    // Null task executor in external state.
    {
        auto dataReplicatorExternalState = std::make_unique<DataReplicatorExternalStateMock>();
        ASSERT_THROWS_CODE_AND_WHAT(
            FileCopyBasedInitialSyncer(options,
                                       std::move(dataReplicatorExternalState),
                                       _dbWorkThreadPool.get(),
                                       _storageInterface.get(),
                                       _replicationProcess.get(),
                                       callback),
            AssertionException,
            ErrorCodes::BadValue,
            "task executor cannot be null");
    }
    // Null callback function.
    {
        auto dataReplicatorExternalState = std::make_unique<DataReplicatorExternalStateMock>();
        dataReplicatorExternalState->taskExecutor = _executorProxy;
        ASSERT_THROWS_CODE_AND_WHAT(
            FileCopyBasedInitialSyncer(options,
                                       std::move(dataReplicatorExternalState),
                                       _dbWorkThreadPool.get(),
                                       _storageInterface.get(),
                                       _replicationProcess.get(),
                                       FileCopyBasedInitialSyncer::OnCompletionFn()),
            AssertionException,
            ErrorCodes::BadValue,
            "callback function cannot be null");
    }
}

TEST_F(FileCopyBasedInitialSyncerTest, CreateDestroy) {}

const std::uint32_t maxAttempts = 1U;

TEST_F(FileCopyBasedInitialSyncerTest, StartupReturnsIllegalOperationIfAlreadyActive) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    ASSERT_TRUE(fileCopyBasedInitialSyncer->isActive());
    ASSERT_EQUALS(ErrorCodes::IllegalOperation,
                  fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    ASSERT_TRUE(fileCopyBasedInitialSyncer->isActive());
}

TEST_F(FileCopyBasedInitialSyncerTest,
       StartupReturnsShutdownInProgressIfFileCopyBasedInitialSyncerIsShuttingDown) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    auto fCBISHangAfterShutdownCancellationFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterShutdownCancellation");
    auto timesEnteredFailPoint =
        fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::alwaysOn);
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    ASSERT_TRUE(fileCopyBasedInitialSyncer->isActive());

    unittest::ThreadAssertionMonitor monitor;
    // In shutdown, cancellation occurs, then futures which expect cancellation are waited on, so
    // mock network needs to run at the same time to process the responses from cancellation.
    auto shutdownThread = monitor.spawnController([&] {
        // SyncSourceSelector returns an invalid sync source so FileCopyBasedInitialSyncer is stuck
        // waiting for another sync source in 'Options::syncSourceRetryWait' ms.
        ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());
    });
    fCBISHangAfterShutdownCancellationFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);
    fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::off);
    _mock->runUntilIdle();

    ASSERT_EQUALS(ErrorCodes::ShutdownInProgress,
                  fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    shutdownThread.join();
}

TEST_F(FileCopyBasedInitialSyncerTest, StartupReturnsShutdownInProgressIfExecutorIsShutdown) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    getExecutor().shutdown();
    ASSERT_EQUALS(ErrorCodes::ShutdownInProgress,
                  fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());

    // Cannot startup file copy based initial syncer again since it's in the Complete state.
    ASSERT_EQUALS(ErrorCodes::ShutdownInProgress,
                  fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
}

TEST_F(FileCopyBasedInitialSyncerTest, CallbackIsCalledIfExecutorIsShutdownAfterStartup) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    ASSERT_TRUE(fileCopyBasedInitialSyncer->isActive());
    getExecutor().shutdown();
    ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, _lastApplied);
}

TEST_F(FileCopyBasedInitialSyncerTest, ShutdownTransitionsStateToCompleteIfCalledBeforeStartup) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());
    ASSERT_EQUALS(ErrorCodes::ShutdownInProgress,
                  fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    // Initial syncer is inactive when it's in the Complete state.
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());
}

TEST_F(FileCopyBasedInitialSyncerTest,
       FCBISReturnsCallbackCanceledIfShutdownImmediatelyAfterStartup) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    auto fCBISHangAfterShutdownCancellationFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterShutdownCancellation");
    auto timesEnteredFailPoint =
        fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::alwaysOn);

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    unittest::ThreadAssertionMonitor monitor;
    // In shutdown, cancellation occurs, then futures which expect cancellation are waited on, so
    // mock network needs to run at the same time to process the responses from cancellation.
    auto shutdownThread = monitor.spawnController([&] {
        // This will cancel the _startInitialSyncAttempt() task started by startup().
        ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());
    });
    fCBISHangAfterShutdownCancellationFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);
    fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::off);
    _mock->runUntilIdle();

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQUALS(ErrorCodes::CallbackCanceled, _lastApplied);
    shutdownThread.join();
}

TEST_F(FileCopyBasedInitialSyncerTest,
       FCBISRetriesSyncSourceSelectionIfChooseNewSyncSourceReturnsInvalidSyncSource) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    // Override chooseNewSyncSource() result in SyncSourceSelectorMock before calling startup()
    // because FileCopyBasedInitialSyncer will look for a valid sync source immediately after
    // startup.
    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort());

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    // Run first sync source selection attempt.
    executor::NetworkInterfaceMock::InNetworkGuard(getNet())->runReadyNetworkOperations();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::IllegalOperation);
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort());

    // First sync source selection attempt failed. Update SyncSourceSelectorMock to return valid
    // sync source next time chooseNewSyncSource() is called.
    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    // Advance clock until the next sync source selection attempt.
    advanceClock(getNet(), _options.syncSourceRetryWait);
    executor::NetworkInterfaceMock::InNetworkGuard(getNet())->runReadyNetworkOperations();

    // The sync source satisfies requirements for FCBIS.
    expectSuccessfulSyncSourceValidation();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort("localhost", 12345));
}

const std::uint32_t chooseSyncSourceMaxAttempts = 10U;

/**
 * Advances executor clock so that InitialSyncer exhausts all 'chooseSyncSourceMaxAttempts' (server
 * parameter numInitialSyncConnectAttempts) sync source selection attempts.
 * If SyncSourceSelectorMock keeps returning an invalid sync source, InitialSyncer will retry every
 * '_options.syncSourceRetryWait' ms up to a maximum of 'chooseSyncSourceMaxAttempts' attempts.
 */
void _simulateChooseSyncSourceFailure(executor::NetworkInterfaceMock* net,
                                      Milliseconds syncSourceRetryWait) {
    advanceClock(net, int(chooseSyncSourceMaxAttempts - 1) * syncSourceRetryWait);
}

TEST_F(
    FileCopyBasedInitialSyncerTest,
    FCBISReturnsInvalidSyncSourceIfNoValidSyncSourceCanBeFoundAfterTenFailedChooseSyncSourceAttempts) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    // Override chooseNewSyncSource() result in SyncSourceSelectorMock before calling startup()
    // because InitialSyncer will look for a valid sync source immediately after startup.
    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort());

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    _simulateChooseSyncSourceFailure(getNet(), _options.syncSourceRetryWait);

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
}

TEST_F(FileCopyBasedInitialSyncerTest, FCBISValidatesSyncSourceSuccessfully) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    // The sync source satisfies requirements for FCBIS.
    expectSuccessfulSyncSourceValidation();
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort("localhost", 12345));
}


TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceIsNotPrimaryOrSecondary) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        // The sync source is not a primary or secondary.
        expectHelloCommandWithResponse(false /* isWritablePrimary */,
                                       false /* secondary */,
                                       static_cast<int>(WireVersion::WIRE_VERSION_51));
        auto net = getNet();
        advanceClock(net, _options.syncSourceRetryWait);
    }
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::InvalidSyncSource);
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
    ASSERT_EQUALS("Sync source is invalid because it is not a primary or secondary.",
                  _lastApplied.getStatus().reason());
}

TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceHasInvalidWireVersion) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        // The sync source does not have a valid wire version for FCBIS.
        expectHelloCommandWithResponse(false /* isWritablePrimary */,
                                       true /* secondary */,
                                       static_cast<int>(WireVersion::WIRE_VERSION_50));
        auto net = getNet();
        advanceClock(net, _options.syncSourceRetryWait);
    }
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::InvalidSyncSource);
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
    ASSERT_EQUALS("Sync source is invalid because it does not have a valid wire version.",
                  _lastApplied.getStatus().reason());
}


TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceHasIncompatibleDirectoryPerDb) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        expectSuccessfulHelloCommand();
        // The sync source has directoryperdb set to true, while we have it set to false.
        expectGetParameterCommandWithResponse(true /* directoryperdb */,
                                              false /* directoryForIndexes */);
        auto net = getNet();
        advanceClock(net, _options.syncSourceRetryWait);
    }
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::InvalidSyncSource);
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
    ASSERT_EQUALS(
        "Sync source is invalid because its directoryPerDB parameter does not match the local "
        "value.",
        _lastApplied.getStatus().reason());
}


TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceHasIncompatibleDirectoryForIndexes) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        expectSuccessfulHelloCommand();
        // The sync source has directoryForIndexes set to true, while we have it set to false.
        expectGetParameterCommandWithResponse(false /* directoryperdb */,
                                              true /* directoryForIndexes */);
        auto net = getNet();
        advanceClock(net, _options.syncSourceRetryWait);
    }
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::InvalidSyncSource);
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
    ASSERT_EQUALS(
        "Sync source is invalid because its directoryForIndexes parameter does not match the local "
        "value.",
        _lastApplied.getStatus().reason());
}

TEST_F(FileCopyBasedInitialSyncerTest, FCBISFailsIfNotUsingWiredTiger) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    storageGlobalParams.engine = "test";

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->waitForStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::IncompatibleServerVersion);
    ASSERT_EQUALS(ErrorCodes::IncompatibleServerVersion, _lastApplied);
    ASSERT_EQUALS("File copy based initial sync requires using the WiredTiger storage engine.",
                  _lastApplied.getStatus().reason());
    storageGlobalParams.engine = "wiredTiger";
}


TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceHasIncompatibleStorageEngine) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        expectSuccessfulHelloCommand();
        expectSuccessfulGetParameterCommand();
        // The sync source is not using the WiredTiger storage engine.
        expectServerStatusCommandWithResponse("testStorageEngine", false /* encryptionEnabled */);

        auto net = getNet();
        advanceClock(net, _options.syncSourceRetryWait);
    }
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::InvalidSyncSource);
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
    ASSERT_EQUALS(
        "Both the sync source and the local node must be using the WiredTiger storage engine.",
        _lastApplied.getStatus().reason());
}


TEST_F(FileCopyBasedInitialSyncerTest,
       FCBISRetriesIfSyncSourceHasIncompatibleEncryptedStorageEngine) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        expectSuccessfulHelloCommand();
        expectSuccessfulGetParameterCommand();
        // The sync source is using the encrypted storage engine, while we are not.
        expectServerStatusCommandWithResponse("wiredTiger", true /* encryptionEnabled */);
        auto net = getNet();
        advanceClock(net, _options.syncSourceRetryWait);
    }
    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::InvalidSyncSource);
    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
    ASSERT_EQUALS(
        "Both the sync source and the local node must be using the encrypted storage engine, or "
        "neither.",
        _lastApplied.getStatus().reason());
}


TEST_F(FileCopyBasedInitialSyncerTest, FCBISSucceedsIfBothNodesAreNotUsingEncryptedStorageEngine) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    expectSuccessfulHelloCommand();
    expectSuccessfulGetParameterCommand();
    // The sync source does not have the encryptionAtRest field, which should not cause an error.
    // The local node is also not using the encrypted storage engine, so FCBIS should succeed.
    _mock->expect(BSON("serverStatus" << 1),
                  RemoteCommandResponse::make_forTest(BSON("storageEngine" << BSON("name"
                                                                                   << "wiredTiger")
                                                                           << "ok" << 1),
                                                      Milliseconds()));
    _mock->runUntilExpectationsSatisfied();

    fileCopyBasedInitialSyncer->join();

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort("localhost", 12345));
}


// Confirms that FileCopyBasedInitialSyncer keeps retrying initial sync.
// Make every initial sync attempt fail early by having the sync source selector always return an
// invalid sync source.
TEST_F(FileCopyBasedInitialSyncerTest,
       FCBISRetriesInitialSyncUpToMaxAttemptsAndReturnsLastAttemptError) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort());

    const std::uint32_t initialSyncMaxAttempts = 3U;
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), initialSyncMaxAttempts));

    auto net = getNet();
    for (std::uint32_t i = 0; i < initialSyncMaxAttempts; ++i) {
        _simulateChooseSyncSourceFailure(net, _options.syncSourceRetryWait);
        advanceClock(net, _options.initialSyncRetryWait);
    }

    fileCopyBasedInitialSyncer->join();

    ASSERT_EQUALS(ErrorCodes::InvalidSyncSource, _lastApplied);
}

TEST_F(FileCopyBasedInitialSyncerTest,
       FCBISReturnsCallbackCanceledIfShutdownWhileRetryingSyncSourceSelection) {
    auto fCBISHangAfterShutdownCancellationFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterShutdownCancellation");
    auto timesEnteredFailPoint =
        fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::alwaysOn);
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort());
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    auto net = getNet();
    {
        executor::NetworkInterfaceMock::InNetworkGuard guard(net);
        auto when = net->now() + _options.syncSourceRetryWait / 2;
        ASSERT_GREATER_THAN(when, net->now());
        ASSERT_EQUALS(when, net->runUntil(when));
    }

    unittest::ThreadAssertionMonitor monitor;
    // In shutdown, cancellation occurs, then futures which expect cancellation are waited on, so
    // mock network needs to run at the same time to process the responses from cancellation.
    auto shutdownThread = monitor.spawnController([&] {
        // This will cancel the _chooseSyncSourceCallback() task scheduled at getNet()->now() +
        // '_options.syncSourceRetryWait'
        ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());
    });
    ON_BLOCK_EXIT([&] { shutdownThread.join(); });
    fCBISHangAfterShutdownCancellationFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);
    fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::off);
    _mock->runUntilIdle();

    fileCopyBasedInitialSyncer->join();

    ASSERT_EQUALS(ErrorCodes::CallbackCanceled, _lastApplied);
}

TEST_F(FileCopyBasedInitialSyncerTest, SyncingFilesUsingBackupCursorOnly) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulFileCloning();
    expectSuccessfulBackupCursorCall();
    // Mark the initial sync as done as long as the lag is not greater than
    // 'fileBasedInitialSyncMaxLagSec'.
    expectSuccessfulReplSetGetStatusCall(
        Timestamp(cursorDataMock.checkpointTimestamp.getSecs() + fileBasedInitialSyncMaxLagSec, 0));
    ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getBackupCursorFiles_forTest(),
                             cursorDataMock.backupCursorFiles));

    expectSuccessfulKillBackupCursorCall();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
}

TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfBackupCursorAlreadyOpen) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    auto backupCursorRequest =
        BSON("aggregate" << 1 << "pipeline" << BSON_ARRAY(BSON("$backupCursor" << BSONObj())));

    expectSuccessfulSyncSourceValidation();
    auto errorCode = 50886;
    _mock
        ->expect(
            backupCursorRequest,
            RemoteCommandResponse::make_forTest(
                Status(
                    ErrorCodes::Error(errorCode),
                    "The existing backup cursor must be closed before $backupCursor can succeed."),
                Milliseconds()))
        .times(1);

    _mock->runUntilExpectationsSatisfied();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest().code(),
              5973001);
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest().reason(),
              "Could not open backup cursor on the sync source: The existing backup cursor must be "
              "closed before $backupCursor can succeed.");
}

TEST_F(FileCopyBasedInitialSyncerTest, FCBISFailsIfSyncSourceIsFsyncLocked) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    auto backupCursorRequest =
        BSON("aggregate" << 1 << "pipeline" << BSON_ARRAY(BSON("$backupCursor" << BSONObj())));
    expectSuccessfulSyncSourceValidation();
    auto errorCode = 50887;
    _mock
        ->expect(backupCursorRequest,
                 RemoteCommandResponse::make_forTest(
                     Status(ErrorCodes::Error(errorCode), "The node is currently fsyncLocked."),
                     Milliseconds()))
        .times(1);

    _mock->runUntilExpectationsSatisfied();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest().code(),
              5973001);
    ASSERT_EQ(
        fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest().reason(),
        "Could not open backup cursor on the sync source: The node is currently fsyncLocked.");
}

TEST_F(FileCopyBasedInitialSyncerTest, FCBISFailsIfBackupCursorCommandNotSupported) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    auto backupCursorRequest =
        BSON("aggregate" << 1 << "pipeline" << BSON_ARRAY(BSON("$backupCursor" << BSONObj())));

    expectSuccessfulSyncSourceValidation();
    auto errorCode = 40324;
    _mock
        ->expect(backupCursorRequest,
                 RemoteCommandResponse::make_forTest(
                     Status(ErrorCodes::Error(errorCode),
                            "Unrecognized pipeline stage name: '$backupCursor'"),
                     Milliseconds()))
        .times(1);

    _mock->runUntilExpectationsSatisfied();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest().code(),
              5973001);
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest().reason(),
              "Could not open backup cursor on the sync source: Unrecognized pipeline stage name: "
              "'$backupCursor'");
}

TEST_F(FileCopyBasedInitialSyncerTest, AlwaysKillBackupCursorOnFailure) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulFileCloning();
    expectSuccessfulBackupCursorCall();
    // Raise failure after opening the backup cursor.
    expectFailedReplSetGetStatusCall();
    ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getBackupCursorFiles_forTest(),
                             cursorDataMock.backupCursorFiles));

    {
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        ASSERT_EQUALS(prog.nFields(), 7) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 1) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT_EQUALS(prog["initialSyncEnd"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        BSONObj attempts = prog["initialSyncAttempts"].Obj();
        ASSERT_EQUALS(attempts.nFields(), 1) << attempts;
        auto attempt0 = attempts["0"].Obj();
        ASSERT_EQUALS(attempt0.nFields(), 6) << attempt0;
        ASSERT_EQUALS(attempt0["status"].str(), "UnknownError: ") << attempts;
        ASSERT_EQUALS(attempt0["syncSource"].str(), "localhost:12345") << attempts;
        ASSERT_GREATER_THAN(attempt0.getIntField("durationMillis"), 0) << attempts;
    }
    expectSuccessfulKillBackupCursorCall();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQUALS(ErrorCodes::UnknownError, _lastApplied);
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              ErrorCodes::UnknownError);
}

TEST_F(FileCopyBasedInitialSyncerTest, AlwaysKillBackupCursorOnShutdown) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);
    auto fCBISHangAfterShutdownCancellationFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterShutdownCancellation");
    auto timesEnteredFailPoint =
        fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::alwaysOn);


    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulFileCloning();
    expectSuccessfulBackupCursorCall();

    unittest::ThreadAssertionMonitor monitor;
    // In shutdown, cancellation occurs, then futures which expect cancellation are waited on, so
    // mock network needs to run at the same time to process the responses from cancellation.
    auto shutdownThread =
        monitor.spawnController([&] { ASSERT_OK(fileCopyBasedInitialSyncer->shutdown()); });
    fCBISHangAfterShutdownCancellationFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);
    fCBISHangAfterShutdownCancellationFailPoint->setMode(FailPoint::off);
    _mock->runUntilIdle();

    fileCopyBasedInitialSyncer->join();
    expectSuccessfulKillBackupCursorCall();
    ASSERT_EQUALS(ErrorCodes::CallbackCanceled, _lastApplied);
    shutdownThread.join();
}

TEST_F(FileCopyBasedInitialSyncerTest, SyncingFilesUsingExtendedCursors) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulFileCloning();
    expectSuccessfulBackupCursorCall();
    // Force the lag to be greater than 'fileBasedInitialSyncMaxLagSec' to extend the backupCursor.
    auto timeStamp = Timestamp(
        cursorDataMock.checkpointTimestamp.getSecs() + fileBasedInitialSyncMaxLagSec + 1, 0);
    expectSuccessfulReplSetGetStatusCall(timeStamp);
    expectSuccessfulExtendedBackupCursorCall(0, timeStamp);

    // Force the lag to be greater than 'fileBasedInitialSyncMaxLagSec' to extend the backupCursor
    // again.
    timeStamp = Timestamp(timeStamp.getSecs() + fileBasedInitialSyncMaxLagSec + 1, 0);
    expectSuccessfulReplSetGetStatusCall(timeStamp);
    expectSuccessfulExtendedBackupCursorCall(1, timeStamp);

    // Mark the initial sync as done as long as the lag is not greater than
    // 'fileBasedInitialSyncMaxLagSec'.
    timeStamp = Timestamp(timeStamp.getSecs() + fileBasedInitialSyncMaxLagSec, 0);
    expectSuccessfulReplSetGetStatusCall(timeStamp);

    ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getBackupCursorFiles_forTest(),
                             cursorDataMock.backupCursorFiles));
    ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getExtendedBackupCursorFiles_forTest(),
                             cursorDataMock.extendedCursorFiles));

    expectSuccessfulKillBackupCursorCall();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
}

TEST_F(FileCopyBasedInitialSyncerTest, KeepingBackupCursorAlive) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    // Enable the failpoint to pause after opening the backupCursor.
    auto hangAfterBackupCursorFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterOpeningBackupCursor");
    auto timesEnteredFailPoint = hangAfterBackupCursorFailPoint->setMode(FailPoint::alwaysOn);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulFileCloning();
    expectSuccessfulBackupCursorCall();
    hangAfterBackupCursorFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);

    // We will keep extended the backup cursor every
    // 'kFileCopyBasedInitialSyncKeepBackupCursorAliveIntervalInMin', testing it only 6 times.
    for (int i = 0; i < 5; i++) {
        advanceClock(
            getNet(),
            convertSecToMills(kFileCopyBasedInitialSyncKeepBackupCursorAliveIntervalInMin * 60));
        expectSuccessfulBackupCursorEmptyGetMore();
    }

    hangAfterBackupCursorFailPoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));

    // Mark the initial sync as done as long as the lag is not greater than
    // 'fileBasedInitialSyncMaxLagSec'.
    expectSuccessfulReplSetGetStatusCall(
        Timestamp(cursorDataMock.checkpointTimestamp.getSecs() + fileBasedInitialSyncMaxLagSec, 0));
    ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getBackupCursorFiles_forTest(),
                             cursorDataMock.backupCursorFiles));

    expectSuccessfulKillBackupCursorCall();
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
}

TEST_F(FileCopyBasedInitialSyncerTest, ResolvesRelativePaths) {
    /* Unix path with no separator at end of dbpath */
    ASSERT_EQ("dirname/filename",
              FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(
                  "/path/to/dbpath/dirname/filename", "/path/to/dbpath"));
    /* Unix path with separator at end of dbpath */
    ASSERT_EQ("dirname/filename",
              FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(
                  "/path/to/dbpath/dirname/filename", "/path/to/dbpath/"));
    /* Windows path without separator at end of dbpath */
    ASSERT_EQ("dirname/filename",
              FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(
                  R"(Q:\path\to\dbpath\dirname\filename)", R"(Q:\path\to\dbpath)"));
    ASSERT_EQ("dirname/filename",
              FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(
                  R"(Q:\path\to\dbpath\dirname\filename)", R"(Q:\path\to\dbpath\)"));

    ASSERT_THROWS(FileCopyBasedInitialSyncer::getPathRelativeTo_forTest(
                      "/path/to/elsewhere/dirname/filename", "/path/to/dbpath"),
                  AssertionException);
}

TEST_F(FileCopyBasedInitialSyncerTest, ClonesFilesFromInitialSource) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    UUID backupId = UUID::gen();
    Timestamp checkpointTs(1, 1);
    _mock->defaultExpect(
        BSON("getMore" << 1 << "collection"
                       << "$cmd.aggregate"),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin), 1, {})
            .toBSON(CursorResponse::ResponseType::SubsequentResponse));
    _mock->defaultExpect(
        BSON("replSetGetStatus" << 1),
        BSON("optimes" << BSON("appliedOpTime" << OpTime(checkpointTs, 1)) << "ok" << 1));
    _mock->defaultExpect(BSON("killCursors"
                              << "$cmd.aggregate"),
                         BSON("ok" << 1));
    // The sync source satisfies requirements for FCBIS.
    expectSuccessfulSyncSourceValidation();

    std::string file1Data = "ABCDEF_DATA_FOR_FILE_1";
    std::string file2Data = "0123456789_DATA_FOR_FILE_2";
    _mock->expect(
        BSON("aggregate" << 1 << "pipeline" << BSON("$eq" << BSON("$backupCursor" << BSONObj()))),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                       1 /* cursorId */,
                       {BSON("metadata" << BSON("backupId" << backupId << "checkpointTimestamp"
                                                           << checkpointTs << "dbpath"
                                                           << "/path/to/dbpath")),
                        BSON("filename"
                             << "/path/to/dbpath/backupfile1"
                             << "fileSize" << int64_t(file1Data.size())),
                        BSON("filename"
                             << "/path/to/dbpath/backupfile2"
                             << "fileSize" << int64_t(file2Data.size()))})
            .toBSONAsInitialResponse());

    mockBackupFileData({file1Data, file2Data});
    _mock->runUntilExpectationsSatisfied();

    fileCopyBasedInitialSyncer->join();
    checkFileData("backupfile1", file1Data);
    checkFileData("backupfile2", file2Data);

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort("localhost", 12345));
}

TEST_F(FileCopyBasedInitialSyncerTest, ClonesFilesFromExtendedCursor) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    UUID backupId = UUID::gen();
    Timestamp checkpointTs(1, 1);
    // Our checkpoint will be lagged to force an extended cursor.
    Timestamp lastAppliedTs(fileBasedInitialSyncMaxLagSec + 2, 1);
    _mock->defaultExpect(
        BSON("getMore" << 1 << "collection"
                       << "$cmd.aggregate"),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin), 1, {})
            .toBSON(CursorResponse::ResponseType::SubsequentResponse));
    _mock->defaultExpect(
        BSON("replSetGetStatus" << 1),
        BSON("optimes" << BSON("appliedOpTime" << OpTime(lastAppliedTs, 1)) << "ok" << 1));
    _mock->defaultExpect(BSON("killCursors"
                              << "$cmd.aggregate"),
                         BSON("ok" << 1));
    // The sync source satisfies requirements for FCBIS.
    expectSuccessfulSyncSourceValidation();

    // Backup cursor query returns metadata but no files.
    _mock->expect(
        BSON("aggregate" << 1 << "pipeline" << BSON("$eq" << BSON("$backupCursor" << BSONObj()))),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                       1 /* cursorId */,
                       {BSON("metadata" << BSON("backupId" << backupId << "checkpointTimestamp"
                                                           << checkpointTs << "dbpath"
                                                           << "/path/to/dbpath"))})
            .toBSONAsInitialResponse());

    // Backup cursor extend query returns files
    _mock->expect(
        BSON("aggregate" << 1 << "pipeline.$backupCursorExtend" << BSON("$exists" << true)),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                       1 /* cursorId */,
                       {BSON("filename"
                             << "/path/to/dbpath/journal/log.0001"),
                        BSON("filename"
                             << "/path/to/dbpath/journal/log.0002")})
            .toBSONAsInitialResponse());

    std::string file1Data = "ABCDEF_DATA_FOR_FILE_1";
    std::string file2Data = "012345_DATA_FOR_FILE_2";
    mockBackupFileData({file1Data, file2Data});
    _mock->runUntilExpectationsSatisfied();

    fileCopyBasedInitialSyncer->join();
    checkFileData("journal/log.0001", file1Data);
    checkFileData("journal/log.0002", file2Data);

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort("localhost", 12345));
}

TEST_F(FileCopyBasedInitialSyncerTest, DeleteOldStorageFilesPhase) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::alwaysOn);
    auto hangBeforeDeletingOldStorageFilesFailPoint =
        globalFailPointRegistry().find("fCBISHangBeforeDeletingOldStorageFiles");
    auto timesEnteredFailPoint =
        hangBeforeDeletingOldStorageFilesFailPoint->setMode(FailPoint::alwaysOn);
    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    // Add old storage files that will be deleted.
    cursorDataMock.createStorageFiles();
    cursorDataMock.validateFilesExistence(true /*shouldExist*/);

    // Let initial sync works untill hitting the delete phase.
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulSyncSourceValidation();
    hangBeforeDeletingOldStorageFilesFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);

    // Override the files to be deleted with the files we added to the dbpath.
    fileCopyBasedInitialSyncer->setOldStorageFilesToBeDeleted_forTest(
        cursorDataMock.getAllRemoteFilesRelativePath());

    // After we finish deleting the old storage files, we need to ensure that the delete marker was
    // successfully written to disk.
    auto hangAfterDeletingOldStorageFilesFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterDeletingOldStorageFiles");
    timesEnteredFailPoint = hangAfterDeletingOldStorageFilesFailPoint->setMode(FailPoint::alwaysOn);
    hangBeforeDeletingOldStorageFilesFailPoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));
    hangAfterDeletingOldStorageFilesFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);

    cursorDataMock.validateMarkerExistence(true /*shouldExist*/,
                                           InitialSyncFileMover::kFilesToDeleteMarker);
    // Let initial sync finish.
    hangAfterDeletingOldStorageFilesFailPoint->setMode(FailPoint::off);
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());

    // Make sure that all files got deleted.
    cursorDataMock.validateFilesExistence(false /*shouldExist*/);
}

TEST_F(FileCopyBasedInitialSyncerTest, WatchdogIsPausedDuringStorageChange) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();

    auto watchdogMonitor = WatchdogMonitorInterface::getGlobalWatchdogMonitorInterface();
    // Watchdog should not be paused.
    ASSERT_EQ(watchdogMonitor->getShouldRunChecks_forTest(), true);

    auto opCtx = makeOpCtx();
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::alwaysOn);
    globalFailPointRegistry().find("fCBISSkipMovingFilesPhase")->setMode(FailPoint::off);

    auto hangAfterPausingWatchdogChecksFailpoint =
        globalFailPointRegistry().find("fCBISHangAfterPausingWatchdogChecks");
    auto hangAfterMovingTheNewFilesFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterMovingTheNewFiles");

    auto timesEnteredHangAfterPausingWatchdogChecksFailPoint =
        hangAfterPausingWatchdogChecksFailpoint->setMode(FailPoint::alwaysOn);

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    // Add old storage files that will be deleted.
    cursorDataMock.createStorageFiles();
    cursorDataMock.validateFilesExistence(true /*shouldExist*/);

    // Create initial sync dummy dir.
    cursorDataMock.createInitialSyncDummyDirectory();
    cursorDataMock.validateInitialSyncDummyDirectoryExistence(true /*shouldExist*/);

    // Override the files to be deleted with the files we added to the dbpath.
    fileCopyBasedInitialSyncer->setOldStorageFilesToBeDeleted_forTest(
        cursorDataMock.getAllRemoteFilesRelativePath());

    // Let initial sync work until before pausing watchdog.
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulSyncSourceValidation();

    hangAfterPausingWatchdogChecksFailpoint->waitForTimesEntered(
        timesEnteredHangAfterPausingWatchdogChecksFailPoint + 1);

    // Watchdog should be paused.
    ASSERT_EQ(watchdogMonitor->getShouldRunChecks_forTest(), false);


    // Create the delete marker.
    cursorDataMock.currentFileMover->writeMarker(cursorDataMock.deleteMarkerFilesList,
                                                 InitialSyncFileMover::kFilesToDeleteMarker,
                                                 InitialSyncFileMover::kFilesToDeleteTmpMarker);
    cursorDataMock.validateMarkerExistence(true /*shouldExist*/,
                                           InitialSyncFileMover::kFilesToDeleteMarker);


    auto timesEnteredHangAfterMovingTheNewFiles =
        hangAfterMovingTheNewFilesFailPoint->setMode(FailPoint::alwaysOn);

    // Advance to after moving the new files.
    hangAfterPausingWatchdogChecksFailpoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));

    hangAfterMovingTheNewFilesFailPoint->waitForTimesEntered(
        timesEnteredHangAfterMovingTheNewFiles + 1);

    // Watchdog should still be paused.
    ASSERT_EQ(watchdogMonitor->getShouldRunChecks_forTest(), false);


    // Let initial sync finish.
    hangAfterMovingTheNewFilesFailPoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());


    // Watchdog should run checks after FCBIS is finished.
    ASSERT_EQ(watchdogMonitor->getShouldRunChecks_forTest(), true);
}

TEST_F(FileCopyBasedInitialSyncerTest, MoveNewFilesPhase) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::alwaysOn);
    globalFailPointRegistry().find("fCBISSkipMovingFilesPhase")->setMode(FailPoint::off);
    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    auto hangBeforeDeletingTheDeleteMarkerFailPoint =
        globalFailPointRegistry().find("fCBISHangBeforeDeletingTheDeleteMarker");
    auto hangBeforeMovingTheNewFilesFailPoint =
        globalFailPointRegistry().find("fCBISHangBeforeMovingTheNewFiles");
    auto hangAfterMovingTheNewFilesFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterMovingTheNewFiles");

    // Add new storage files that will be moved.
    cursorDataMock.createStorageFiles(true /*insideInitialSyncDir*/);
    cursorDataMock.validateFilesExistence(true /*shouldExist*/, true /*insideInitialSyncDir*/);
    // Create initial sync dummy dir.
    cursorDataMock.createInitialSyncDummyDirectory();
    cursorDataMock.validateInitialSyncDummyDirectoryExistence(true /*shouldExist*/);

    // Advance to the step of deleting the delete marker.
    auto timesEnteredFailPoint =
        hangBeforeDeletingTheDeleteMarkerFailPoint->setMode(FailPoint::alwaysOn);
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    expectSuccessfulSyncSourceValidation();
    hangBeforeDeletingTheDeleteMarkerFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);

    // Moving marker should exist.
    cursorDataMock.validateMarkerExistence(true /*shouldExist*/,
                                           InitialSyncFileMover::kMovingFilesMarker);

    // Create the delete marker.
    cursorDataMock.currentFileMover->writeMarker(cursorDataMock.deleteMarkerFilesList,
                                                 InitialSyncFileMover::kFilesToDeleteMarker,
                                                 InitialSyncFileMover::kFilesToDeleteTmpMarker);
    cursorDataMock.validateMarkerExistence(true /*shouldExist*/,
                                           InitialSyncFileMover::kFilesToDeleteMarker);
    cursorDataMock.validateMarkerExistence(false /*shouldExist*/,
                                           InitialSyncFileMover::kFilesToDeleteTmpMarker);

    timesEnteredFailPoint = hangBeforeMovingTheNewFilesFailPoint->setMode(FailPoint::alwaysOn);
    // Let the delete marker get deleted.
    hangBeforeDeletingTheDeleteMarkerFailPoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));
    hangBeforeMovingTheNewFilesFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);

    // Delete marker should be deleted.
    cursorDataMock.validateMarkerExistence(false /*shouldExist*/,
                                           InitialSyncFileMover::kFilesToDeleteMarker);
    // Check files are in initialSync directory.
    cursorDataMock.validateFilesExistence(true /*shouldExist*/, true /*insideInitialSyncDir*/);
    cursorDataMock.validateInitialSyncDummyDirectoryExistence(true /*shouldExist*/);

    timesEnteredFailPoint = hangAfterMovingTheNewFilesFailPoint->setMode(FailPoint::alwaysOn);
    hangBeforeMovingTheNewFilesFailPoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));
    hangAfterMovingTheNewFilesFailPoint->waitForTimesEntered(timesEnteredFailPoint + 1);

    // Moving marker should exist.
    cursorDataMock.validateMarkerExistence(true /*shouldExist*/,
                                           InitialSyncFileMover::kMovingFilesMarker);
    // Check files are in dbpath.
    cursorDataMock.validateFilesExistence(true /*shouldExist*/, false /*insideInitialSyncDir*/);
    // Check files are not in initialSync directory.
    cursorDataMock.validateFilesExistence(false /*shouldExist*/, true /*insideInitialSyncDir*/);
    // Initial sync dir should exist.
    cursorDataMock.validateInitialSyncDirExistence(true);
    // Initial sync dummy dir should exist.
    cursorDataMock.validateInitialSyncDummyDirectoryExistence(true /*shouldExist*/);

    // Let initial sync finish.
    hangAfterMovingTheNewFilesFailPoint->setMode(FailPoint::off);
    // Advance the clock to make sure that the failPoint hang loop terminates.
    advanceClock(getNet(), Milliseconds(static_cast<Milliseconds::rep>(100)));

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());

    // Moving marker shouldn't exist.
    cursorDataMock.validateMarkerExistence(false /*shouldExist*/,
                                           InitialSyncFileMover::kMovingFilesMarker);
    // Initial sync dir should be removed.
    cursorDataMock.validateInitialSyncDirExistence(false /*shouldExist*/);
}

TEST_F(FileCopyBasedInitialSyncerTest, CleanUpLocalCollectionsAfterSync) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    auto oplogTruncateAfterPointBefore =
        _replicationProcess->getConsistencyMarkers()->getOplogTruncateAfterPoint(opCtx.get());
    ASSERT_EQ(oplogTruncateAfterPointBefore, Timestamp());
    auto initialSyncIdBefore =
        _replicationProcess->getConsistencyMarkers()->getInitialSyncId(opCtx.get());
    ASSERT_TRUE(initialSyncIdBefore.isEmpty());

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    fileCopyBasedInitialSyncer->setLastSyncedOpTime_forTest(Timestamp(1, 1));
    ASSERT_EQ(fileCopyBasedInitialSyncer->_cleanUpLocalCollectionsAfterSync_forTest(
                  opCtx.get(),
                  getExternalState()->loadLocalConfigDocument(opCtx.get()),
                  StatusWith<LastVote>(ErrorCodes::NoMatchingDocument, "no vote")),
              Status::OK());

    auto oplogTruncateAfterPointAfter =
        _replicationProcess->getConsistencyMarkers()->getOplogTruncateAfterPoint(opCtx.get());
    ASSERT_EQ(oplogTruncateAfterPointAfter, oplogTruncateAfterPointBefore);
    auto initialSyncIdAfter =
        _replicationProcess->getConsistencyMarkers()->getInitialSyncId(opCtx.get());

    ASSERT_FALSE(initialSyncIdAfter.isEmpty());
}

TEST_F(FileCopyBasedInitialSyncerTest,
       VerifyEmptyListOfStorageFilesToBeDeletedIsCorrectlyRecorded) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    // Add empty list of files to devNullEngine for the local backup cursor.
    std::vector<std::string> backupFilenames = {};
    populateBackupFiles(opCtx.get(), backupFilenames);

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::alwaysOn, 0);
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    expectSuccessfulSyncSourceValidation();

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());

    // The only filenames in '_syncingFilesState' should be the fixed-name files that are not
    // returned by the backup cursor. These three files are explicitly added to the
    // 'oldStorageFilesToBeDeleted' vector.
    auto result = fileCopyBasedInitialSyncer->getOldStorageFilesToBeDeleted_forTest();
    ASSERT_EQUALS(4, result.size());
}

TEST_F(FileCopyBasedInitialSyncerTest, VerifyStorageFilesToBeDeletedAreCorrectlyRecorded) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    // Add files to devNullEngine for the local backup cursor to read.
    std::vector<std::string> backupFilenames = {"filename1.wt", "filename2.wt", "filename3.wt"};
    populateBackupFiles(opCtx.get(), backupFilenames);

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::alwaysOn, 0);
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    expectSuccessfulSyncSourceValidation();

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());

    // Verify the list of storage files were written to _syncingFilesState.
    auto result = fileCopyBasedInitialSyncer->getOldStorageFilesToBeDeleted_forTest();
    // We add 4 to the size of 'backupFilenames' to account for the four fixed-name files
    // explicitly added to the 'oldStorageFilesToBeDeleted' vector. These files are not returned by
    // the backup cursor.
    ASSERT_EQUALS(backupFilenames.size() + 4, result.size());
    for (const auto& filename : backupFilenames) {
        ASSERT(std::find(result.begin(), result.end(), filename) != result.end());
    }
}

TEST_F(FileCopyBasedInitialSyncerTest, SyncingFilesUsingBackupCursorOnlyGetStats) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    auto hangAfterStartingFileClone =
        globalFailPointRegistry().find("fCBISHangAfterStartingFileClone");
    auto timesEnteredHangAfterStartingFileClone =
        hangAfterStartingFileClone->setMode(FailPoint::alwaysOn);

    auto hangAfterFileCloning = globalFailPointRegistry().find("fCBISHangAfterFileCloning");
    auto timesEnteredHangAfterFileCloning = hangAfterFileCloning->setMode(FailPoint::alwaysOn);

    auto hangBeforeFinish = globalFailPointRegistry().find("fCBISHangBeforeFinish");
    auto timesEnteredHangBeforeFinish = hangBeforeFinish->setMode(FailPoint::alwaysOn);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    stdx::thread bgThread;
    // Run in another thread.
    bgThread = stdx::thread([&] {
        Client::initThread("Expector", getGlobalServiceContext()->getService());
        expectSuccessfulFileCloning();
        expectSuccessfulBackupCursorCall();
        // Mark the initial sync as done as long as the lag is not greater than
        // 'fileBasedInitialSyncMaxLagSec'.
        expectSuccessfulReplSetGetStatusCall(Timestamp(
            cursorDataMock.checkpointTimestamp.getSecs() + fileBasedInitialSyncMaxLagSec, 0));
        ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getBackupCursorFiles_forTest(),
                                 cursorDataMock.backupCursorFiles));

        expectSuccessfulKillBackupCursorCall();
    });

    {
        hangAfterStartingFileClone->waitForTimesEntered(timesEnteredHangAfterStartingFileClone + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangAfterStartingFileClone->setMode(FailPoint::off);

        ASSERT_EQUALS(prog.nFields(), 13) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog.getIntField("approxTotalBytesCopied"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog.getIntField("totalTimeUnreachableMillis"), 0) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 1) << files;
        auto file0 = files["0"].Obj();
        ASSERT_EQUALS(file0.nFields(), 8) << files;
        ASSERT_EQUALS(file0["filePath"].str(), "WiredTiger") << prog;
        ASSERT_EQUALS(file0.getIntField("fileSize"), 47) << prog;
        ASSERT_EQUALS(file0.getIntField("bytesCopied"), 0) << prog;
        ASSERT_EQUALS(file0["start"].type(), Date) << prog;
        ASSERT_EQUALS(file0["end"].type(), Date) << prog;
        ASSERT_EQUALS(file0.getIntField("elapsedMillis"), 0) << prog;
        ASSERT_EQUALS(file0.getIntField("receivedBatches"), 1) << prog;
        ASSERT_EQUALS(file0.getIntField("writtenBatches"), 1) << prog;
    }
    {
        hangAfterFileCloning->waitForTimesEntered(timesEnteredHangAfterFileCloning + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangAfterFileCloning->setMode(FailPoint::off);

        ASSERT_EQUALS(prog.nFields(), 13) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog.getIntField("approxTotalBytesCopied"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog.getIntField("totalTimeUnreachableMillis"), 0) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 10) << files;

        auto file0 = files["0"].Obj();
        ASSERT_EQUALS(file0.nFields(), 8) << files;
        ASSERT_EQUALS(file0["filePath"].str(), "WiredTiger") << prog;
        ASSERT_EQUALS(file0.getIntField("fileSize"), 47) << prog;
        ASSERT_EQUALS(file0.getIntField("bytesCopied"), 0) << prog;
        ASSERT_EQUALS(file0["start"].type(), Date) << prog;
        ASSERT_EQUALS(file0["end"].type(), Date) << prog;
        ASSERT_EQUALS(file0.getIntField("elapsedMillis"), 0) << prog;
        ASSERT_EQUALS(file0.getIntField("receivedBatches"), 1) << prog;
        ASSERT_EQUALS(file0.getIntField("writtenBatches"), 1) << prog;
    }
    {
        hangBeforeFinish->waitForTimesEntered(timesEnteredHangBeforeFinish + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangBeforeFinish->setMode(FailPoint::off);

        ASSERT_EQUALS(prog.nFields(), 14) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        BSONObj attempts = prog["initialSyncAttempts"].Obj();
        ASSERT_EQUALS(attempts.nFields(), 1) << attempts;
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 314752297) << prog;
        ASSERT_GREATER_THAN_OR_EQUALS(prog.getIntField("approxTotalBytesCopied"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog.getIntField("totalTimeUnreachableMillis"), 0) << prog;
        ASSERT_EQUALS(prog["syncSourceLastApplied"].type(), bsonTimestamp) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 10) << files;
    }

    bgThread.join();

    {
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();

        ASSERT_EQUALS(prog.nFields(), 7) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT_EQUALS(prog["initialSyncEnd"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        BSONObj attempts = prog["initialSyncAttempts"].Obj();
        ASSERT_EQUALS(attempts.nFields(), 1) << attempts;
        auto attempt0 = attempts["0"].Obj();
        ASSERT_EQUALS(attempt0.nFields(), 6) << attempt0;
        ASSERT_EQUALS(attempt0["status"].str(), "OK") << attempts;
        ASSERT_EQUALS(attempt0["syncSource"].str(), "localhost:12345") << attempts;
        ASSERT_GREATER_THAN(attempt0.getIntField("durationMillis"), 0) << attempts;
    }
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
}

TEST_F(FileCopyBasedInitialSyncerTest, SyncingFilesUsingExtendedCursorsGetStats) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    auto hangBeforeExtendBackupCursor =
        globalFailPointRegistry().find("fCBISHangBeforeExtendBackupCursor");
    auto timesEnteredHangBeforeExtendBackupCursor =
        hangBeforeExtendBackupCursor->setMode(FailPoint::alwaysOn);

    auto hangAfterAttemptingExtendBackupCursor =
        globalFailPointRegistry().find("fCBISHangAfterAttemptingExtendBackupCursor");
    auto timesEnteredHangAfterAttemptingExtendBackupCursor =
        hangAfterAttemptingExtendBackupCursor->setMode(FailPoint::alwaysOn);

    auto hangBeforeFinish = globalFailPointRegistry().find("fCBISHangBeforeFinish");
    auto timesEnteredHangBeforeFinish = hangBeforeFinish->setMode(FailPoint::alwaysOn);

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    stdx::thread bgThread;
    // Run in another thread.
    bgThread = stdx::thread([&] {
        Client::initThread("Expector", getGlobalServiceContext()->getService());
        expectSuccessfulFileCloning();
        expectSuccessfulBackupCursorCall();
        // Force the lag to be greater than 'fileBasedInitialSyncMaxLagSec' to extend the
        // backupCursor.
        auto timeStamp = Timestamp(
            cursorDataMock.checkpointTimestamp.getSecs() + fileBasedInitialSyncMaxLagSec + 1, 0);
        expectSuccessfulReplSetGetStatusCall(timeStamp);
        expectSuccessfulExtendedBackupCursorCall(0, timeStamp);

        // Force the lag to be greater than 'fileBasedInitialSyncMaxLagSec' to extend the
        // backupCursor again.
        timeStamp = Timestamp(timeStamp.getSecs() + fileBasedInitialSyncMaxLagSec + 1, 0);
        expectSuccessfulReplSetGetStatusCall(timeStamp);
        expectSuccessfulExtendedBackupCursorCall(1, timeStamp);

        // Mark the initial sync as done as long as the lag is not greater than
        // 'fileBasedInitialSyncMaxLagSec'.
        timeStamp = Timestamp(timeStamp.getSecs() + fileBasedInitialSyncMaxLagSec, 0);
        expectSuccessfulReplSetGetStatusCall(timeStamp);

        ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getBackupCursorFiles_forTest(),
                                 cursorDataMock.backupCursorFiles));
        ASSERT(verifyCursorFiles(fileCopyBasedInitialSyncer->getExtendedBackupCursorFiles_forTest(),
                                 cursorDataMock.extendedCursorFiles));

        expectSuccessfulKillBackupCursorCall();
    });

    {
        hangBeforeExtendBackupCursor->waitForTimesEntered(timesEnteredHangBeforeExtendBackupCursor +
                                                          1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangBeforeExtendBackupCursor->setMode(FailPoint::off);

        ASSERT_EQUALS(prog.nFields(), 14) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 314752297) << prog;
        ASSERT(prog["approxTotalBytesCopied"].isNumber()) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog.getIntField("totalTimeUnreachableMillis"), 0) << prog;
        ASSERT_EQUALS(prog["syncSourceLastApplied"].type(), bsonTimestamp) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 10) << files;
    }

    {
        hangAfterAttemptingExtendBackupCursor->waitForTimesEntered(
            timesEnteredHangAfterAttemptingExtendBackupCursor + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangAfterAttemptingExtendBackupCursor->setMode(FailPoint::off);

        ASSERT_EQUALS(prog.nFields(), 14) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 314752297) << prog;
        ASSERT(prog["approxTotalBytesCopied"].isNumber()) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["syncSourceLastApplied"].type(), bsonTimestamp) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 10) << files;
    }
    {
        hangBeforeFinish->waitForTimesEntered(timesEnteredHangBeforeFinish + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangBeforeFinish->setMode(FailPoint::off);

        ASSERT_EQUALS(prog.nFields(), 14) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        BSONObj attempts = prog["initialSyncAttempts"].Obj();
        ASSERT_EQUALS(attempts.nFields(), 1) << attempts;
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 314752297) << prog;
        ASSERT(prog["approxTotalBytesCopied"].isNumber()) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 314752297) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["syncSourceLastApplied"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog.getIntField("totalTimeUnreachableMillis"), 0) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 14) << files;

        auto file0 = files["0"].Obj();
        ASSERT_EQUALS(file0.nFields(), 8) << files;
        ASSERT_EQUALS(file0["filePath"].str(), "WiredTiger") << prog;
        ASSERT_EQUALS(file0.getIntField("fileSize"), 47) << prog;
        ASSERT_EQUALS(file0.getIntField("bytesCopied"), 0) << prog;
        ASSERT_EQUALS(file0["start"].type(), Date) << prog;
        ASSERT_EQUALS(file0["end"].type(), Date) << prog;
        ASSERT_EQUALS(file0.getIntField("elapsedMillis"), 0) << prog;
        ASSERT_EQUALS(file0.getIntField("receivedBatches"), 1) << prog;
        ASSERT_EQUALS(file0.getIntField("writtenBatches"), 1) << prog;
    }


    bgThread.join();

    {
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();

        ASSERT_EQUALS(prog.nFields(), 7) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT_EQUALS(prog["initialSyncEnd"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        BSONObj attempts = prog["initialSyncAttempts"].Obj();
        ASSERT_EQUALS(attempts.nFields(), 1) << attempts;
        auto attempt0 = attempts["0"].Obj();
        ASSERT_EQUALS(attempt0.nFields(), 6) << attempt0;
        ASSERT_EQUALS(attempt0["status"].str(), "OK") << attempts;
        ASSERT_EQUALS(attempt0["syncSource"].str(), "localhost:12345") << attempts;
        ASSERT_GREATER_THAN(attempt0.getIntField("durationMillis"), 0) << attempts;
    }
    fileCopyBasedInitialSyncer->join();
    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
}

TEST_F(FileCopyBasedInitialSyncerTest, ClonesFilesFromInitialSourceGetStats) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    globalFailPointRegistry().find("fCBISSkipSyncingFilesPhase")->setMode(FailPoint::off);

    auto hangAfterBackupCursorFailPoint =
        globalFailPointRegistry().find("fCBISHangAfterOpeningBackupCursor");
    auto timesEnteredHangAfterBackupCursor =
        hangAfterBackupCursorFailPoint->setMode(FailPoint::alwaysOn);

    auto hangAfterFileCloningAsync =
        globalFailPointRegistry().find("fCBISHangAfterFileCloningAsync");
    auto timesEnteredHangAfterFileCloningAsync =
        hangAfterFileCloningAsync->setMode(FailPoint::alwaysOn);

    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    UUID backupId = UUID::gen();
    Timestamp checkpointTs(1, 1);
    _mock->defaultExpect(
        BSON("getMore" << 1 << "collection"
                       << "$cmd.aggregate"),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin), 1, {})
            .toBSON(CursorResponse::ResponseType::SubsequentResponse));
    _mock->defaultExpect(
        BSON("replSetGetStatus" << 1),
        BSON("optimes" << BSON("appliedOpTime" << OpTime(checkpointTs, 1)) << "ok" << 1));
    _mock->defaultExpect(BSON("killCursors"
                              << "$cmd.aggregate"),
                         BSON("ok" << 1));

    // The sync source satisfies requirements for FCBIS.
    expectSuccessfulSyncSourceValidation();

    std::string file1Data = "ABCDEF_DATA_FOR_FILE_1";
    std::string file2Data = "0123456789_DATA_FOR_FILE_2";
    _mock->expect(
        BSON("aggregate" << 1 << "pipeline" << BSON("$eq" << BSON("$backupCursor" << BSONObj()))),
        CursorResponse(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin),
                       1 /* cursorId */,
                       {BSON("metadata" << BSON("backupId" << backupId << "checkpointTimestamp"
                                                           << checkpointTs << "dbpath"
                                                           << "/path/to/dbpath")),
                        BSON("filename"
                             << "/path/to/dbpath/backupfile1"
                             << "fileSize" << int64_t(file1Data.size())),
                        BSON("filename"
                             << "/path/to/dbpath/backupfile2"
                             << "fileSize" << int64_t(file2Data.size()))})
            .toBSONAsInitialResponse());

    mockBackupFileData({file1Data, file2Data});
    expectSuccessfulReplSetGetStatusCall(
        Timestamp(cursorDataMock.checkpointTimestamp.getSecs() + fileBasedInitialSyncMaxLagSec, 0),
        false /* don't run, just set up the expectation */);

    // Runs until we hit the failpoint.
    _mock->runUntil(getNet()->now() + Milliseconds(1));

    {
        hangAfterBackupCursorFailPoint->waitForTimesEntered(timesEnteredHangAfterBackupCursor + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangAfterBackupCursorFailPoint->setMode(FailPoint::off);
        // Runs until we're no longer in the failpoint loop, which has a 100ms delay
        _mock->runUntil(getNet()->now() + Milliseconds(100));

        ASSERT_EQUALS(prog.nFields(), 12) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());
        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 48) << prog;
        ASSERT_EQUALS(prog.getIntField("approxTotalBytesCopied"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 48) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("files"), BSONObj());
    }


    {
        hangAfterFileCloningAsync->waitForTimesEntered(timesEnteredHangAfterFileCloningAsync + 1);
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();
        hangAfterFileCloningAsync->setMode(FailPoint::off);
        // Runs until we're no longer in the failpoint loop, which has a 100ms delay
        _mock->runUntil(getNet()->now() + Milliseconds(100));

        ASSERT_EQUALS(prog.nFields(), 14) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        ASSERT_BSONOBJ_EQ(prog.getObjectField("initialSyncAttempts"), BSONObj());

        ASSERT_EQUALS(prog.getIntField("approxTotalDataSize"), 48) << prog;
        ASSERT_EQUALS(prog.getIntField("approxTotalBytesCopied"), 48) << prog;
        ASSERT_EQUALS(prog.getIntField("remainingInitialSyncEstimatedMillis"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("initialBackupDataSize"), 48) << prog;
        ASSERT_EQUALS(prog["previousOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog["currentOplogEnd"].type(), bsonTimestamp) << prog;
        ASSERT_EQUALS(prog.getIntField("totalTimeUnreachableMillis"), 0) << prog;
        BSONObj files = prog["files"].Obj();
        ASSERT_EQUALS(files.nFields(), 2) << files;

        auto file0 = files["0"].Obj();
        ASSERT_EQUALS(file0.nFields(), 8) << files;
        ASSERT_EQUALS(file0["filePath"].str(), "backupfile1") << prog;
        ASSERT_EQUALS(file0.getIntField("fileSize"), 22) << prog;
        ASSERT_EQUALS(file0.getIntField("bytesCopied"), 22) << prog;
        ASSERT_EQUALS(file0["start"].type(), Date) << prog;
        ASSERT_EQUALS(file0["end"].type(), Date) << prog;
        ASSERT_EQUALS(file0.getIntField("elapsedMillis"), 0) << prog;
        ASSERT_EQUALS(file0.getIntField("receivedBatches"), 1) << prog;
        ASSERT_EQUALS(file0.getIntField("writtenBatches"), 1) << prog;

        auto file1 = files["1"].Obj();
        ASSERT_EQUALS(file1.nFields(), 8) << files;
        ASSERT_EQUALS(file1["filePath"].str(), "backupfile2") << prog;
        ASSERT_EQUALS(file1.getIntField("fileSize"), 26) << prog;
        ASSERT_EQUALS(file1.getIntField("bytesCopied"), 26) << prog;
        ASSERT_EQUALS(file1["start"].type(), Date) << prog;
        ASSERT_EQUALS(file1["end"].type(), Date) << prog;
        ASSERT_EQUALS(file1.getIntField("elapsedMillis"), 0) << prog;
        ASSERT_EQUALS(file1.getIntField("receivedBatches"), 1) << prog;
        ASSERT_EQUALS(file1.getIntField("writtenBatches"), 1) << prog;
    }

    _mock->runUntilExpectationsSatisfied();
    fileCopyBasedInitialSyncer->join();

    {
        BSONObj prog = fileCopyBasedInitialSyncer->getInitialSyncProgress();

        ASSERT_EQUALS(prog.nFields(), 7) << prog;
        ASSERT_EQUALS(prog["method"].str(), "fileCopyBased") << prog;
        ASSERT_EQUALS(prog.getIntField("failedInitialSyncAttempts"), 0) << prog;
        ASSERT_EQUALS(prog.getIntField("maxFailedInitialSyncAttempts"), maxAttempts) << prog;
        ASSERT_EQUALS(prog["initialSyncStart"].type(), Date) << prog;
        ASSERT_EQUALS(prog["initialSyncEnd"].type(), Date) << prog;
        ASSERT(prog["totalInitialSyncElapsedMillis"].isNumber()) << prog;
        BSONObj attempts = prog["initialSyncAttempts"].Obj();
        ASSERT_EQUALS(attempts.nFields(), 1) << attempts;
        auto attempt0 = attempts["0"].Obj();
        ASSERT_EQUALS(attempt0.nFields(), 6) << attempt0;
        ASSERT_EQUALS(attempt0["status"].str(), "OK") << attempts;
        ASSERT_EQUALS(attempt0["syncSource"].str(), "localhost:12345") << attempts;
        ASSERT_GREATER_THAN(attempt0.getIntField("durationMillis"), 0) << attempts;
    }
    checkFileData("backupfile1", file1Data);
    checkFileData("backupfile2", file2Data);

    ASSERT_EQ(fileCopyBasedInitialSyncer->getStartInitialSyncAttemptFutureStatus_forTest(),
              Status::OK());
    ASSERT_EQ(fileCopyBasedInitialSyncer->getSyncSource_forTest(), HostAndPort("localhost", 12345));
}

}  // namespace
}  // namespace mongo
