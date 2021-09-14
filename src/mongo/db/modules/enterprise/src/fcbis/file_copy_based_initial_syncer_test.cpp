/**
 *    Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include "file_copy_based_initial_syncer.h"
#include "mongo/db/client.h"
#include "mongo/db/index_builds_coordinator_mongod.h"
#include "mongo/db/query/getmore_command_gen.h"
#include "mongo/db/repl/collection_cloner.h"
#include "mongo/db/repl/data_replicator_external_state_mock.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_consistency_markers_mock.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/replication_recovery_mock.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/repl/sync_source_selector.h"
#include "mongo/db/repl/sync_source_selector_mock.h"
#include "mongo/db/repl/task_executor_mock.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/db/storage/storage_engine_mock.h"
#include "mongo/dbtests/mock/mock_dbclient_connection.h"
#include "mongo/dbtests/mock/mock_remote_db_server.h"
#include "mongo/executor/mock_network_fixture.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/executor/thread_pool_task_executor_test_fixture.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/mutex.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/concurrency/thread_pool.h"

namespace {

using namespace mongo;
using namespace mongo::repl;
using namespace mongo::test::mock;

using executor::NetworkInterfaceMock;
using executor::RemoteCommandRequest;
using executor::RemoteCommandResponse;
using LockGuard = stdx::lock_guard<Latch>;

struct CollectionCloneInfo {
    std::shared_ptr<CollectionMockStats> stats = std::make_shared<CollectionMockStats>();
    CollectionBulkLoaderMock* loader = nullptr;
    Status status{ErrorCodes::NotYetInitialized, ""};
};

class FileCopyBasedInitialSyncerTest : public executor::ThreadPoolExecutorTest,
                                       public SyncSourceSelector,
                                       public ScopedGlobalServiceContextForTest {
public:
    FileCopyBasedInitialSyncerTest() : _threadClient(getGlobalServiceContext()) {}

    executor::ThreadPoolMock::Options makeThreadPoolMockOptions() const override;

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
                                                  const OpTime& lastOpTimeFetched) override {
        return _syncSourceSelector->shouldChangeSyncSource(
            currentSource, replMetadata, oqMetadata, previousOpTimeFetched, lastOpTimeFetched);
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
    Mutex _storageInterfaceWorkDoneMutex =
        MONGO_MAKE_LATCH("FileCopyBasedInitialSyncerTest::_storageInterfaceWorkDoneMutex");
    StorageInterfaceResults _storageInterfaceWorkDone;

    void setUp() override {
        executor::ThreadPoolExecutorTest::setUp();

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
            _storageInterfaceWorkDone.droppedCollections.push_back(nss.ns());
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

        auto* service = getGlobalServiceContext();
        service->setFastClockSource(std::make_unique<ClockSourceMock>());
        service->setPreciseClockSource(std::make_unique<ClockSourceMock>());
        ThreadPool::Options dbThreadPoolOptions;
        dbThreadPoolOptions.poolName = "dbthread";
        dbThreadPoolOptions.minThreads = 1U;
        dbThreadPoolOptions.maxThreads = 1U;
        dbThreadPoolOptions.onCreateThread = [](const std::string& threadName) {
            Client::initThread(threadName.c_str());
        };
        _dbWorkThreadPool = std::make_unique<ThreadPool>(dbThreadPoolOptions);
        _dbWorkThreadPool->startup();

        // Required by CollectionCloner::listIndexesStage() and IndexBuildsCoordinator.
        service->setStorageEngine(std::make_unique<StorageEngineMock>());
        IndexBuildsCoordinator::set(service, std::make_unique<IndexBuildsCoordinatorMongod>());

        _target = HostAndPort{"localhost:12346"};
        _mockServer = std::make_unique<MockRemoteDBServer>(_target.toString());
        _mock = std::make_unique<MockNetwork>(getNet());

        reset();

        launchExecutorThread();

        _replicationProcess = std::make_unique<ReplicationProcess>(
            _storageInterface.get(),
            std::make_unique<ReplicationConsistencyMarkersMock>(),
            std::make_unique<ReplicationRecoveryMock>());

        _executorProxy = std::make_unique<TaskExecutorMock>(&getExecutor());

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

        ThreadPool::Options threadPoolOptions;
        threadPoolOptions.poolName = "replication";
        threadPoolOptions.minThreads = 1U;
        threadPoolOptions.maxThreads = 1U;
        threadPoolOptions.onCreateThread = [](const std::string& threadName) {
            Client::initThread(threadName.c_str());
        };

        auto dataReplicatorExternalState = std::make_unique<DataReplicatorExternalStateMock>();
        dataReplicatorExternalState->taskExecutor = _executorProxy;
        dataReplicatorExternalState->currentTerm = 1LL;
        dataReplicatorExternalState->lastCommittedOpTime = _myLastOpTime;
        {
            ReplSetConfig config(
                ReplSetConfig::parse(BSON("_id"
                                          << "myset"
                                          << "version" << 1 << "protocolVersion" << 1 << "members"
                                          << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                                   << "localhost:12345"))
                                          << "settings"
                                          << BSON("electionTimeoutMillis" << 10000))));
            dataReplicatorExternalState->replSetConfigResult = config;
        }
        _externalState = dataReplicatorExternalState.get();

        _lastApplied = getDetectableErrorStatus();
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
            // TODO (SERVER-59728): Set this flag accordingly.
            _fileCopyBasedInitialSyncer->skipSyncingFilesPhase_ForTest();
        } catch (...) {
            ASSERT_OK(exceptionToStatus());
        }
    }

    void tearDownExecutorThread() {
        if (_executorThreadShutdownComplete) {
            return;
        }
        getExecutor().shutdown();
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

    StatusWith<OpTimeAndWallTime> _lastApplied = Status(ErrorCodes::NotYetInitialized, "");
    InitialSyncerInterface::OnCompletionFn _onCompletion;

private:
    DataReplicatorExternalStateMock* _externalState;
    std::shared_ptr<FileCopyBasedInitialSyncer> _fileCopyBasedInitialSyncer;
    ThreadClient _threadClient;
    bool _executorThreadShutdownComplete = false;
};

executor::ThreadPoolMock::Options FileCopyBasedInitialSyncerTest::makeThreadPoolMockOptions()
    const {
    executor::ThreadPoolMock::Options options;
    options.onCreateThread = []() { Client::initThread("FileCopyBasedInitialSyncerTest"); };
    return options;
}

void advanceClock(NetworkInterfaceMock* net, Milliseconds duration) {
    executor::NetworkInterfaceMock::InNetworkGuard guard(net);
    auto when = net->now() + duration;
    ASSERT_EQUALS(when, net->runUntil(when));
}

ServiceContext::UniqueOperationContext makeOpCtx() {
    return cc().makeOperationContext();
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
    ASSERT_FALSE(fileCopyBasedInitialSyncer->isActive());
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
    ASSERT_TRUE(fileCopyBasedInitialSyncer->isActive());
    // SyncSourceSelector returns an invalid sync source so FileCopyBasedInitialSyncer is stuck
    // waiting for another sync source in 'Options::syncSourceRetryWait' ms.

    ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());
    ASSERT_EQUALS(ErrorCodes::ShutdownInProgress,
                  fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));
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

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));
    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    // This will cancel the _startInitialSyncAttempt() task started by startup().
    ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQUALS(ErrorCodes::CallbackCanceled, _lastApplied);
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
    BSONObjBuilder bob;
    bob.append("isWritablePrimary", true);
    bob.append("secondary", false);
    bob.append("maxWireVersion", static_cast<int>(WireVersion::WIRE_VERSION_51));
    bob.append("ok", 1);
    _mock->expect(BSON("hello" << 1), RemoteCommandResponse({bob.obj(), Milliseconds()})).times(1);

    _mock->runUntilExpectationsSatisfied();

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
    FCBISReturnsInitialSyncOplogSourceMissingIfNoValidSyncSourceCanBeFoundAfterTenFailedChooseSyncSourceAttempts) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    // Override chooseNewSyncSource() result in SyncSourceSelectorMock before calling startup()
    // because InitialSyncer will look for a valid sync source immediately after startup.
    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort());

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    _simulateChooseSyncSourceFailure(getNet(), _options.syncSourceRetryWait);

    fileCopyBasedInitialSyncer->join();
    ASSERT_EQUALS(ErrorCodes::InitialSyncOplogSourceMissing, _lastApplied);
}


TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceIsNotPrimaryOrSecondary) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        // The sync source is not a primary or secondary.
        BSONObjBuilder bob;
        bob.append("isWritablePrimary", false);
        bob.append("secondary", false);
        bob.append("maxWireVersion", static_cast<int>(WireVersion::WIRE_VERSION_51));
        bob.append("ok", 1);
        _mock->expect(BSON("hello" << 1), RemoteCommandResponse({bob.obj(), Milliseconds()}))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
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

TEST_F(FileCopyBasedInitialSyncerTest, FCBISRetriesIfSyncSourceHasInvalidWireVersion_Mock) {
    auto* fileCopyBasedInitialSyncer = getFileCopyBasedInitialSyncer();
    auto opCtx = makeOpCtx();

    _syncSourceSelector->setChooseNewSyncSourceResult_forTest(HostAndPort("localhost", 12345));

    ASSERT_OK(fileCopyBasedInitialSyncer->startup(opCtx.get(), maxAttempts));

    for (std::uint32_t i = 0; i < chooseSyncSourceMaxAttempts; ++i) {
        // The sync source does not have a valid wire version for FCBIS.
        BSONObjBuilder bob;
        bob.append("isWritablePrimary", false);
        bob.append("secondary", true);
        bob.append("maxWireVersion", static_cast<int>(WireVersion::WIRE_VERSION_50));
        bob.append("ok", 1);
        _mock->expect(BSON("hello" << 1), RemoteCommandResponse({bob.obj(), Milliseconds()}))
            .times(1);
        _mock->runUntilExpectationsSatisfied();
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

    ASSERT_EQUALS(ErrorCodes::InitialSyncOplogSourceMissing, _lastApplied);
}

TEST_F(FileCopyBasedInitialSyncerTest,
       FCBISReturnsCallbackCanceledIfShutdownWhileRetryingSyncSourceSelection) {
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

    // This will cancel the _chooseSyncSourceCallback() task scheduled at getNet()->now() +
    // '_options.syncSourceRetryWait'
    ASSERT_OK(fileCopyBasedInitialSyncer->shutdown());

    fileCopyBasedInitialSyncer->join();

    ASSERT_EQUALS(ErrorCodes::CallbackCanceled, _lastApplied);
}
}  // namespace
