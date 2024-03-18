/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 */

#include "mongoqd_main.h"

#include <boost/optional.hpp>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/status.h"
#include "mongo/client/connpool.h"
#include "mongo/client/dbclient_rs.h"
#include "mongo/client/global_conn_pool.h"
#include "mongo/client/remote_command_targeter_factory_impl.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/config.h"
#include "mongo/db/audit.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authz_manager_external_state_s.h"
#include "mongo/db/auth/user_cache_invalidator_job.h"
#include "mongo/db/client.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/ftdc/ftdc_mongos.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/keys_collection_client_sharded.h"
#include "mongo/db/log_process_details.h"
#include "mongo/db/logical_time_validator.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/process_health/fault_manager.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/kill_sessions.h"
#include "mongo/db/session/kill_sessions_remote.h"
#include "mongo/db/session/logical_session_cache_impl.h"
#include "mongo/db/session/service_liaison_impl.h"
#include "mongo/db/session/service_liaison_router.h"
#include "mongo/db/session/session_killer.h"
#include "mongo/db/startup_warnings_common.h"
#include "mongo/db/wire_version.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/process_id.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/client/shard_factory.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/client/shard_remote.h"
#include "mongo/s/client/sharding_connection_hook.h"
#include "mongo/s/client_transport_observer_mongos.h"
#include "mongo/s/config_server_catalog_cache_loader.h"
#include "mongo/s/grid.h"
#include "mongo/s/load_balancer_support.h"
#include "mongo/s/mongos_server_parameters_gen.h"
#include "mongo/s/mongos_topology_coordinator.h"
#include "mongo/s/query/cluster_cursor_cleanup_job.h"
#include "mongo/s/query/cluster_cursor_manager.h"
#include "mongo/s/resource_yielders.h"
#include "mongo/s/router_uptime_reporter.h"
#include "mongo/s/service_entry_point_mongos.h"
#include "mongo/s/session_catalog_router.h"
#include "mongo/s/sessions_collection_sharded.h"
#include "mongo/s/sharding_initialization.h"
#include "mongo/s/sharding_state.h"
#include "mongo/s/transaction_router.h"
#include "mongo/scripting/dbdirectclient_factory.h"
#include "mongo/scripting/engine.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/service_executor.h"
#include "mongo/transport/session_manager_common.h"
#include "mongo/transport/transport_layer_manager_impl.h"
#include "mongo/util/admin_access.h"
#include "mongo/util/cmdline_utils/censor_cmdline.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/debugger.h"
#include "mongo/util/exception_filter_win32.h"
#include "mongo/util/exit.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/fast_clock_source_factory.h"
#include "mongo/util/latch_analyzer.h"
#include "mongo/util/net/ocsp/ocsp_manager.h"
#include "mongo/util/net/private/ssl_expiration.h"
#include "mongo/util/net/socket_exception.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/periodic_runner.h"
#include "mongo/util/periodic_runner_factory.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"
#include "mongoqd_options.h"
#include "read_write_concern_defaults_cache_lookup_mongoqd.h"
#include "version_mongoqd.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {
namespace {

using logv2::LogComponent;

// Failpoint for disabling replicaSetChangeConfigServerUpdateHook calls on signaled mongoqd.
MONGO_FAIL_POINT_DEFINE(failReplicaSetChangeConfigServerUpdateHook);
MONGO_FAIL_POINT_DEFINE(pauseWhileKillingOperationsAtShutdown);

#if defined(_WIN32)
const ntservice::NtServiceDefaultStrings defaultServiceStrings = {
    L"MongoQD", L"MongoDB Serverless Router", L"MongoDB Serverless Router"};
#endif

constexpr auto kSignKeysRetryInterval = Seconds{1};

Status waitForSigningKeys(OperationContext* opCtx) {
    auto const shardRegistry = Grid::get(opCtx)->shardRegistry();

    while (true) {
        auto configCS = shardRegistry->getConfigServerConnectionString();
        auto rsm = ReplicaSetMonitor::get(configCS.getSetName());
        // mongod will set minWireVersion == maxWireVersion for hello requests from
        // internalClient.
        if (rsm && (rsm->getMaxWireVersion() < WireVersion::SUPPORTS_OP_MSG)) {
            LOGV2(6184429, "Waiting for signing keys not supported by config shard");
            return Status::OK();
        }
        auto stopStatus = opCtx->checkForInterruptNoAssert();
        if (!stopStatus.isOK()) {
            return stopStatus;
        }

        try {
            if (LogicalTimeValidator::get(opCtx)->shouldGossipLogicalTime()) {
                return Status::OK();
            }
            LOGV2(6184428,
                  "Waiting for signing keys, sleeping before checking again",
                  "signingKeysCheckInterval"_attr = Seconds(kSignKeysRetryInterval));
            sleepFor(kSignKeysRetryInterval);
            continue;
        } catch (const DBException& ex) {
            LOGV2_WARNING(6184427,
                          "Error while waiting for signing keys, sleeping before checking again",
                          "signingKeysCheckInterval"_attr = Seconds(kSignKeysRetryInterval),
                          "error"_attr = ex);
            sleepFor(kSignKeysRetryInterval);
            continue;
        }
    }
}

/**
 * Abort all active transactions in the catalog that has not yet been committed.
 *
 * Outline:
 * 1. Mark all sessions as killed and collect killTokens from each session.
 * 2. Create a new Client in order not to pollute the current OperationContext.
 * 3. Create new OperationContexts for each session to be killed and perform the necessary setup
 *    to be able to abort transactions properly: like setting TxnNumber and attaching the session
 *    to the OperationContext.
 * 4. Send abortTransaction.
 */
void implicitlyAbortAllTransactions(OperationContext* opCtx) {
    struct AbortTransactionDetails {
    public:
        AbortTransactionDetails(LogicalSessionId _lsid, SessionCatalog::KillToken _killToken)
            : lsid(std::move(_lsid)), killToken(std::move(_killToken)) {}

        LogicalSessionId lsid;
        SessionCatalog::KillToken killToken;
    };

    const auto catalog = SessionCatalog::get(opCtx);

    SessionKiller::Matcher matcherAllSessions(
        KillAllSessionsByPatternSet{makeKillAllSessionsByPattern(opCtx)});

    const auto abortDeadline =
        opCtx->getServiceContext()->getFastClockSource()->now() + Seconds(15);

    std::vector<AbortTransactionDetails> toKill;
    catalog->scanSessions(matcherAllSessions, [&](const ObservableSession& session) {
        toKill.emplace_back(session.getSessionId(),
                            session.kill(ErrorCodes::InterruptedAtShutdown));
    });

    auto newClient =
        opCtx->getServiceContext()->getService()->makeClient("ImplicitlyAbortTxnAtShutdown");

    {
        stdx::lock_guard<Client> lk(*newClient.get());
        newClient.get()->setSystemOperationUnkillableByStepdown(lk);
    }

    AlternativeClientRegion acr(newClient);

    Status shutDownStatus(ErrorCodes::InterruptedAtShutdown,
                          "aborting transactions due to shutdown");

    for (auto& killDetails : toKill) {
        auto uniqueNewOpCtx = cc().makeOperationContext();
        auto newOpCtx = uniqueNewOpCtx.get();

        newOpCtx->setDeadlineByDate(abortDeadline, ErrorCodes::ExceededTimeLimit);

        OperationContextSession sessionCtx(newOpCtx, std::move(killDetails.killToken));

        auto session = OperationContextSession::get(newOpCtx);
        newOpCtx->setLogicalSessionId(session->getSessionId());

        auto txnRouter = TransactionRouter::get(newOpCtx);
        if (txnRouter.isInitialized()) {
            txnRouter.implicitlyAbortTransaction(newOpCtx, shutDownStatus);
        }
    }
}

/**
 * NOTE: This function may be called at any time after registerShutdownTask is called below. It must
 * not depend on the prior execution of mongo initializers or the existence of threads.
 */
void cleanupTask(const ShutdownTaskArgs& shutdownArgs) {
    const auto serviceContext = getGlobalServiceContext();
    {
        // This client initiation pattern is only to be used here, with plans to eliminate this
        // pattern down the line.
        if (!haveClient()) {
            Client::initThread(getThreadName(), getGlobalServiceContext()->getService());

            stdx::lock_guard<Client> lk(cc());
            cc().setSystemOperationUnkillableByStepdown(lk);
        }
        Client& client = cc();

        ServiceContext::UniqueOperationContext uniqueTxn;
        OperationContext* opCtx = client.getOperationContext();
        if (!opCtx) {
            uniqueTxn = client.makeOperationContext();
            opCtx = uniqueTxn.get();
        }

        Milliseconds quiesceTime;
        if (shutdownArgs.quiesceTime) {
            quiesceTime = *shutdownArgs.quiesceTime;
        } else {
            // IDL gaurantees that quiesceTime is populated.
            invariant(!shutdownArgs.isUserInitiated);
            quiesceTime = Milliseconds(mongosShutdownTimeoutMillisForSignaledShutdown.load());
        }

        if (auto mongosTopCoord = MongosTopologyCoordinator::get(opCtx)) {
            mongosTopCoord->enterQuiesceModeAndWait(opCtx, quiesceTime);
        }

        // Shutdown the TransportLayer so that new connections aren't accepted
        if (auto tl = serviceContext->getTransportLayerManager()) {
            LOGV2_OPTIONS(
                6184426, {LogComponent::kNetwork}, "shutdown: going to close all sockets...");

            tl->shutdown();
        }

        try {
            // Abort transactions while we can still send remote commands.
            implicitlyAbortAllTransactions(opCtx);
        } catch (const DBException& excep) {
            LOGV2_WARNING(6184425, "Error aborting all active transactions", "error"_attr = excep);
        }

        if (auto lsc = LogicalSessionCache::get(serviceContext)) {
            lsc->joinOnShutDown();
        }

        ReplicaSetMonitor::shutdown();

        opCtx->setIsExecutingShutdown();

        if (serviceContext) {
            serviceContext->setKillAllOperations();

            if (MONGO_unlikely(pauseWhileKillingOperationsAtShutdown.shouldFail())) {
                LOGV2(6184424, "pauseWhileKillingOperationsAtShutdown failpoint enabled");
                sleepsecs(1);
            }
        }

        // Perform all shutdown operations after setKillAllOperations is called in order to ensure
        // that any pending threads are about to terminate

        if (auto validator = LogicalTimeValidator::get(serviceContext)) {
            validator->shutDown();
        }

        if (auto cursorManager = Grid::get(opCtx)->getCursorManager()) {
            cursorManager->shutdown(opCtx);
        }

        if (auto pool = Grid::get(opCtx)->getExecutorPool()) {
            pool->shutdownAndJoin();
        }

        if (auto shardRegistry = Grid::get(opCtx)->shardRegistry()) {
            shardRegistry->shutdown();
        }

        if (Grid::get(serviceContext)->isShardingInitialized()) {
            CatalogCacheLoader::get(serviceContext).shutDown();
        }

        // Shutdown Full-Time Data Capture
        stopMongoSFTDC();
    }

    audit::logShutdown(Client::getCurrent());

#ifndef MONGO_CONFIG_USE_RAW_LATCHES
    LatchAnalyzer::get(serviceContext).dump();
#endif

#ifdef MONGO_CONFIG_SSL
    OCSPManager::shutdown(serviceContext);
#endif
}

Status initializeSharding(OperationContext* opCtx) {
    auto targeterFactory = std::make_unique<RemoteCommandTargeterFactoryImpl>();
    auto targeterFactoryPtr = targeterFactory.get();

    ShardFactory::BuilderCallable setBuilder = [targeterFactoryPtr](
                                                   const ShardId& shardId,
                                                   const ConnectionString& connStr) {
        return std::make_unique<ShardRemote>(shardId, connStr, targeterFactoryPtr->create(connStr));
    };

    ShardFactory::BuilderCallable masterBuilder = [targeterFactoryPtr](
                                                      const ShardId& shardId,
                                                      const ConnectionString& connStr) {
        return std::make_unique<ShardRemote>(shardId, connStr, targeterFactoryPtr->create(connStr));
    };

    ShardFactory::BuildersMap buildersMap{
        {ConnectionString::ConnectionType::kReplicaSet, std::move(setBuilder)},
        {ConnectionString::ConnectionType::kStandalone, std::move(masterBuilder)},
    };

    auto shardFactory =
        std::make_unique<ShardFactory>(std::move(buildersMap), std::move(targeterFactory));

    CatalogCacheLoader::set(opCtx->getServiceContext(),
                            std::make_unique<ConfigServerCatalogCacheLoader>());

    auto catalogCache =
        std::make_unique<CatalogCache>(opCtx->getServiceContext(), CatalogCacheLoader::get(opCtx));

    // List of hooks which will be called by the ShardRegistry when it discovers a shard has been
    // removed.
    std::vector<ShardRegistry::ShardRemovalHook> shardRemovalHooks = {
        // Invalidate appropriate entries in the catalog cache when a shard is removed. It's safe to
        // capture the catalog cache pointer since the Grid (and therefore CatalogCache and
        // ShardRegistry) are never destroyed.
        [catCache = catalogCache.get()](const ShardId& removedShard) {
            catCache->invalidateEntriesThatReferenceShard(removedShard);
        }};

    if (!mongoqdGlobalParams.configdbs) {
        return {ErrorCodes::BadValue, "Unrecognized connection string."};
    }

    auto shardRegistry = std::make_unique<ShardRegistry>(opCtx->getServiceContext(),
                                                         std::move(shardFactory),
                                                         mongoqdGlobalParams.configdbs,
                                                         std::move(shardRemovalHooks));

    Status status = initializeGlobalShardingState(
        opCtx,
        std::move(catalogCache),
        std::move(shardRegistry),
        [service = opCtx->getServiceContext()] { return makeShardingEgressHooksList(service); },
        boost::none,
        [](ShardingCatalogClient* catalogClient) {
            return std::make_unique<KeysCollectionClientSharded>(catalogClient);
        });

    if (!status.isOK()) {
        return status;
    }

    status = loadGlobalSettingsFromConfigServer(opCtx, Grid::get(opCtx)->catalogClient());
    if (!status.isOK()) {
        return status;
    }

    status = waitForSigningKeys(opCtx);
    if (!status.isOK()) {
        return status;
    }

    // Loading of routing information may fail. Since this is just an optimization (warmup), any
    // failure must not prevent mongoqd from starting.
    try {
        preCacheMongosRoutingInfo(opCtx);
    } catch (const DBException& ex) {
        LOGV2_WARNING(6203602, "Failed to warmup routing information", "error"_attr = redact(ex));
    }

    status = preWarmConnectionPool(opCtx);
    if (!status.isOK()) {
        return status;
    }

    Grid::get(opCtx)->setShardingInitialized();

    return Status::OK();
}

namespace {
ServiceContext::ConstructorActionRegisterer registerWireSpec{
    "RegisterWireSpec", [](ServiceContext* service) {
        // Since the upgrade order calls for upgrading mongoqd last, it only needs to talk the
        // latest wire version. This ensures that users will get errors if they upgrade in the wrong
        // order.
        WireSpec::Specification spec;
        spec.outgoing.minWireVersion = LATEST_WIRE_VERSION;
        spec.outgoing.maxWireVersion = LATEST_WIRE_VERSION;
        spec.isInternalClient = true;

        WireSpec::getWireSpec(service).initialize(std::move(spec));
    }};
}  // namespace

class ShardingReplicaSetChangeListener final
    : public ReplicaSetChangeNotifier::Listener,
      public std::enable_shared_from_this<ShardingReplicaSetChangeListener> {
public:
    ShardingReplicaSetChangeListener(ServiceContext* serviceContext)
        : _serviceContext(serviceContext) {}
    ~ShardingReplicaSetChangeListener() final = default;

    void onFoundSet(const Key& key) noexcept final {}

    void onConfirmedSet(const State& state) noexcept final {
        auto connStr = state.connStr;
        try {
            LOGV2(6184422,
                  "Updating the shard registry with confirmed replica set",
                  "connectionString"_attr = connStr);
            Grid::get(_serviceContext)
                ->shardRegistry()
                ->updateReplSetHosts(connStr,
                                     ShardRegistry::ConnectionStringUpdateType::kConfirmed);
        } catch (const ExceptionForCat<ErrorCategory::ShutdownError>& e) {
            LOGV2(6184421,
                  "Unable to update the shard registry with confirmed replica set",
                  "error"_attr = e);
        }
    }

    void onPossibleSet(const State& state) noexcept final {
        try {
            Grid::get(_serviceContext)
                ->shardRegistry()
                ->updateReplSetHosts(state.connStr,
                                     ShardRegistry::ConnectionStringUpdateType::kPossible);
        } catch (const DBException& ex) {
            LOGV2_DEBUG(6184420,
                        2,
                        "Unable to update sharding state with possible replica set",
                        "error"_attr = ex);
        }
    }

    void onDroppedSet(const Key& key) noexcept final {}

private:
    ServiceContext* _serviceContext;
};

ExitCode runMongoqdServer(ServiceContext* serviceContext) {
    ThreadClient tc("mongoqdMain", serviceContext->getService());

    {
        stdx::lock_guard<Client> lk(*tc.get());
        tc.get()->setSystemOperationUnkillableByStepdown(lk);
    }

    logMongoqdVersionInfo(nullptr);

    // Set up the periodic runner for background job execution
    {
        auto runner = makePeriodicRunner(serviceContext);
        serviceContext->setPeriodicRunner(std::move(runner));
    }

#ifdef MONGO_CONFIG_SSL
    OCSPManager::start(serviceContext);
    CertificateExpirationMonitor::get()->start(serviceContext);
#endif

    ShardingState::create(serviceContext);
    serviceContext->getService()->setServiceEntryPoint(std::make_unique<ServiceEntryPointMongos>());

    {
        const auto loadBalancerPort = load_balancer_support::getLoadBalancerPort();
        if (loadBalancerPort && *loadBalancerPort == serverGlobalParams.port) {
            LOGV2_ERROR(6184416,
                        "Load balancer port must be different from the normal ingress port.",
                        "port"_attr = serverGlobalParams.port);
            quickExit(ExitCode::badOptions);
        }

        auto tl = transport::TransportLayerManagerImpl::createWithConfig(
            &serverGlobalParams,
            serviceContext,
            loadBalancerPort,
            boost::none,
            std::make_unique<ClientTransportObserverMongos>());
        if (auto res = tl->setup(); !res.isOK()) {
            LOGV2_ERROR(6184415, "Error setting up listener", "error"_attr = res);
            return ExitCode::netError;
        }
        serviceContext->setTransportLayerManager(std::move(tl));
    }

    // Add sharding hooks to both connection pools - ShardingConnectionHook includes auth hooks
    globalConnPool.addHook(new ShardingConnectionHook(makeShardingEgressHooksList(serviceContext)));

    // Hook up a Listener for changes from the ReplicaSetMonitor
    // This will last for the scope of this function. i.e. until shutdown finishes
    auto shardingRSCL =
        ReplicaSetMonitor::getNotifier().makeListener<ShardingReplicaSetChangeListener>(
            serviceContext);

    // Mongoqd connection pools already takes care of authenticating new connections so the
    // replica set connection shouldn't need to.
    DBClientReplicaSet::setAuthPooledSecondaryConn(false);

    if (getHostName().empty()) {
        quickExit(ExitCode::badOptions);
    }

    ResourceYielderFactory::initialize(serviceContext);

    ReadWriteConcernDefaults::create(serviceContext, readWriteConcernDefaultsCacheLookupMongoQD);

    auto opCtxHolder = tc->makeOperationContext();
    auto const opCtx = opCtxHolder.get();

    try {
        uassertStatusOK(initializeSharding(opCtx));
    } catch (const DBException& ex) {
        if (ex.code() == ErrorCodes::CallbackCanceled) {
            invariant(globalInShutdownDeprecated());
            LOGV2(6184414, "Shutdown called before mongoqd finished starting up");
            return ExitCode::clean;
        }

        LOGV2_ERROR(6184413, "Error initializing sharding system", "error"_attr = redact(ex));
        return ExitCode::shardingError;
    }

    Grid::get(serviceContext)
        ->getBalancerConfiguration()
        ->refreshAndCheck(opCtx)
        .transitional_ignore();

    try {
        ReadWriteConcernDefaults::get(serviceContext).refreshIfNecessary(opCtx);
    } catch (const DBException& ex) {
        LOGV2_WARNING(6184412,
                      "Error loading read and write concern defaults at startup",
                      "error"_attr = redact(ex));
    }

    startMongoSFTDC(serviceContext);

    if (mongoqdGlobalParams.scriptingEnabled) {
        ScriptEngine::setup(ExecutionEnvironment::Server);
    }

    Status status = AuthorizationManager::get(serviceContext->getService())->initialize(opCtx);
    if (!status.isOK()) {
        LOGV2_ERROR(6184411, "Error initializing authorization data", "error"_attr = status);
        return ExitCode::shardingError;
    }

    // Construct the router uptime reporter after the startup parameters have been parsed in order
    // to ensure that it picks up the server port instead of reporting the default value.
    RouterUptimeReporter::get(serviceContext).startPeriodicThread(serviceContext);

    clusterCursorCleanupJob.go();

    UserCacheInvalidator::start(serviceContext, opCtx);
    if (audit::initializeSynchronizeJob) {
        audit::initializeSynchronizeJob(serviceContext);
    }

    PeriodicTask::startRunningPeriodicTasks();

    status =
        process_health::FaultManager::get(serviceContext)->startPeriodicHealthChecks().getNoThrow();
    if (!status.isOK()) {
        LOGV2_ERROR(
            6184430, "Error completing initial health check", "error"_attr = redact(status));
        return ExitCode::processHealthCheck;
    }

    SessionKiller::set(
        serviceContext->getService(ClusterRole::RouterServer),
        std::make_shared<SessionKiller>(serviceContext->getService(ClusterRole::RouterServer),
                                        killSessionsRemote));

    LogicalSessionCache::set(
        serviceContext,
        std::make_unique<LogicalSessionCacheImpl>(
            std::make_unique<ServiceLiaisonImpl>(
                service_liaison_router_callbacks::getOpenCursorSessions,
                service_liaison_router_callbacks::killCursorsWithMatchingSessions),
            std::make_unique<SessionsCollectionSharded>(),
            RouterSessionCatalog::reapSessionsOlderThan));

    transport::ServiceExecutor::startupAll(serviceContext);

    status = serviceContext->getTransportLayerManager()->start();
    if (!status.isOK()) {
        LOGV2_ERROR(6184409, "Error starting transport layer", "error"_attr = redact(status));
        return ExitCode::netError;
    }

    if (!initialize_server_global_state::writePidFile()) {
        return ExitCode::abrupt;
    }

    // Startup options are written to the audit log at the end of startup so that cluster server
    // parameters are guaranteed to have been initialized from disk at this point.
    audit::logStartupOptions(tc.get(), serverGlobalParams.parsedOpts);

    serviceContext->notifyStorageStartupRecoveryComplete();

#if !defined(_WIN32)
    initialize_server_global_state::signalForkSuccess();
#else
    if (ntservice::shouldStartService()) {
        ntservice::reportStatus(SERVICE_RUNNING);
        LOGV2(6184408, "Service running");
    }
#endif

    // Block until shutdown.
    MONGO_IDLE_THREAD_BLOCK;
    return waitForShutdown();
}

#if defined(_WIN32)
ExitCode initService() {
    return runMongoqdServer(getGlobalServiceContext());
}
#endif

/**
 * This function should contain the startup "actions" that we take based on the startup config. It
 * is intended to separate the actions from "storage" and "validation" of our startup configuration.
 */
void startupConfigActions(const std::vector<std::string>& argv) {
#if defined(_WIN32)
    std::vector<std::string> disallowedOptions;
    disallowedOptions.push_back("upgrade");
    ntservice::configureService(
        initService, moe::startupOptionsParsed, defaultServiceStrings, disallowedOptions, argv);
#endif
}

std::unique_ptr<AuthzManagerExternalState> createAuthzManagerExternalStateMongos() {
    return std::make_unique<AuthzManagerExternalStateMongos>();
}

ExitCode main(ServiceContext* serviceContext) {
    serviceContext->setFastClockSource(FastClockSourceFactory::create(Milliseconds{10}));

    // We either have a setting where all processes are in localhost or none are
    const auto& configServers = mongoqdGlobalParams.configdbs.getServers();
    invariant(!configServers.empty());
    const auto allowLocalHost = configServers.front().isLocalHost();

    for (const auto& configServer : configServers) {
        if (configServer.isLocalHost() != allowLocalHost) {
            LOGV2_OPTIONS(6184407,
                          {LogComponent::kDefault},
                          "cannot mix localhost and ip addresses in configdbs");
            return ExitCode::badOptions;
        }
    }

#if defined(_WIN32)
    if (ntservice::shouldStartService()) {
        ntservice::startService();
        // If we reach here, then we are not running as a service. Service installation exits
        // directly and so never reaches here either.
    }
#endif

    return runMongoqdServer(serviceContext);
}

MONGO_INITIALIZER_GENERAL(ForkServer, ("EndStartupOptionHandling"), ("default"))
(InitializerContext* context) {
    initialize_server_global_state::forkServerOrDie();
}

// Initialize the featureCompatibilityVersion server parameter since mongoqd does not have a
// featureCompatibilityVersion document from which to initialize the parameter. The parameter is set
// to the latest version because there is no feature gating that currently occurs at the mongoqd
// level. The shards are responsible for rejecting usages of new features if their
// featureCompatibilityVersion is lower.
MONGO_INITIALIZER_WITH_PREREQUISITES(SetFeatureCompatibilityVersionLatest,
                                     ("EndStartupOptionStorage"))
// (Generic FCV reference): This FCV reference should exist across LTS binary versions.
(InitializerContext* context) {
    serverGlobalParams.mutableFCV.setVersion(multiversion::GenericFCV::kLatest);
}

#ifdef MONGO_CONFIG_SSL
MONGO_INITIALIZER_GENERAL(setSSLManagerType, (), ("SSLManager"))
(InitializerContext* context) {
    isSSLServer = true;
}
#endif

}  // namespace

ExitCode mongoqd_main(int argc, char* argv[]) {
    serverGlobalParams.clusterRole = ClusterRole::RouterServer;

    if (argc < 1)
        return ExitCode::badOptions;

    waitForDebugger();

    setupSignalHandlers();

    Status status = runGlobalInitializers(std::vector<std::string>(argv, argv + argc));
    if (!status.isOK()) {
        LOGV2_FATAL_OPTIONS(
            6184406,
            logv2::LogOptions(logv2::LogComponent::kDefault, logv2::FatalMode::kContinue),
            "Error during global initialization",
            "error"_attr = status);
        return ExitCode::abrupt;
    }

    try {
        setGlobalServiceContext(ServiceContext::make());
    } catch (...) {
        auto cause = exceptionToStatus();
        LOGV2_FATAL_OPTIONS(
            6184405,
            logv2::LogOptions(logv2::LogComponent::kDefault, logv2::FatalMode::kContinue),
            "Error creating service context",
            "error"_attr = redact(cause));
        return ExitCode::abrupt;
    }

    const auto service = getGlobalServiceContext();

    if (audit::setAuditInterface) {
        audit::setAuditInterface(service);
    }

    audit::rotateAuditLog();
    registerShutdownTask(cleanupTask);

    ErrorExtraInfo::invariantHaveAllParsers();

    startupConfigActions(std::vector<std::string>(argv, argv + argc));
    cmdline_utils::censorArgvArray(argc, argv);

    logCommonStartupWarnings(serverGlobalParams);

    try {
        if (!initialize_server_global_state::checkSocketPath())
            return ExitCode::abrupt;

        startSignalProcessingThread();

        return main(service);
    } catch (const DBException& e) {
        LOGV2_ERROR(6184404, "uncaught DBException in mongoqd main", "error"_attr = redact(e));
        return ExitCode::uncaught;
    } catch (const std::exception& e) {
        LOGV2_ERROR(
            6184403, "uncaught std::exception in mongoqd main", "error"_attr = redact(e.what()));
        return ExitCode::uncaught;
    } catch (...) {
        LOGV2_ERROR(6184402, "uncaught unknown exception in mongoqd main");
        return ExitCode::uncaught;
    }
}

}  // namespace mongo
