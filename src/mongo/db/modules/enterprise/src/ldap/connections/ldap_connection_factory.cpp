/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kConnectionPool

#include "mongo/platform/basic.h"

#include "ldap_connection_factory.h"

#include <memory>

#include "mongo/base/status_with.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/executor/connection_pool.h"
#include "mongo/executor/connection_pool_stats.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/alarm.h"
#include "mongo/util/alarm_runner_background_thread.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/functional.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/strong_weak_finish_line.h"
#include "mongo/util/system_clock_source.h"
#include "mongo/util/timer.h"

#include "../ldap_connection_options.h"
#include "../ldap_manager.h"
#include "../ldap_options.h"
#include "../ldap_query.h"
#include "ldap/ldap_parameters_gen.h"
#ifndef _WIN32
#include "openldap_connection.h"
#else
#include "windows_ldap_connection.h"
#endif

namespace mongo {
namespace {
using namespace executor;

class LDAPHostTimingData {
public:
    void markFailed() {
        stdx::lock_guard<Latch> lk(_mutex);
        _failed = true;
    }

    void updateLatency(Milliseconds millis) {
        stdx::lock_guard<Latch> lk(_mutex);
        if (_failed) {
            _latency = millis;
            _failed = false;
        } else {
            // This calculates a moving average of the round trip time - this formula was taken from
            // https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#calculation-of-average-round-trip-times
            constexpr double alpha = 0.2;
            double newLatency = alpha * millis.count() + (1 - alpha) * _latency.count();
            _latency = Milliseconds(static_cast<int64_t>(std::nearbyint(newLatency)));
        }
    }

    Milliseconds getLatency() const {
        stdx::lock_guard<Latch> lk(_mutex);
        return _failed ? Milliseconds::max() : _latency;
    }

    AtomicWord<int64_t>& uses() {
        return _uses;
    }

private:
    mutable Mutex _mutex = MONGO_MAKE_LATCH("LDAPHostTimingData::_mutex");
    Milliseconds _latency{0};
    bool _failed = true;
    AtomicWord<int64_t> _uses{0};
};

struct LDAPPoolTimingData {
    Mutex mutex = MONGO_MAKE_LATCH("LDAPPoolTimingData::mutex");
    stdx::unordered_map<HostAndPort, std::shared_ptr<LDAPHostTimingData>> timingData;
};

std::unique_ptr<LDAPConnection> makeNativeLDAPConn(const LDAPConnectionOptions& opts) {
#ifndef _WIN32
    return std::make_unique<OpenLDAPConnection>(opts);
#else
    return std::make_unique<WindowsLDAPConnection>(opts);
#endif
}

// OpenLDAP can be compiled in a non-threadsafe manner, so we call into it to check for that.
// Windows is always thread-safe.
static inline bool isNativeImplThreadSafe() {
#ifndef _WIN32
    return OpenLDAPConnection::isThreadSafe();
#else
    return true;
#endif
}

ConnectionPool::Options makePoolOptions(Milliseconds timeout) {
    ConnectionPool::Options opts;
    opts.refreshTimeout = timeout;
    opts.minConnections = ldapConnectionPoolMinimumConnectionsPerHost;
    opts.maxConnections = ldapConnectionPoolMaximumConnectionsPerHost;
    opts.maxConnecting = ldapConnectionPoolMaximumConnectionsInProgressPerHost;
    opts.refreshRequirement = Milliseconds(ldapConnectionPoolRefreshInterval);
    opts.hostTimeout = Seconds(ldapConnectionPoolIdleHostTimeoutSecs);
    return opts;
}

/*
 * This implements the timer interface for the ConnectionPool.
 * Timers will be expired in order on a single background thread.
 */
class LDAPTimer : public ConnectionPool::TimerInterface {
public:
    explicit LDAPTimer(ClockSource* clockSource, std::shared_ptr<AlarmScheduler> scheduler)
        : _clockSource(clockSource), _scheduler(std::move(scheduler)), _handle(nullptr) {}

    virtual ~LDAPTimer() {
        if (_handle) {
            _handle->cancel().ignore();
        }
    }

    void setTimeout(Milliseconds timeout, TimeoutCallback cb) final {
        auto res = _scheduler->alarmFromNow(timeout);
        _handle = std::move(res.handle);

        std::move(res.future).getAsync([cb](Status status) {
            if (status == ErrorCodes::CallbackCanceled) {
                return;
            }

            fassert(51052, status);
            cb();
        });
    }

    void cancelTimeout() final {
        auto handle = std::move(_handle);
        if (handle) {
            handle->cancel().ignore();
        }
    }

    Date_t now() final {
        return _clockSource->now();
    }

private:
    ClockSource* const _clockSource;
    std::shared_ptr<AlarmScheduler> _scheduler;
    AlarmScheduler::SharedHandle _handle;
};

/*
 * This represents a single pooled connection in the context of the ConnectionPool. This
 * will get wrapped up so it looks like an LDAPConnection before being handed out to the
 * caller. When it goes out of scope the connection will be returned to the pool automatically
 */
class PooledLDAPConnection : public ConnectionPool::ConnectionInterface,
                             public std::enable_shared_from_this<PooledLDAPConnection> {
public:
    explicit PooledLDAPConnection(std::shared_ptr<OutOfLineExecutor> executor,
                                  ClockSource* clockSource,
                                  const std::shared_ptr<AlarmScheduler>& alarmScheduler,
                                  const HostAndPort& host,
                                  LDAPConnectionOptions options,
                                  size_t generation,
                                  std::shared_ptr<LDAPHostTimingData>);

    virtual ~PooledLDAPConnection() = default;

    const HostAndPort& getHostAndPort() const final {
        return _target;
    }

    // This cannot block under any circumstances because the ConnectionPool is holding
    // a mutex while calling isHealthy(). Since we don't have a good way of knowing whether
    // the connection is healthy, just return true here.
    bool isHealthy() final {
        return true;
    }

    void setTimeout(Milliseconds timeout, TimeoutCallback cb) final {
        _timer.setTimeout(timeout, cb);
    }

    void cancelTimeout() final {
        _timer.cancelTimeout();
    }

    Date_t now() final {
        return _timer.now();
    }

    transport::ConnectSSLMode getSslMode() const final {
        return _options.transportSecurity == LDAPTransportSecurityType::kTLS
            ? transport::kEnableSSL
            : transport::kDisableSSL;
    }

    LDAPConnection* getConn() const {
        return _conn.get();
    }

    const LDAPConnectionOptions& getConnectionOptions() const {
        return _options;
    }

    void incrementUsesCounter() {
        _timingData->uses().addAndFetch(1);
    }


    template <typename ResultType>
    Future<ResultType> runFuncWithTimeout(std::string context,
                                          unique_function<StatusOrStatusWith<ResultType>()> func) {
        auto pf = makePromiseFuture<ResultType>();
        auto alarm = _alarmScheduler->alarmFromNow(_options.timeout);
        auto state = std::make_shared<FunctionWithTimeoutState<ResultType>>(
            std::move(context), std::move(alarm.handle), std::move(pf.promise));

        std::move(alarm.future).getAsync([state](Status status) {
            if (!status.isOK() || state->done.swap(true)) {
                return;
            }

            state->promise.setError({ErrorCodes::OperationFailed, "Operation timed out"});
        });

        _executor->schedule([this, state, func = std::move(func)](Status status) {
            if (!status.isOK()) {
                return;
            }

            auto result = func();
            // If we timed out in the meantime, throw away the result and return immediately
            if (state->done.swap(true)) {
                LOGV2_DEBUG(24060,
                            1,
                            "LDAP operation {op} completed after timeout: {result}",
                            "LDAP operation completed after timeout",
                            "op"_attr = state->context,
                            "result"_attr = _resultToString(result));
                return;
            }

            // Cancel the alarm - we don't care if it's actually canceled or not, just trying to
            // prevent the function from firing when it's not going to do anything useful.
            state->alarmHandle->cancel().ignore();
            state->promise.setWith([&] { return std::move(result); });
        });

        return std::move(pf.future);
    }

private:
    void setup(Milliseconds timeout, SetupCallback cb) final;
    void refresh(Milliseconds timeout, RefreshCallback cb) final;

private:
    template <typename T>
    std::string _resultToString(const StatusWith<T>& sw) {
        return sw.getStatus().toString();
    }

    std::string _resultToString(const Status& s) {
        return s.toString();
    }

    template <typename ResultType>
    struct FunctionWithTimeoutState {
        FunctionWithTimeoutState(std::string context_,
                                 AlarmScheduler::SharedHandle alarmHandle_,
                                 Promise<ResultType> promise_)
            : context(std::move(context_)),
              alarmHandle(std::move(alarmHandle_)),
              promise(std::move(promise_)) {}

        AtomicWord<bool> done{false};
        std::string context;
        AlarmScheduler::SharedHandle alarmHandle;
        Promise<ResultType> promise;
    };

    std::shared_ptr<OutOfLineExecutor> _executor;
    std::shared_ptr<AlarmScheduler> _alarmScheduler;
    LDAPTimer _timer;
    LDAPConnectionOptions _options;
    std::unique_ptr<LDAPConnection> _conn;
    HostAndPort _target;
    std::shared_ptr<LDAPHostTimingData> _timingData;
};

void PooledLDAPConnection::setup(Milliseconds timeout, SetupCallback cb) {
    _options.timeout = timeout;
    _executor->schedule([this, cb = std::move(cb)](auto execStatus) {
        if (!execStatus.isOK()) {
            cb(this, execStatus);
            return;
        }

        _conn = makeNativeLDAPConn(_options);
        auto status = _conn->connect();
        if (!status.isOK()) {
            return cb(this, std::move(status));
        }

        Timer queryTimer;
        auto emptyQueryStatus = _conn->checkLiveness();
        auto elapsed = duration_cast<Milliseconds>(queryTimer.elapsed());

        if (emptyQueryStatus.isOK()) {
            LOGV2_DEBUG(24061,
                        1,
                        "Connecting to LDAP server {host} took {connectTimeElapsed}",
                        "Connected to LDAP server",
                        "host"_attr = _target,
                        "connectTimeElapsed"_attr = elapsed);
            _timingData->updateLatency(elapsed);
        } else {
            _timingData->markFailed();
        }
        cb(this, std::move(emptyQueryStatus));
    });
}

void PooledLDAPConnection::refresh(Milliseconds timeout, RefreshCallback cb) {
    _executor->schedule([this, cb = std::move(cb)](auto execStatus) {
        if (!execStatus.isOK()) {
            cb(this, execStatus);
            return;
        }

        Timer queryTimer;
        auto status = _conn->checkLiveness();
        auto elapsed = duration_cast<Milliseconds>(queryTimer.elapsed());
        LOGV2_DEBUG(24062,
                    1,
                    "Refreshed LDAP connection in {refreshTimelapsed}",
                    "Refreshed LDAP connection",
                    "refreshTimeElapsed"_attr = elapsed);

        if (status.isOK()) {
            _timingData->updateLatency(elapsed);
            indicateSuccess();
            indicateUsed();
        } else {
            _timingData->markFailed();
            indicateFailure(status);
        }
        cb(this, status);
    });
}

PooledLDAPConnection::PooledLDAPConnection(std::shared_ptr<OutOfLineExecutor> executor,
                                           ClockSource* clockSource,
                                           const std::shared_ptr<AlarmScheduler>& alarmScheduler,
                                           const HostAndPort& host,
                                           LDAPConnectionOptions options,
                                           size_t generation,
                                           std::shared_ptr<LDAPHostTimingData> timingData)
    : ConnectionInterface(generation),
      _executor(std::move(executor)),
      _alarmScheduler(alarmScheduler),
      _timer(clockSource, alarmScheduler),
      _options(std::move(options)),
      _conn(nullptr),
      _target(host),
      _timingData(std::move(timingData)) {}

// This is the actual LDAP connection that will be handed out of the factory, it keeps
// a shared_ptr reference to this connection pool type alive and calls the appropriate
// bookkeeping functions for each LDAP operation.
class WrappedConnection : public LDAPConnection {
public:
    explicit WrappedConnection(LDAPConnectionOptions options, ConnectionPool::ConnectionHandle conn)
        : LDAPConnection(options), _conn(std::move(conn)) {}

    ~WrappedConnection();

    Status connect() final;
    Status bindAsUser(const LDAPBindOptions&) final;
    Status checkLiveness() final;
    StatusWith<LDAPEntityCollection> query(LDAPQuery query) final;
    Status disconnect() final;
    boost::optional<std::string> currentBoundUser() const final;

private:
    LDAPConnection* _getConn() const {
        return checked_cast<PooledLDAPConnection*>(_conn.get())->getConn();
    }

    template <typename ResultType>
    Future<ResultType> _runFuncWithTimeout(std::string context,
                                           unique_function<StatusOrStatusWith<ResultType>()> func) {
        const auto ptr = checked_cast<PooledLDAPConnection*>(_conn.get());
        return ptr->runFuncWithTimeout<ResultType>(std::move(context), std::move(func));
    }

    ConnectionPool::ConnectionHandle _conn;
};

WrappedConnection::~WrappedConnection() {
    if (_conn->getStatus() == ConnectionPool::kConnectionStateUnknown) {
        _conn->indicateSuccess();
    }
}

Status WrappedConnection::connect() {
    auto status = _getConn()->connect();
    if (!status.isOK()) {
        _conn->indicateFailure(status);
    } else {
        _conn->indicateSuccess();
    }
    return status;
}

Status WrappedConnection::bindAsUser(const LDAPBindOptions& options) {
    auto status = _runFuncWithTimeout<void>(options.toCleanString(),
                                            [&] { return _getConn()->bindAsUser(options); })
                      .getNoThrow();
    if (!status.isOK()) {
        _conn->indicateFailure(status);
    } else {
        _conn->indicateSuccess();
    }
    return status;
}

boost::optional<std::string> WrappedConnection::currentBoundUser() const {
    return _getConn()->currentBoundUser();
}

Status WrappedConnection::checkLiveness() {
    return _getConn()->checkLiveness();
}

StatusWith<LDAPEntityCollection> WrappedConnection::query(LDAPQuery query) {
    auto swResults = _runFuncWithTimeout<LDAPEntityCollection>(
                         query.toString(), [&] { return _getConn()->query(std::move(query)); })
                         .getNoThrow();
    if (!swResults.isOK()) {
        _conn->indicateFailure(swResults.getStatus());
    } else {
        _conn->indicateSuccess();
    }

    return swResults;
}

Status WrappedConnection::disconnect() {
    auto status = _getConn()->disconnect();
    _conn->indicateFailure(
        {ErrorCodes::TransportSessionClosed, "LDAP connection was disconnected"});
    return status;
}

std::unique_ptr<LDAPConnection> pooledConnToWrappedConn(ConnectionPool::ConnectionHandle handle) {
    const auto ldapHandle = checked_cast<PooledLDAPConnection*>(handle.get());
    auto connOptions = ldapHandle->getConnectionOptions();
    return std::make_unique<WrappedConnection>(std::move(connOptions), std::move(handle));
}

}  // namespace

class LDAPTypeFactory : public executor::ConnectionPool::DependentTypeFactoryInterface {
public:
    LDAPTypeFactory()
        : _clockSource(SystemClockSource::get()),
          _executor(std::make_shared<ThreadPool>(_makeThreadPoolOptions())),
          _timerScheduler(std::make_shared<AlarmSchedulerPrecise>(_clockSource)),
          _timerRunner({_timerScheduler}),
          _timingData(std::make_shared<LDAPPoolTimingData>()) {}

    std::shared_ptr<ConnectionPool::ConnectionInterface> makeConnection(const HostAndPort&,
                                                                        transport::ConnectSSLMode,
                                                                        size_t generation) final;

    std::shared_ptr<ConnectionPool::TimerInterface> makeTimer() final {
        _start();
        return std::make_shared<LDAPTimer>(_clockSource, _timerScheduler);
    }

    const std::shared_ptr<OutOfLineExecutor>& getExecutor() final {
        return _executor;
    }

    Date_t now() final {
        return _clockSource->now();
    }

    void shutdown() final {
        if (!_running) {
            return;
        }
        _timerRunner.shutdown();

        auto pool = checked_pointer_cast<ThreadPool>(_executor);
        pool->shutdown();
        pool->join();
    }

protected:
    friend class LDAPConnectionFactory;
    friend class LDAPConnectionFactoryServerStatus;

    const std::shared_ptr<LDAPPoolTimingData>& getTimingData() const {
        return _timingData;
    }

    ThreadPool::Stats getThreadPoolStats() const {
        auto threadPool = static_cast<ThreadPool*>(_executor.get());
        return threadPool->getStats();
    }

private:
    void _start() {
        if (_running)
            return;
        _timerRunner.start();

        auto pool = checked_pointer_cast<ThreadPool>(_executor);
        pool->startup();

        _running = true;
    }

    static inline ThreadPool::Options _makeThreadPoolOptions() {
        ThreadPool::Options opts;
        opts.poolName = "LDAPConnPool";
        opts.maxThreads = ThreadPool::Options::kUnlimited;
        opts.maxIdleThreadAge = Seconds{5};

        return opts;
    }

    ClockSource* const _clockSource;
    std::shared_ptr<OutOfLineExecutor> _executor;
    std::shared_ptr<AlarmScheduler> _timerScheduler;
    bool _running = false;
    AlarmRunnerBackgroundThread _timerRunner;
    std::shared_ptr<LDAPPoolTimingData> _timingData;
};


std::shared_ptr<executor::ConnectionPool::ConnectionInterface> LDAPTypeFactory::makeConnection(
    const HostAndPort& host, transport::ConnectSSLMode sslMode, size_t generation) {
    _start();

    LDAPConnectionOptions options(Milliseconds::min(),
                                  {host.toString()},
                                  (sslMode == transport::kEnableSSL)
                                      ? LDAPTransportSecurityType::kTLS
                                      : LDAPTransportSecurityType::kNone);

    auto timingData = [&] {
        stdx::lock_guard<Latch> lk(_timingData->mutex);
        auto it = _timingData->timingData.find(host);
        if (it != _timingData->timingData.end()) {
            return it->second;
        }

        bool inserted;
        std::tie(it, inserted) =
            _timingData->timingData.insert({host, std::make_shared<LDAPHostTimingData>()});
        invariant(inserted);

        return it->second;
    }();

    return std::make_shared<PooledLDAPConnection>(_executor,
                                                  _clockSource,
                                                  _timerScheduler,
                                                  host,
                                                  std::move(options),
                                                  generation,
                                                  std::move(timingData));
}

class LDAPConnectionFactoryServerStatus : public ServerStatusSection {
public:
    LDAPConnectionFactoryServerStatus(LDAPConnectionFactory* factory)
        : ServerStatusSection("ldapConnPool"), _factory(factory) {}

    bool includeByDefault() const override {
        // Include this section by default if there are any LDAP servers defined.
        return LDAPManager::get(getGlobalServiceContext())->hasHosts();
    }

    BSONObj generateSection(OperationContext* opCtx,
                            const BSONElement& configElement) const override;

private:
    LDAPConnectionFactory* const _factory;
};

BSONObj LDAPConnectionFactoryServerStatus::generateSection(OperationContext* opCtx,
                                                           const BSONElement& configElement) const {
    BSONObjBuilder out;

    {
        const auto threadPoolStats = _factory->_typeFactory->getThreadPoolStats();
        BSONObjBuilder threadPoolStatsSection(out.subobjStart("threadPool"));
        threadPoolStatsSection << "numThreads" << static_cast<int>(threadPoolStats.numThreads)
                               << "numIdleThreads"
                               << static_cast<int>(threadPoolStats.numIdleThreads)
                               << "numPendingTasks"
                               << static_cast<int>(threadPoolStats.numPendingTasks)
                               << "lastFullUtilizationDate"
                               << threadPoolStats.lastFullUtilizationDate;
    }

    ConnectionPoolStats connPoolStats;
    _factory->_pool->appendConnectionStats(&connPoolStats);
    const auto timingData = [&] {
        const auto& timingData = _factory->_typeFactory->getTimingData();
        stdx::lock_guard<Latch> lk(timingData->mutex);
        return timingData->timingData;
    }();

    {
        BSONArrayBuilder timingDataSection(out.subarrayStart("ldapServerStats"));

        for (const auto& kv : connPoolStats.statsByHost) {
            BSONObjBuilder perHost(timingDataSection.subobjStart());
            auto it = timingData.find(kv.first);
            Milliseconds latency = Milliseconds::max();
            int64_t uses = 0;
            if (it != timingData.end()) {
                latency = it->second->getLatency();
                uses = it->second->uses().loadRelaxed();
            }

            perHost << "host" << kv.first.toString() << "connectionsInUse"
                    << static_cast<int64_t>(kv.second.inUse) << "connectionsAvailable"
                    << static_cast<int64_t>(kv.second.available) << "connectionsCreated"
                    << static_cast<int64_t>(kv.second.created) << "connectionsRefreshing"
                    << static_cast<int64_t>(kv.second.refreshing) << "uses" << uses;
            if (latency != Milliseconds::max()) {
                perHost << "latencyMillis" << latency.count();
            }
        }
    }

    return out.obj();
}

LDAPConnectionFactory::LDAPConnectionFactory(Milliseconds poolSetupTimeout)
    : _typeFactory(std::make_shared<LDAPTypeFactory>()),
      _pool(std::make_shared<executor::ConnectionPool>(
          _typeFactory, "LDAP", makePoolOptions(poolSetupTimeout))),
      _serverStatusSection(std::make_unique<LDAPConnectionFactoryServerStatus>(this)) {}

struct LDAPCompletionState {
    LDAPCompletionState(int outstandingSources_, Promise<std::unique_ptr<LDAPConnection>> promise_)
        : finishLine(outstandingSources_), promise(std::move(promise_)) {}

    StrongWeakFinishLine finishLine;
    Promise<std::unique_ptr<LDAPConnection>> promise;
};

StatusWith<std::unique_ptr<LDAPConnection>> LDAPConnectionFactory::create(
    const LDAPConnectionOptions& options) {

    if (!options.usePooledConnection || !isNativeImplThreadSafe()) {
        auto conn = makeNativeLDAPConn(options);
        auto connectStatus = conn->connect();
        if (!connectStatus.isOK()) {
            return connectStatus;
        }

        return StatusWith<std::unique_ptr<LDAPConnection>>(std::move(conn));
    }

    auto sslMode = (options.transportSecurity == LDAPTransportSecurityType::kTLS)
        ? transport::kEnableSSL
        : transport::kDisableSSL;

    std::vector<HostAndPort> hosts;
    try {
        std::transform(options.hosts.begin(),
                       options.hosts.end(),
                       std::back_inserter(hosts),
                       [&](const std::string& server) {
                           auto host = HostAndPort::parse(server);
                           if (host.isOK() && !host.getValue().hasPort()) {
                               auto port = sslMode == transport::kEnableSSL ? ":636"_sd : ":389"_sd;
                               host = HostAndPort::parse(str::stream() << server << port);
                           }

                           return uassertStatusOK(host);
                       });
    } catch (const DBException& e) {
        return e.toStatus();
    }

    if (ldapConnectionPoolUseLatencyForHostPriority) {
        const auto& timingData = _typeFactory->getTimingData();
        stdx::lock_guard<Latch> lk(timingData->mutex);
        const auto getLatencyFor = [&](const HostAndPort& hp) {
            auto it = timingData->timingData.find(hp);
            return it == timingData->timingData.end() ? Milliseconds::max()
                                                      : it->second->getLatency();
        };

        std::stable_sort(
            hosts.begin(), hosts.end(), [&](const HostAndPort& a, const HostAndPort& b) {
                return getLatencyFor(a) < getLatencyFor(b);
            });
    }

    auto pf = makePromiseFuture<std::unique_ptr<LDAPConnection>>();
    auto state = std::make_shared<LDAPCompletionState>(hosts.size(), std::move(pf.promise));
    for (auto it = hosts.begin(); it != hosts.end() && !state->finishLine.isReady(); ++it) {
        const auto& server = *it;

        auto onConnect = [state,
                          server](StatusWith<executor::ConnectionPool::ConnectionHandle> swHandle) {
            if (swHandle.isOK()) {
                if (state->finishLine.arriveStrongly()) {
                    LOGV2_DEBUG(24063, 1, "Using LDAP server {host}", "host"_attr = server);
                    auto implPtr = static_cast<PooledLDAPConnection*>(swHandle.getValue().get());
                    implPtr->incrementUsesCounter();

                    state->promise.emplaceValue(
                        pooledConnToWrappedConn(std::move(swHandle.getValue())));
                } else {
                    swHandle.getValue()->indicateSuccess();
                }
            } else {
                LOGV2(24064,
                      "Got error connecting to {host}: {status},",
                      "Connection error",
                      "host"_attr = server,
                      "status"_attr = swHandle.getStatus());
                if (state->finishLine.arriveWeakly()) {
                    state->promise.setError(
                        swHandle.getStatus().withContext("Could not establish LDAP connection"));
                }
            }
        };
        auto semi = _pool->get(server, sslMode, options.timeout);
        if (semi.isReady()) {
            onConnect(std::move(semi).getNoThrow());
        } else {
            std::move(semi).thenRunOn(_typeFactory->getExecutor()).getAsync(std::move(onConnect));
        }
    }

    return std::move(pf.future).getNoThrow();
}
}  // namespace mongo
