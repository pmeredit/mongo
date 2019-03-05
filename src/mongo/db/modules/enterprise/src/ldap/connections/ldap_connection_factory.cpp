/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kConnectionPool

#include "mongo/platform/basic.h"

#include "ldap_connection_factory.h"

#include "mongo/base/status_with.h"
#include "mongo/executor/connection_pool.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/alarm.h"
#include "mongo/util/alarm_runner_background_thread.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/functional.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/system_clock_source.h"

#include "../ldap_connection_options.h"
#include "../ldap_options.h"
#include "../ldap_query.h"
#ifndef _WIN32
#include "openldap_connection.h"
#else
#include "windows_ldap_connection.h"
#endif

namespace mongo {
namespace {
using namespace executor;

static inline LDAPQuery makeRootDSEQuery() {
    auto swRootDSEQuery =
        LDAPQueryConfig::createLDAPQueryConfig("?supportedSASLMechanisms?base?(objectclass=*)");
    invariant(swRootDSEQuery.isOK());  // This isn't user configurable, so should never fail

    auto swQuery = LDAPQuery::instantiateQuery(swRootDSEQuery.getValue());
    invariant(swQuery);

    return std::move(swQuery.getValue());
}

Status runEmptyQuery(LDAPConnection* conn) {
    // This creates a query that queries the RootDSE, which should always be readable
    // without auth.
    static const auto query = makeRootDSEQuery();
    return conn->query(query).getStatus();
}

std::unique_ptr<LDAPConnection> makeNativeLDAPConn(const LDAPConnectionOptions& opts) {
#ifndef _WIN32
    return stdx::make_unique<OpenLDAPConnection>(opts);
#else
    return stdx::make_unique<WindowsLDAPConnection>(opts);
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
    explicit PooledLDAPConnection(const std::shared_ptr<ThreadPool> executor,
                                  ClockSource* clockSource,
                                  const std::shared_ptr<AlarmScheduler>& alarmScheduler,
                                  const HostAndPort& host,
                                  LDAPConnectionOptions options,
                                  size_t generation);

    virtual ~PooledLDAPConnection() = default;

    const HostAndPort& getHostAndPort() const final {
        return _target;
    }

    bool isHealthy() final {
        return runEmptyQuery(_conn.get()).isOK();
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

private:
    void setup(Milliseconds timeout, SetupCallback cb) final;
    void refresh(Milliseconds timeout, RefreshCallback cb) final;

private:
    std::shared_ptr<ThreadPool> _executor;
    LDAPTimer _timer;
    LDAPConnectionOptions _options;
    std::unique_ptr<LDAPConnection> _conn;
    HostAndPort _target;
};

void PooledLDAPConnection::setup(Milliseconds timeout, SetupCallback cb) {
    _options.timeout = timeout;
    _executor->schedule([this, cb](auto execStatus) {
        if (!execStatus.isOK()) {
            cb(this, execStatus);
            return;
        }

        _conn = makeNativeLDAPConn(_options);
        Status status = _conn->connect();
        if (!status.isOK()) {
            return cb(this, status);
        }

        cb(this, runEmptyQuery(_conn.get()));
    });
}

void PooledLDAPConnection::refresh(Milliseconds timeout, RefreshCallback cb) {
    _executor->schedule([this, cb](auto execStatus) {
        if (!execStatus.isOK()) {
            cb(this, execStatus);
            return;
        }

        auto status = runEmptyQuery(_conn.get());
        if (status.isOK()) {
            indicateSuccess();
        } else {
            indicateFailure(status);
        }
        cb(this, status);
    });
}

PooledLDAPConnection::PooledLDAPConnection(const std::shared_ptr<ThreadPool> executor,
                                           ClockSource* clockSource,
                                           const std::shared_ptr<AlarmScheduler>& alarmScheduler,
                                           const HostAndPort& host,
                                           LDAPConnectionOptions options,
                                           size_t generation)
    : ConnectionInterface(generation),
      _executor(executor),
      _timer(clockSource, alarmScheduler),
      _options(std::move(options)),
      _conn(nullptr),
      _target(host) {}

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
    StatusWith<LDAPEntityCollection> query(LDAPQuery query) final;
    Status disconnect() final;
    boost::optional<std::string> currentBoundUser() const final;

private:
    LDAPConnection* _getConn() const {
        return checked_cast<PooledLDAPConnection*>(_conn.get())->getConn();
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
        _conn->indicateUsed();
    }
    return status;
}

Status WrappedConnection::bindAsUser(const LDAPBindOptions& options) {
    auto status = _getConn()->bindAsUser(options);
    if (!status.isOK()) {
        _conn->indicateFailure(status);
    } else {
        _conn->indicateSuccess();
        _conn->indicateUsed();
    }
    return status;
}

boost::optional<std::string> WrappedConnection::currentBoundUser() const {
    return _getConn()->currentBoundUser();
}

StatusWith<LDAPEntityCollection> WrappedConnection::query(LDAPQuery query) {
    auto swResults = _getConn()->query(std::move(query));
    if (!swResults.isOK()) {
        _conn->indicateFailure(swResults.getStatus());
    } else {
        _conn->indicateSuccess();
        _conn->indicateUsed();
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
          _timerRunner({_timerScheduler}) {}

    std::shared_ptr<ConnectionPool::ConnectionInterface> makeConnection(const HostAndPort&,
                                                                        transport::ConnectSSLMode,
                                                                        size_t generation) final;

    std::shared_ptr<ConnectionPool::TimerInterface> makeTimer() final {
        _start();
        return std::make_shared<LDAPTimer>(_clockSource, _timerScheduler);
    }

    OutOfLineExecutor& getExecutor() final {
        return *_executor;
    }

    Date_t now() final {
        return _clockSource->now();
    }

    void shutdown() final {
        if (!_running) {
            return;
        }
        _timerRunner.shutdown();
        _executor->shutdown();
        _executor->join();
    }

private:
    void _start() {
        if (_running)
            return;
        _timerRunner.start();
        _executor->startup();
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
    std::shared_ptr<ThreadPool> _executor;
    std::shared_ptr<AlarmScheduler> _timerScheduler;
    bool _running = false;
    AlarmRunnerBackgroundThread _timerRunner;
};

std::shared_ptr<executor::ConnectionPool::ConnectionInterface> LDAPTypeFactory::makeConnection(
    const HostAndPort& host, transport::ConnectSSLMode sslMode, size_t generation) {
    _start();

    LDAPConnectionOptions options(Milliseconds::min(),
                                  {host.toString()},
                                  (sslMode == transport::kEnableSSL)
                                      ? LDAPTransportSecurityType::kTLS
                                      : LDAPTransportSecurityType::kNone);

    return std::make_shared<PooledLDAPConnection>(
        _executor, _clockSource, _timerScheduler, host, std::move(options), generation);
}

LDAPConnectionFactory::LDAPConnectionFactory(Milliseconds poolSetupTimeout)
    : _pool(std::make_unique<executor::ConnectionPool>(
          std::make_shared<LDAPTypeFactory>(), "LDAP", makePoolOptions(poolSetupTimeout))) {}

struct LDAPCompletionState {
    LDAPCompletionState(int outstandingSources_, Promise<std::unique_ptr<LDAPConnection>> promise_)
        : outstandingSources(outstandingSources_), promise(std::move(promise_)) {}

    stdx::mutex mutex;
    bool done = false;
    int outstandingSources;
    Promise<std::unique_ptr<LDAPConnection>> promise;
    std::vector<Status> errors;
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

    auto pf = makePromiseFuture<std::unique_ptr<LDAPConnection>>();
    auto state = std::make_shared<LDAPCompletionState>(options.hosts.size(), std::move(pf.promise));
    for (const auto& server : hosts) {
        _pool->get(server, sslMode, options.timeout)
            .getAsync([state,
                       server](StatusWith<executor::ConnectionPool::ConnectionHandle> swHandle) {
                stdx::lock_guard<stdx::mutex> lk(state->mutex);
                if (state->done) {
                    return;
                }

                state->outstandingSources--;
                if (!swHandle.isOK()) {
                    log() << "Got error connecting to " << server << ": " << swHandle.getStatus();
                    state->errors.push_back(swHandle.getStatus().withContext(server.toString()));
                    if (state->outstandingSources) {
                        return;
                    }

                    StringBuilder sb;
                    sb << "Could not establish LDAP connection: ";
                    bool isFirst = true;
                    for (const auto& error : state->errors) {
                        sb << error.toString();
                        if (isFirst) {
                            sb << ", ";
                            isFirst = false;
                        }
                    }

                    state->done = true;
                    state->promise.setError({ErrorCodes::OperationFailed, sb.str()});
                    return;
                }

                state->done = true;
                auto handle = std::move(swHandle.getValue());
                state->promise.emplaceValue(pooledConnToWrappedConn(std::move(handle)));
            });
    }

    return std::move(pf.future).getNoThrow();
}
}  // namespace mongo
