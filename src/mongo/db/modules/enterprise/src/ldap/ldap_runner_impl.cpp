/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <algorithm>
#include <memory>

#include "ldap_runner_impl.h"

#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/db/client.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/periodic_runner.h"

#include "connections/ldap_connection.h"
#include "connections/ldap_connection_factory.h"
#include "ldap_host.h"
#include "ldap_options.h"
#include "ldap_query.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kNetwork

namespace mongo {

namespace {
const size_t kMaxConnections = 10;

bool shouldRetry(Status status) {
    if (ErrorCodes::isRetriableError(status.code())) {
        return true;
    }

    return false;
}

}  // namespace

// TODO: Use a connection pool instead of constantly creating new connections
LDAPRunnerImpl::LDAPRunnerImpl(LDAPBindOptions defaultBindOptions,
                               LDAPConnectionOptions options,
                               std::unique_ptr<LDAPConnectionFactory> factory)
    : _factory(std::move(factory)),
      _defaultBindOptions(std::move(defaultBindOptions)),
      _options(std::move(options)) {}

LDAPRunnerImpl::~LDAPRunnerImpl() = default;

Status LDAPRunnerImpl::bindAsUser(const std::string& user,
                                  const SecureString& pwd,
                                  TickSource* tickSource,
                                  const SharedUserAcquisitionStats& userAcquisitionStats) {
    for (int retry = 0, maxRetryCount = getRetryCount();; ++retry) {
        LDAPConnectionOptions connectionOptions;
        {
            stdx::lock_guard<Latch> lock(_memberAccessMutex);
            connectionOptions = _options;
        }

        auto swConnection = _factory->create(std::move(connectionOptions));
        if (!swConnection.isOK()) {
            return swConnection.getStatus();
        }

        // It is safe to use authenticationChoice and saslMechanism outside of the mutex since they
        // are not runtime settable.
        auto bindOptions =
            std::make_unique<LDAPBindOptions>(user,
                                              pwd,
                                              _defaultBindOptions.authenticationChoice,
                                              _defaultBindOptions.saslMechanisms,
                                              false);

        // Attempt to bind to the LDAP server with the provided credentials.
        auto status = swConnection.getValue()->bindAsUser(
            std::move(bindOptions), tickSource, userAcquisitionStats);
        if (retry >= maxRetryCount || !shouldRetry(status)) {
            return status;
        }
        LOGV2_INFO(6709401, "Retrying LDAP bind", "retry"_attr = retry, "status"_attr = status);
    }
}

StatusWith<std::unique_ptr<LDAPConnection>> LDAPRunnerImpl::getConnection(
    TickSource* tickSource, const SharedUserAcquisitionStats& userAcquisitionStats) {
    LDAPConnectionOptions connectionOptions;

    {
        stdx::lock_guard<Latch> lock(_memberAccessMutex);
        connectionOptions = _options;
    }
    return getConnectionWithOptions(connectionOptions, tickSource, userAcquisitionStats);
}

StatusWith<std::unique_ptr<LDAPConnection>> LDAPRunnerImpl::getConnectionWithOptions(
    LDAPConnectionOptions connectionOptions,
    TickSource* tickSource,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    for (int retry = 0, maxRetryCount = getRetryCount();; ++retry) {

        LDAPBindOptions bindOptions;
        std::vector<SecureString> bindPasswords;
        {
            stdx::lock_guard<Latch> lock(_memberAccessMutex);
            bindOptions = _defaultBindOptions;
            bindPasswords = _bindPasswords;
        }
        auto swConnection = _factory->create(connectionOptions);
        if (!swConnection.isOK()) {
            return swConnection.getStatus();
        }

        // If a user has been provided, bind to it.
        const auto boundUser = swConnection.getValue()->currentBoundUser();
        if (bindOptions.shouldBind() && (!boundUser || *boundUser != bindOptions.bindDN)) {
            Status bindStatus = Status::OK();
            if (!bindPasswords.empty()) {
                for (const auto& pwd : bindPasswords) {
                    bindOptions.password = pwd;
                    bindStatus = swConnection.getValue()->bindAsUser(
                        std::make_unique<LDAPBindOptions>(bindOptions),
                        tickSource,
                        userAcquisitionStats);
                    if (bindStatus.isOK()) {
                        break;
                    }
                }
            } else {
                bindStatus = swConnection.getValue()->bindAsUser(
                    std::make_unique<LDAPBindOptions>(bindOptions),
                    tickSource,
                    userAcquisitionStats);
            }

            if (bindStatus.isOK()) {
                return swConnection;
            }

            if (retry >= maxRetryCount || !shouldRetry(bindStatus)) {
                return bindStatus;
            }

            LOGV2_INFO(
                6709402, "Retrying LDAP bind", "retry"_attr = retry, "status"_attr = bindStatus);
            continue;
        }

        return swConnection;
    }
}

StatusWith<LDAPEntityCollection> LDAPRunnerImpl::runQuery(
    const LDAPQuery& query,
    TickSource* tickSource,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    for (int retry = 0, maxRetryCount = getRetryCount();; ++retry) {
        auto swConnection = getConnection(tickSource, userAcquisitionStats);
        if (!swConnection.isOK()) {
            return swConnection.getStatus();
        }
        auto status = swConnection.getValue()->query(query, tickSource, userAcquisitionStats);
        if (retry >= maxRetryCount || !shouldRetry(status.getStatus())) {
            return status;
        }
        LOGV2_INFO(6709403,
                   "Retrying LDAP query",
                   "retry"_attr = retry,
                   "status"_attr = status.getStatus());
    }
}

Status LDAPRunnerImpl::checkLiveness(TickSource* tickSource,
                                     const SharedUserAcquisitionStats& userAcquisitionStats) {
    auto swConnection = getConnection(tickSource, userAcquisitionStats);
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    return swConnection.getValue()->checkLiveness(tickSource, userAcquisitionStats);
}

Status LDAPRunnerImpl::checkLivenessNotPooled(
    const LDAPConnectionOptions& connectionOptions,
    TickSource* tickSource,
    const SharedUserAcquisitionStats& userAcquisitionStats) {
    invariant(!connectionOptions.usePooledConnection);
    auto swConnection =
        getConnectionWithOptions(connectionOptions, tickSource, userAcquisitionStats);
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    return swConnection.getValue()->checkLiveness(tickSource, userAcquisitionStats);
}

std::vector<LDAPHost> LDAPRunnerImpl::getHosts() const {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    return _options.hosts;
}

void LDAPRunnerImpl::setHosts(std::vector<LDAPHost> hosts) {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);

    // Drop pooled connections to any hosts that have been removed.
    stdx::unordered_set<HostAndPort> newHosts;
    for (const auto& host : hosts) {
        newHosts.insert(host.serializeHostAndPort());
    }
    _factory->dropRemovedHosts(newHosts);

    _options.hosts = std::move(hosts);
}

bool LDAPRunnerImpl::hasHosts() const {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    return !_options.hosts.empty();
}

Milliseconds LDAPRunnerImpl::getTimeout() const {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    return _options.timeout;
}

void LDAPRunnerImpl::setTimeout(Milliseconds timeout) {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    _options.timeout = timeout;
}

int LDAPRunnerImpl::getRetryCount() const {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    return _options.retryCount;
}

void LDAPRunnerImpl::setRetryCount(int retryCount) {
    uassert(6709400, "LDAP retry count can not be negative.", retryCount >= 0);

    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    _options.retryCount = retryCount;
}


std::string LDAPRunnerImpl::getBindDN() const {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    return _defaultBindOptions.bindDN;
}

void LDAPRunnerImpl::setBindDN(const std::string& bindDN) {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    _defaultBindOptions.bindDN = bindDN;
}

void LDAPRunnerImpl::setBindPasswords(std::vector<SecureString> pwds) {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    _bindPasswords = std::move(pwds);
}

void LDAPRunnerImpl::setUseConnectionPool(bool val) {
    stdx::lock_guard<Latch> lock(_memberAccessMutex);
    _options.usePooledConnection = val;
}

}  // namespace mongo
