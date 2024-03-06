/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <memory>

#include "ldap_runner_impl.h"

#include "mongo/util/assert_util.h"

#include "connections/ldap_connection.h"
#include "connections/ldap_connection_factory.h"
#include "ldap_options.h"
#include "ldap_query.h"

namespace mongo {

namespace {
const size_t kMaxConnections = 10;
}  // namespace

// TODO: Use a connection pool instead of constantly creating new connections
LDAPRunnerImpl::LDAPRunnerImpl(LDAPBindOptions defaultBindOptions, LDAPConnectionOptions options)
    : _factory(options.timeout),
      _defaultBindOptions(std::move(defaultBindOptions)),
      _options(std::move(options)) {}

LDAPRunnerImpl::~LDAPRunnerImpl() = default;

Status LDAPRunnerImpl::bindAsUser(const std::string& user, const SecureString& pwd) {
    LDAPConnectionOptions connectionOptions;
    {
        stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
        connectionOptions = _options;
    }
    auto swConnection = _factory.create(std::move(connectionOptions));
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    // It is safe to use authenticationChoice and saslMechanism outside of the mutex since they are
    // not runtime settable.
    LDAPBindOptions bindOptions(user,
                                pwd,
                                _defaultBindOptions.authenticationChoice,
                                _defaultBindOptions.saslMechanisms,
                                false);

    // Attempt to bind to the LDAP server with the provided credentials.
    return swConnection.getValue()->bindAsUser(std::move(bindOptions));
}

StatusWith<LDAPEntityCollection> LDAPRunnerImpl::runQuery(const LDAPQuery& query) {
    LDAPBindOptions bindOptions;
    LDAPConnectionOptions connectionOptions;
    {
        stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
        bindOptions = _defaultBindOptions;
        connectionOptions = _options;
    }
    auto swConnection = _factory.create(std::move(connectionOptions));
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    auto connection = std::move(swConnection.getValue());
    const auto boundUser = connection->currentBoundUser();
    // If a user has been provided, bind to it.
    if (bindOptions.shouldBind() && (!boundUser || *boundUser != bindOptions.bindDN)) {
        Status status = connection->bindAsUser(std::move(bindOptions));
        if (!status.isOK()) {
            return status;
        }
    }

    // We now have a connection. Run the query, accumulating a result.
    return connection->query(query);
}

std::vector<std::string> LDAPRunnerImpl::getHosts() const {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    return _options.hosts;
}

void LDAPRunnerImpl::setHosts(std::vector<std::string> hosts) {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);

    _options.hosts = std::move(hosts);
}

Milliseconds LDAPRunnerImpl::getTimeout() const {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    return _options.timeout;
}

void LDAPRunnerImpl::setTimeout(Milliseconds timeout) {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    _options.timeout = timeout;
}

std::string LDAPRunnerImpl::getBindDN() const {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    return _defaultBindOptions.bindDN;
}

void LDAPRunnerImpl::setBindDN(const std::string& bindDN) {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    _defaultBindOptions.bindDN = bindDN;
}

void LDAPRunnerImpl::setBindPassword(SecureString pwd) {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    _defaultBindOptions.password = std::move(pwd);
}

void LDAPRunnerImpl::setUseConnectionPool(bool val) {
    stdx::lock_guard<stdx::mutex> lock(_memberAccessMutex);
    _options.usePooledConnection = val;
}

}  // namespace mongo
