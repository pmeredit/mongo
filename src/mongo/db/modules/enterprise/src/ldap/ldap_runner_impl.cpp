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
    : _factory(LDAPConnectionFactory()),
      _defaultBindOptions(std::move(defaultBindOptions)),
      _options(std::move(options)) {}

LDAPRunnerImpl::~LDAPRunnerImpl() = default;

Status LDAPRunnerImpl::verifyLDAPCredentials(const LDAPBindOptions& bindOptions) {
    StatusWith<std::unique_ptr<LDAPConnection>> swConnection = _factory.create(_options);
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    // Attempt to bind to the LDAP server with the provided credentials
    return swConnection.getValue()->bindAsUser(bindOptions);
}

StatusWith<LDAPEntityCollection> LDAPRunnerImpl::runQuery(const LDAPQuery& query) {
    StatusWith<std::unique_ptr<LDAPConnection>> swConnection = _factory.create(_options);
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    // If a user has been provided, bind to it
    if (!_defaultBindOptions.bindDN.empty()) {
        Status status = swConnection.getValue()->bindAsUser(_defaultBindOptions);
        if (!status.isOK()) {
            return status;
        }
    }

    // We now have a connection. Run the query, accumulating a result
    return swConnection.getValue()->query(query);
}

}  // namespace mongo
