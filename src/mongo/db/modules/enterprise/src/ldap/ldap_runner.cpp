/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_runner.h"

#include "mongo/util/assert_util.h"

#include "ldap_connection.h"
#include "ldap_connection_factory.h"
#include "ldap_query.h"

namespace mongo {

namespace {
const size_t kMaxConnections = 10;
}  // namespace

LDAPRunner::LDAPRunner(std::unique_ptr<LDAPConnectionFactory> factory,
                       LDAPBindOptions bindOptions,
                       LDAPConnectionOptions options)
    : _factory(std::move(factory)),
      _bindOptions(std::move(bindOptions)),
      _options(std::move(options)) {}

LDAPRunner::~LDAPRunner() = default;

StatusWith<LDAPEntityCollection> LDAPRunner::runQuery(const LDAPQuery& query) {
    // Create a new connection
    // TODO: Use a connection pool
    StatusWith<std::unique_ptr<LDAPConnection>> swConnection = _factory->create(_options);
    if (!swConnection.isOK()) {
        return swConnection.getStatus();
    }

    // If a user has been provided, bind to it
    if (!_bindOptions.bindDN.empty()) {
        Status status = swConnection.getValue()->bindAsUser(_bindOptions);
        if (!status.isOK()) {
            return status;
        }
    }

    // We now have a connection. Run the query, accumulating a result
    return swConnection.getValue()->query(query);
}
}  // namespace mongo
