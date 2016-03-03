/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_runner_interface.h"

#include "connections/ldap_connection_factory.h"
#include "ldap_connection_options.h"

namespace mongo {
class LDAPConnectionFactory;
template <typename T>
class StatusWith;

/** A concrete implementation of the LDAP Query runner.
 *
 * This implementation spawns and uses connections to remote LDAP servers.
 * It is thread safe.
 */
class LDAPRunner : public LDAPRunnerInterface {
public:
    LDAPRunner(LDAPBindOptions bindOptions, LDAPConnectionOptions options);
    ~LDAPRunner() final;

    StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) final;

private:
    LDAPConnectionFactory _factory;
    LDAPBindOptions _bindOptions;
    LDAPConnectionOptions _options;
};
}  // namespace mongo
