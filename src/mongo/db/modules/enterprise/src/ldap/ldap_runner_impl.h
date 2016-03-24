/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_runner.h"

#include "connections/ldap_connection_factory.h"
#include "ldap_connection_options.h"

namespace mongo {
struct LDAPBindOptions;
class LDAPConnectionFactory;
class ServiceContext;
template <typename T>
class StatusWith;

/** A concrete implementation of the LDAP bind and query runner.
 *
 * This implementation spawns and uses connections to remote LDAP servers.
 * It is thread safe.
 */
class LDAPRunnerImpl : public LDAPRunner {
public:
    LDAPRunnerImpl(LDAPBindOptions defaultBindOptions, LDAPConnectionOptions options);
    ~LDAPRunnerImpl() final;

    Status verifyLDAPCredentials(const LDAPBindOptions& bindOptions) final;
    StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) final;

private:
    LDAPConnectionFactory _factory;
    LDAPBindOptions _defaultBindOptions;
    LDAPConnectionOptions _options;
};
}  // namespace mongo
