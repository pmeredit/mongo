/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_runner_interface.h"

#include <memory>

#include "ldap_connection_options.h"

namespace mongo {
class LDAPConnection;
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
    LDAPRunner(std::unique_ptr<LDAPConnectionFactory> factory,
               LDAPBindOptions bindOptions,
               LDAPConnectionOptions options);
    ~LDAPRunner() final;

    StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) final;

private:
    std::unique_ptr<LDAPConnectionFactory> _factory;
    LDAPBindOptions _bindOptions;
    LDAPConnectionOptions _options;
};
}  // namespace mongo
