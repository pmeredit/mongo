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

    Status bindAsUser(const std::string& user, const SecureString& pwd) final;
    StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) final;

    std::string getHostURIs() const final;
    void setHostURIs(const std::string& hostURIs) final;

    Milliseconds getTimeout() const final;
    void setTimeout(Milliseconds timeout) final;

    std::string getBindDN() const final;
    void setBindDN(const std::string& bindDN) final;

    void setBindPassword(SecureString pwd) final;

private:
    LDAPConnectionFactory _factory;

    /**
     * Protects access to _defaultBindOptions.bindDN, _defaultBindOptions.password,
     * _options.serverURIs and _options.timeout.
     */
    mutable stdx::mutex _memberAccessMutex;

    LDAPBindOptions _defaultBindOptions;
    LDAPConnectionOptions _options;
};
}  // namespace mongo
