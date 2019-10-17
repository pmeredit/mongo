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
    Status checkLiveness() final;

    std::vector<std::string> getHosts() const final;
    void setHosts(std::vector<std::string> hosts) final;
    bool hasHosts() const final;

    Milliseconds getTimeout() const final;
    void setTimeout(Milliseconds timeout) final;

    std::string getBindDN() const final;
    void setBindDN(const std::string& bindDN) final;

    void setBindPasswords(std::vector<SecureString> pwd) final;

    void setUseConnectionPool(bool val) final;

private:
    StatusWith<std::unique_ptr<LDAPConnection>> getConnection();

    LDAPConnectionFactory _factory;

    /**
     * Protects access to _defaultBindOptions.bindDN, _defaultBindOptions.password,
     * _options.serverURIs and _options.timeout.
     */
    mutable Mutex _memberAccessMutex = MONGO_MAKE_LATCH("LDAPRunnerImpl::_memberAccessMutex");

    std::vector<SecureString> _bindPasswords;
    LDAPBindOptions _defaultBindOptions;
    LDAPConnectionOptions _options;
};
}  // namespace mongo
