/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_host.h"
#include "ldap_runner.h"

#include "mongo/util/system_tick_source.h"

#include "connections/ldap_connection_factory.h"
#include "ldap_connection_options.h"

namespace mongo {
struct LDAPBindOptions;
class ServiceContext;
template <typename T>
class StatusWith;

/** A concrete implementation of the LDAP bind and query runner.
 *
 * This implementation spawns and uses connections to remote LDAP servers.
 * It is thread safe.
 */
class LDAPRunnerImpl final : public LDAPRunner {
public:
    LDAPRunnerImpl(LDAPBindOptions defaultBindOptions,
                   LDAPConnectionOptions options,
                   std::unique_ptr<LDAPConnectionFactory> factory);
    ~LDAPRunnerImpl() final;

    Status bindAsUser(const std::string& user,
                      const SecureString& pwd,
                      TickSource* tickSource,
                      const SharedUserAcquisitionStats& userAcquisitionStats) final;
    StatusWith<LDAPEntityCollection> runQuery(
        const LDAPQuery& query,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats) final;

    Status checkLiveness(TickSource* tickSource,
                         const SharedUserAcquisitionStats& userAcquisitionStats) final;
    Status checkLivenessNotPooled(const LDAPConnectionOptions& connectionOptions,
                                  TickSource* tickSource,
                                  const SharedUserAcquisitionStats& userAcquisitionStats) final;

    std::vector<LDAPHost> getHosts() const final;
    void setHosts(std::vector<LDAPHost> hosts) final;
    bool hasHosts() const final;

    Milliseconds getTimeout() const final;
    void setTimeout(Milliseconds timeout) final;

    int getRetryCount() const final;
    void setRetryCount(int retryCount) final;

    std::string getBindDN() const final;
    void setBindDN(const std::string& bindDN) final;

    void setBindPasswords(std::vector<SecureString> pwd) final;

    void setUseConnectionPool(bool val) final;

private:
    StatusWith<std::unique_ptr<LDAPConnection>> getConnection(
        TickSource* tickSource, const SharedUserAcquisitionStats& userAcquisitionStats);
    // Note: when used with non-standard options the conenction must not be pooled.
    StatusWith<std::unique_ptr<LDAPConnection>> getConnectionWithOptions(
        LDAPConnectionOptions connectionOptions,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats);

    std::unique_ptr<LDAPConnectionFactory> _factory;

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
