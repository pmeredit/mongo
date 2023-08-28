/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>

#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/executor/connection_pool.h"

#include "../ldap_resolver_cache.h"

namespace mongo {

template <class T>
class StatusWith;

class LDAPTypeFactory;
class LDAPConnectionReaper;
class LDAPConnection;
struct LDAPConnectionOptions;

/**
 * Interface for factories which produce LDAPConnection objects.
 */
class LDAPConnectionFactory {
public:
    virtual ~LDAPConnectionFactory() = default;

    explicit LDAPConnectionFactory(Milliseconds poolSetupTimeout);

    /**
     * Factory function to produce LDAP client objects
     * @param options Describes the connection to create
     *
     * @return Objects of type OpenLDAPConnection, or error
     */
    StatusWith<std::unique_ptr<LDAPConnection>> create(const LDAPConnectionOptions& options);

    /**
     * Method to drop all pooled connections to all hosts except for the new ones in newHosts.
     * @param newHosts Specifies the new hosts for whom connections should not be dropped.
     */
    void dropRemovedHosts(const stdx::unordered_set<HostAndPort>& newHosts);

private:
    friend class LDAPConnectionFactoryServerStatus;
    friend class LDAPOperationsServerStatusSection;

    std::shared_ptr<LDAPConnectionReaper> _reaper;
    std::shared_ptr<LDAPTypeFactory> _typeFactory;
    std::shared_ptr<executor::ConnectionPool> _pool;
    std::unique_ptr<ServerStatusSection> _connectionServerStatusSection;
    std::unique_ptr<ServerStatusSection> _operationsServerStatusSection;
    std::unique_ptr<LDAPDNSResolverCache> _dnsCache;
};

}  // namespace mongo
