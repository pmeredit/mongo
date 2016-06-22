/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/base/secure_allocator.h"
#include "mongo/stdx/mutex.h"

#include "ldap_manager.h"
#include "ldap_query_config.h"
#include "ldap_runner_impl.h"
#include "name_mapping/internal_to_ldap_user_name_mapper.h"

namespace mongo {
struct LDAPBindOptions;
class LDAPConnectionFactory;
class RoleName;
class ServiceContext;
template <typename T>
class StatusWith;
class UserName;

/**
 * A concrete implementation of the LDAP bind and query runner.
 *
 * This implementation spawns and uses connections to remote LDAP servers.
 * It is thread safe.
 */
class LDAPManagerImpl : public LDAPManager {
public:
    LDAPManagerImpl(std::unique_ptr<LDAPRunner> runner,
                    UserNameSubstitutionLDAPQueryConfig queryParameters,
                    InternalToLDAPUserNameMapper nameMapper);
    ~LDAPManagerImpl() final;

    StatusWith<std::vector<RoleName>> getUserRoles(const UserName& userName) final;
    Status verifyLDAPCredentials(const std::string& user, const SecureString& pwd) final;

    ////////////////////////////////////////////////////////////
    //
    // State inspection and manipulation methods.
    //
    ////////////////////////////////////////////////////////////

    std::string getHostURIs() const final;
    void setHostURIs(const std::string& hostURIs) final;

    Milliseconds getTimeout() const final;
    void setTimeout(Milliseconds timeout) final;

    std::string getBindDN() const final;
    void setBindDN(const std::string& bindDN) final;

    void setBindPassword(SecureString pwd) final;

    std::string getUserToDNMapping() const final;
    void setUserNameMapper(InternalToLDAPUserNameMapper nameMapper) final;

    std::string getQueryTemplate() const final;
    void setQueryConfig(UserNameSubstitutionLDAPQueryConfig queryConfig) final;

private:
    /**
     * For a provided LDAP search query, get the requested entities.
     * These entities can be of two different 'forms'. In the first, the provided
     * query returns a set of entities, without specifying any attributes. This method
     * should return the DNs of these entities. In the second, attributes were requested,
     * so we should return the DNs contained in these attributes.
     *
     * @param query An LDAP search query to perform against the server
     * @return Errors arising from the query or the results
     */
    StatusWith<LDAPDNVector> _getGroupDNsFromServer(LDAPQuery& query);

    /**
     * The LDAPRunner used to make outgoing queries to the LDAP server.
     */
    std::unique_ptr<LDAPRunner> _runner;

    /**
     * Protects access to the _queryConfig and _userToDN member variables.
     */
    mutable stdx::mutex _memberAccessMutex;

    /**
     * Template containing a query, in which authenticated user's DN will replace
     * '{USER}'
     */
    UserNameSubstitutionLDAPQueryConfig _queryConfig;

    /**
     * Mapper from authentication user name to LDAP DN
     */
    std::shared_ptr<InternalToLDAPUserNameMapper> _userToDN;
};
}  // namespace mongo
