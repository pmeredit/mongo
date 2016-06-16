/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/base/secure_allocator.h"

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
    LDAPManagerImpl(LDAPBindOptions defaultBindOptions,
                    LDAPConnectionOptions options,
                    UserNameSubstitutionLDAPQueryConfig queryParameters,
                    InternalToLDAPUserNameMapper userToDN);
    ~LDAPManagerImpl() final;

    StatusWith<std::vector<RoleName>> getUserRoles(const UserName& userName) final;
    Status verifyLDAPCredentials(const std::string& user, const SecureString& pwd) final;

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
    LDAPRunnerImpl _runner;

    /**
     * Template containing a query, in which authenticated user's DN will replace
     * '{USER}'
     */
    UserNameSubstitutionLDAPQueryConfig _queryConfig;

    /**
     * Mapper from authentication user name to LDAP DN
     */
    InternalToLDAPUserNameMapper _userToDN;
};
}  // namespace mongo
