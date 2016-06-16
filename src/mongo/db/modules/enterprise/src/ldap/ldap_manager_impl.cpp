/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include <memory>

#include "ldap_manager_impl.h"

#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "connections/ldap_connection_factory.h"
#include "ldap_options.h"
#include "ldap_query.h"

namespace mongo {

LDAPManagerImpl::LDAPManagerImpl(LDAPBindOptions defaultBindOptions,
                                 LDAPConnectionOptions options,
                                 UserNameSubstitutionLDAPQueryConfig queryParameters,
                                 InternalToLDAPUserNameMapper userToDN)
    : _runner(std::move(defaultBindOptions), std::move(options)),
      _queryConfig(std::move(queryParameters)),
      _userToDN(std::move(userToDN)) {}

LDAPManagerImpl::~LDAPManagerImpl() = default;

Status LDAPManagerImpl::verifyLDAPCredentials(const std::string& user, const SecureString& pwd) {
    auto swUser = _userToDN.transform(&_runner, user);
    if (!swUser.getStatus().isOK()) {
        return Status(swUser.getStatus().code(),
                      "Failed to transform authentication user name to LDAP DN: " +
                          swUser.getStatus().reason(),
                      swUser.getStatus().location());
    }

    return _runner.bindAsUser(std::move(swUser.getValue()), pwd);
}

StatusWith<std::vector<RoleName>> LDAPManagerImpl::getUserRoles(const UserName& userName) {
    auto swUser = _userToDN.transform(&_runner, userName.getUser());
    if (!swUser.isOK()) {
        return Status(swUser.getStatus().code(),
                      "Failed to transform bind user name to LDAP DN: " +
                          swUser.getStatus().reason(),
                      swUser.getStatus().location());
    }

    StatusWith<LDAPQuery> swQuery = LDAPQuery::instantiateQuery(_queryConfig, swUser.getValue());
    if (!swQuery.isOK()) {
        return swQuery.getStatus();
    }

    StatusWith<LDAPDNVector> swEntities = _getGroupDNsFromServer(swQuery.getValue());
    if (!swEntities.isOK()) {
        return Status(swEntities.getStatus().code(),
                      "Failed to obtain LDAP entities: " + swEntities.getStatus().reason(),
                      swEntities.getStatus().location());
    }

    std::vector<RoleName> roles;
    for (const LDAPDN& dn : swEntities.getValue()) {
        roles.push_back(RoleName(dn.c_str(), "admin"));
    }
    return roles;
}

StatusWith<LDAPDNVector> LDAPManagerImpl::_getGroupDNsFromServer(LDAPQuery& query) {
    bool isAcquiringAttributes = !query.getAttributes().empty();

    // Perform the query specified in ldapLDAPQuery against the server.
    StatusWith<LDAPEntityCollection> queryResultStatus = _runner.runQuery(query);
    if (!queryResultStatus.isOK()) {
        return queryResultStatus.getStatus();
    }
    LDAPEntityCollection queryResults(std::move(queryResultStatus.getValue()));

    // There are several different ways that an entity's group member may be described in LDAP.
    // It may contain attributes, each of which contain the DN of a group it is a member of.
    // The entity's DN may be listed as an attribute on the group's object. These two are likely
    // the most common configuration, and so are what we support.
    LDAPDNVector results;
    if (isAcquiringAttributes) {
        LOG(2) << "Acquiring group DNs from attributes on a single entity";
        // If we've requested attributes in our LDAP query, we assume that we're querying for a
        // single LDAP entity, which lists its group memberships as values on the requested
        // attributes. The values of these attributes are used as the DNs of the groups that the
        // user is a member of.
        if (queryResults.size() != 1) {
            // We wanted exactly one result. Something went wrong.
            const std::string msg =
                "Expected exactly one LDAP entity from which to parse "
                "attributes.";
            error() << msg << " Found " << queryResults.size() << ".";
            return Status{ErrorCodes::UserDataInconsistent, msg};
        }

        // Take every attribute value, and move it to the results.
        // The names of the attributes are ignored.
        LDAPAttributeKeyValuesMap attributeValues = queryResults.begin()->second;
        for (LDAPAttributeKeyValuesMap::value_type& values : attributeValues) {
            std::move(values.second.begin(), values.second.end(), std::back_inserter(results));
        }
    } else {
        // If we're not requesting attributes, then we assume that we're performing a query for
        // all group objects which profess to contain the user as a member. We use the DNs of the
        // acquired group objects as DNs of the groups the user is a member of.

        LOG(2) << "Acquiring group DNs from entities which possess user as attribute";
        // We are returning a set of entities which claim the user in their attributes
        for (auto it = queryResults.begin(); it != queryResults.end(); ++it) {
            results.emplace_back(std::move(it->first));
        }
    }
    return results;
}

}  // namespace mongo
