/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "authz_manager_external_state_ldap.h"

#include <pcrecpp.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/db/auth/authz_manager_external_state_d.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "connections/ldap_connection_factory.h"
#include "connections/ldap_connection.h"
#include "ldap_options.h"
#include "ldap_runner.h"

namespace mongo {

namespace {
const pcrecpp::RE userRegex("\\{USER\\}");
}  // namespace

AuthzManagerExternalStateLDAP::AuthzManagerExternalStateLDAP(
    std::unique_ptr<AuthzManagerExternalStateLocal> wrappedExternalState,
    UserNameSubstitutionLDAPQueryConfig queryParameters,
    InternalToLDAPUserNameMapper userToDN)
    : _hasInitializedInvalidation(0),
      _wrappedExternalState(std::move(wrappedExternalState)),
      _queryConfig(std::move(queryParameters)),
      _userToDN(std::move(userToDN)) {}

Status AuthzManagerExternalStateLDAP::initialize(OperationContext* txn) {
    Status status = _wrappedExternalState->initialize(txn);

    // If initialization of the internal
    // object fails, do not perform LDAP specific initialization
    if (!status.isOK()) {
        return status;
    }

    if (_hasInitializedInvalidation.swap(1) == 0) {
        _invalidator.go();
        log() << "Server configured with LDAP Authorization. Spawned $external user cache "
                 "invalidator.";
    }

    // Detect if any documents exist in $external. If yes, log that they will not be
    // accessable while LDAP Authorization is active.
    BSONObj userObj;
    if (_wrappedExternalState->findOne(txn,
                                       AuthorizationManager::usersCollectionNamespace,
                                       BSON("db"
                                            << "$external"),
                                       &userObj).isOK()) {
        log() << "LDAP Authorization has been enabled. Authorization attempts on the "
                 "$external database will be routed to the remote LDAP server. "
                 "Any existing users which may have been created on the $external "
                 "database have been disabled. These users have not been deleted. "
                 "Restarting mongod without LDAP Authorization will restore access to "
                 "them.";
    }
    return Status::OK();
}

Status AuthzManagerExternalStateLDAP::getUserDescription(OperationContext* txn,
                                                         const UserName& userName,
                                                         BSONObj* result) {
    if (userName.getDB() != "$external") {
        return _wrappedExternalState->getUserDescription(txn, userName, result);
    }

    StatusWith<std::vector<RoleName>> swRoles = _getUserRoles(txn, userName);
    if (!swRoles.isOK()) {
        // Log failing Status objects produced from role acquisition, but because they may contain
        // sensitive information, do not propagate them to the client.
        error() << "LDAP authorization failed: " << swRoles.getStatus();
        return Status{ErrorCodes::OperationFailed, "Failed to acquire LDAP group membership"};
    }
    BSONArrayBuilder roleArr;
    for (RoleName role : swRoles.getValue()) {
        roleArr << BSON("role" << role.getRole() << "db" << role.getDB());
    }

    BSONObjBuilder builder;
    // clang-format off
    builder << "user" << userName.getUser()
            << "db" << "$external"
            << "credentials" << BSON("external" << true)
            << "roles" << roleArr.arr();
    //clang-format on
    BSONObj unresolvedUserDocument = builder.obj();

    mutablebson::Document resultDoc(unresolvedUserDocument,
                                    mutablebson::Document::kInPlaceDisabled);
    _wrappedExternalState->resolveUserRoles(&resultDoc, swRoles.getValue());
    *result = resultDoc.getObject();

    return Status::OK();
}

StatusWith<std::vector<RoleName>> AuthzManagerExternalStateLDAP::_getUserRoles(
    OperationContext* txn, const UserName& userName) {

    StatusWith<std::string> swUser = _userToDN.transform(txn, userName.getUser());
    if (!swUser.isOK()) {
        return Status(swUser.getStatus().code(),
                      "Failed to transform user name to LDAP DN: " + swUser.getStatus().reason(),
                      swUser.getStatus().location());
    }

    StatusWith<LDAPQuery> swQuery = LDAPQuery::instantiateQuery(
                                        _queryConfig, swUser.getValue());
    if (!swQuery.isOK()) {
        return swQuery.getStatus();
    }

    StatusWith<LDAPDNVector> swEntities = _getGroupDNsFromServer(txn, swQuery.getValue());
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

StatusWith<LDAPDNVector> AuthzManagerExternalStateLDAP::_getGroupDNsFromServer(
    OperationContext* txn, LDAPQuery& query) {
    bool isAcquiringAttributes = !query.getAttributes().empty();

    // Perform the query specified in ldapLDAPQuery against the server.
    StatusWith<LDAPEntityCollection> queryResultStatus = LDAPRunner::get(txn->getServiceContext())->runQuery(query);
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


namespace {
std::unique_ptr<AuthzManagerExternalState> createLDAPAuthzManagerExternalState() {
    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
            globalLDAPParams.userAcquisitionQueryTemplate);
    massertStatusOK(swQueryParameters.getStatus());

    BSONObj expression;
    try {
        expression = fromjson(globalLDAPParams.userToDNMapping);
    } catch (DBException& e) {
        e.addContext(
            "Failed to parse JSON description of the relationship between "
            "MongoDB usernames and LDAP DNs");
        throw e;
    }
    auto swMapper =
        InternalToLDAPUserNameMapper::createNameMapper(std::move(expression));
    massertStatusOK(swMapper.getStatus());

    return stdx::make_unique<AuthzManagerExternalStateLDAP>(
        stdx::make_unique<AuthzManagerExternalStateMongod>(),
        std::move(swQueryParameters.getValue()),
        std::move(swMapper.getValue()));
}

MONGO_INITIALIZER_GENERAL(CreateLDAPAuthorizationExternalStateFactory,
                          ("CreateAuthorizationExternalStateFactory", "EndStartupOptionStorage"),
                          ("CreateAuthorizationManager", "SetLDAPRunnerImpl"))(InitializerContext* context) {
    // This initializer dependency injects the LDAPAuthzManagerExternalState into the
    // AuthorizationManager, by replacing the factory function the AuthorizationManager uses
    // to get its external state object.
    if (globalLDAPParams.isLDAPAuthzEnabled()) {
        AuthzManagerExternalState::create = createLDAPAuthzManagerExternalState;
    }
    return Status::OK();
}
}  // namespace
}  // namespace mongo
