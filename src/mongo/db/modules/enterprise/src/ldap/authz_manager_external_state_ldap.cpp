/**
 *  Copyright (C) 2016 MongoDB Inc.
 */


#include "mongo/platform/basic.h"

#include "authz_manager_external_state_ldap.h"

#include <functional>
#include <memory>

#include "mongo/base/init.h"
#include "mongo/base/shim.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/db/auth/authz_manager_external_state_d.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/curop.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

#include "ldap_manager.h"
#include "ldap_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {

AuthzManagerExternalStateLDAP::AuthzManagerExternalStateLDAP(
    std::unique_ptr<AuthzManagerExternalStateLocal> wrappedExternalState)
    : _hasInitializedInvalidation(0), _wrappedExternalState(std::move(wrappedExternalState)) {}

Status AuthzManagerExternalStateLDAP::initialize(OperationContext* opCtx) {
    Status status = _wrappedExternalState->initialize(opCtx);

    // If initialization of the internal
    // object fails, do not perform LDAP specific initialization
    if (!status.isOK()) {
        return status;
    }

    if (_hasInitializedInvalidation.swap(1) == 0) {
        _poller.go();
        LOGV2(24215,
              "Server configured with LDAP Authorization. Spawned $external user cache "
              "poller.");
    }

    // Detect if any documents exist in $external. If yes, log that they will not be
    // accessable while LDAP Authorization is active.
    BSONObj userObj;
    if (_wrappedExternalState
            ->findOne(opCtx,
                      AuthorizationManager::usersCollectionNamespace,
                      BSON("db"
                           << "$external"),
                      &userObj)
            .isOK()) {
        LOGV2(24216,
              "LDAP Authorization has been enabled. Authorization attempts on the "
              "$external database will be routed to the remote LDAP server. "
              "Any existing users which may have been created on the $external "
              "database have been disabled. These users have not been deleted. "
              "Restarting mongod without LDAP Authorization will restore access to "
              "them.");
    }
    return Status::OK();
}

namespace {
StatusWith<UserRequest> queryLDAPRolesForUserRequest(OperationContext* opCtx,
                                                     const UserRequest& userReq) {
    auto swRoles = LDAPManager::get(opCtx->getServiceContext())
                       ->getUserRoles(userReq.name,
                                      opCtx->getServiceContext()->getTickSource(),
                                      CurOp::get(opCtx)->getMutableUserAcquisitionStats());
    if (!swRoles.isOK()) {
        // Log failing Status objects produced from role acquisition, but because they may contain
        // sensitive information, do not propagate them to the client.
        LOGV2_ERROR(24217,
                    "LDAP authorization failed: {swRoles_getStatus}",
                    "swRoles_getStatus"_attr = swRoles.getStatus());
        return {ErrorCodes::OperationFailed, "Failed to acquire LDAP group membership"};
    }

    auto newRequest = userReq;
    newRequest.roles = std::set<RoleName>(swRoles.getValue().cbegin(), swRoles.getValue().cend());
    return newRequest;
}
}  // namespace

Status AuthzManagerExternalStateLDAP::getUserDescription(OperationContext* opCtx,
                                                         const UserRequest& userReq,
                                                         BSONObj* result) {
    const UserName& userName = userReq.name;
    if (userName.getDB() != "$external" || userReq.roles) {
        return _wrappedExternalState->getUserDescription(opCtx, userReq, result);
    }

    auto swReq = queryLDAPRolesForUserRequest(opCtx, userReq);
    if (!swReq.isOK()) {
        return swReq.getStatus();
    }

    return _wrappedExternalState->getUserDescription(opCtx, swReq.getValue(), result);
}

StatusWith<User> AuthzManagerExternalStateLDAP::getUserObject(OperationContext* opCtx,
                                                              const UserRequest& userReq) {
    const UserName& userName = userReq.name;
    if (userName.getDB() != "$external" || userReq.roles) {
        return _wrappedExternalState->getUserObject(opCtx, userReq);
    }

    auto swReq = queryLDAPRolesForUserRequest(opCtx, userReq);
    if (!swReq.isOK()) {
        return swReq.getStatus();
    }

    return _wrappedExternalState->getUserObject(opCtx, swReq.getValue());
}

namespace {

std::unique_ptr<AuthzManagerExternalState> authzManagerExternalStateCreateImpl() {
    auto localState = std::make_unique<AuthzManagerExternalStateMongod>();
    if (!globalLDAPParams->isLDAPAuthzEnabled()) {
        return localState;
    }
    return std::make_unique<AuthzManagerExternalStateLDAP>(std::move(localState));
}

auto authzManagerExternalStateCreateRegistration = MONGO_WEAK_FUNCTION_REGISTRATION_WITH_PRIORITY(
    AuthzManagerExternalState::create, authzManagerExternalStateCreateImpl, 1);

}  // namespace

}  // namespace mongo
