/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "authorization_backend_ldap.h"

#include <functional>
#include <memory>

#include "mongo/base/init.h"
#include "mongo/base/shim.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/curop.h"
#include "mongo/db/exec/mutable_bson/document.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

#include "ldap_manager.h"
#include "ldap_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {

namespace {

StatusWith<std::unique_ptr<UserRequest>> queryLDAPRolesForUserRequest(
    OperationContext* opCtx,
    const UserRequest& userReq,
    const SharedUserAcquisitionStats& userAcquisitionStats) {
    auto swRoles = LDAPManager::get(opCtx->getServiceContext())
                       ->getUserRoles(userReq.getUserName(),
                                      opCtx->getServiceContext()->getTickSource(),
                                      userAcquisitionStats);
    if (!swRoles.isOK()) {
        // Log failing Status objects produced from role acquisition, but because they may contain
        // sensitive information, do not propagate them to the client.
        LOGV2_ERROR(24217,
                    "LDAP authorization failed: {swRoles_getStatus}",
                    "swRoles_getStatus"_attr = swRoles.getStatus());
        return {ErrorCodes::LDAPRoleAcquisitionError, "Failed to acquire LDAP group membership"};
    }

    std::unique_ptr<UserRequest> returnRequest = userReq.clone();

    returnRequest->setRoles(
        std::set<RoleName>(swRoles.getValue().cbegin(), swRoles.getValue().cend()));
    return std::move(returnRequest);
}
}  // namespace

Status AuthorizationBackendLDAP::initialize(OperationContext* opCtx) {
    if (_hasInitializedInvalidation.swap(1) == 0) {
        _poller.go();
        LOGV2(24215,
              "Server configured with LDAP Authorization. Spawned $external user cache "
              "poller.");
    }

    // Detect if any documents exist in $external. If yes, log that they will not be
    // accessable while LDAP Authorization is active.
    BSONObj userObj;
    if (findOne(opCtx,
                NamespaceString::kAdminUsersNamespace,
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

StatusWith<User> AuthorizationBackendLDAP::getUserObject(
    OperationContext* opCtx,
    const UserRequest& userReq,
    const SharedUserAcquisitionStats& userAcquisitionStats) {
    const UserName userName = userReq.getUserName();
    if (userName.getDB() != "$external" || userReq.getRoles()) {
        return AuthorizationBackendLocal::getUserObject(opCtx, userReq, userAcquisitionStats);
    }

    auto swReq = queryLDAPRolesForUserRequest(opCtx, userReq, userAcquisitionStats);
    if (!swReq.isOK()) {
        return swReq.getStatus();
    }

    return AuthorizationBackendLocal::getUserObject(
        opCtx, *swReq.getValue().get(), userAcquisitionStats);
}


Status AuthorizationBackendLDAP::getUserDescription(
    OperationContext* opCtx,
    const UserRequest& userReq,
    BSONObj* result,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    const UserName& userName = userReq.getUserName();
    if (userName.getDB() != "$external" || userReq.getRoles()) {
        return AuthorizationBackendLocal::getUserDescription(
            opCtx, userReq, result, userAcquisitionStats);
    }

    auto swReq = queryLDAPRolesForUserRequest(opCtx, userReq, userAcquisitionStats);
    if (!swReq.isOK()) {
        return swReq.getStatus();
    }

    return AuthorizationBackendLocal::getUserDescription(
        opCtx, *swReq.getValue().get(), result, userAcquisitionStats);
}


}  // namespace mongo::auth
