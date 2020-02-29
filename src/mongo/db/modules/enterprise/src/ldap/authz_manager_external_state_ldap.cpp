/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

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
#include "mongo/db/auth/user_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

#include "ldap_manager.h"
#include "ldap_options.h"

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
        _invalidator.go();
        log() << "Server configured with LDAP Authorization. Spawned $external user cache "
                 "invalidator.";
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
        log() << "LDAP Authorization has been enabled. Authorization attempts on the "
                 "$external database will be routed to the remote LDAP server. "
                 "Any existing users which may have been created on the $external "
                 "database have been disabled. These users have not been deleted. "
                 "Restarting mongod without LDAP Authorization will restore access to "
                 "them.";
    }
    return Status::OK();
}

Status AuthzManagerExternalStateLDAP::getUserDescription(OperationContext* opCtx,
                                                         const UserRequest& userReq,
                                                         BSONObj* result) {
    const UserName& userName = userReq.name;
    if (userName.getDB() != "$external" || userReq.roles) {
        return _wrappedExternalState->getUserDescription(opCtx, userReq, result);
    }

    StatusWith<std::vector<RoleName>> swRoles =
        LDAPManager::get(opCtx->getServiceContext())->getUserRoles(userName);
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
    // clang-format on
    BSONObj unresolvedUserDocument = builder.obj();

    mutablebson::Document resultDoc(unresolvedUserDocument,
                                    mutablebson::Document::kInPlaceDisabled);
    _wrappedExternalState->resolveUserRoles(&resultDoc, swRoles.getValue());
    *result = resultDoc.getObject();

    return Status::OK();
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
