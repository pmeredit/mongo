/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
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
#include "mongo/db/auth/authorization_backend_interface.h"
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
    return auth::AuthorizationBackendInterface::get(opCtx->getService())->initialize(opCtx);
}

Status AuthzManagerExternalStateLDAP::getUserDescription(
    OperationContext* opCtx,
    const UserRequest& userReq,
    BSONObj* result,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    return auth::AuthorizationBackendInterface::get(opCtx->getService())
        ->getUserDescription(opCtx, userReq, result, userAcquisitionStats);
}

StatusWith<User> AuthzManagerExternalStateLDAP::getUserObject(
    OperationContext* opCtx,
    const UserRequest& userReq,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    return auth::AuthorizationBackendInterface::get(opCtx->getService())
        ->getUserObject(opCtx, userReq, userAcquisitionStats);
}

}  // namespace mongo
