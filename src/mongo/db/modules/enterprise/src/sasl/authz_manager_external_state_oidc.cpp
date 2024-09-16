/**
 *  Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "authz_manager_external_state_oidc.h"

#include "sasl/sasl_oidc_server_conversation.h"
#include "sasl/user_request_oidc.h"

#include "authorization_manager_factory_external_impl.h"
#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_manager_factory.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {
namespace {

Status reauthStatus(Status status) {
    return {ErrorCodes::ReauthenticationRequired, status.reason()};
}

MONGO_INITIALIZER(RegisterAuthzManagerOIDC)(InitializerContext* context) {
    createOIDCAuthzManagerExternalState =
        [](std::unique_ptr<AuthzManagerExternalState> wrappedState)
        -> std::unique_ptr<AuthzManagerExternalState> {
        return std::make_unique<AuthzManagerExternalStateOIDC>(std::move(wrappedState));
    };
};

}  // namespace

Status AuthzManagerExternalStateOIDC::getUserDescription(
    OperationContext* opCtx,
    const UserRequest& userReq,
    BSONObj* result,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    return _wrappedExternalState->getUserDescription(opCtx, userReq, result, userAcquisitionStats);
}

StatusWith<User> AuthzManagerExternalStateOIDC::getUserObject(
    OperationContext* opCtx,
    const UserRequest& userReq,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    return _wrappedExternalState->getUserObject(opCtx, userReq, userAcquisitionStats);
}

}  // namespace mongo::auth
