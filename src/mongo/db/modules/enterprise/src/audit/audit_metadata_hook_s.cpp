/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/audit.h"

#include <vector>

#include "audit_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/client.h"
#include "mongo/rpc/metadata/audit_metadata.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace audit {

void writeImpersonatedUsersToMetadata(OperationContext* opCtx, BSONObjBuilder* metadataBob) {
    if (!getGlobalAuditManager()->enabled || !opCtx) {
        return;
    }

    AuthorizationSession* authorizationSession(AuthorizationSession::get(opCtx->getClient()));

    std::vector<UserName> impersonatedUsers;
    auto userNames = authorizationSession->getAuthenticatedUserNames();
    while (userNames.more()) {
        impersonatedUsers.emplace_back(userNames.next());
    }

    std::vector<RoleName> impersonatedRoles;
    auto roleNames = authorizationSession->getAuthenticatedRoleNames();
    while (roleNames.more()) {
        impersonatedRoles.emplace_back(roleNames.next());
    }

    uassertStatusOK(rpc::AuditMetadata(
                        std::make_tuple(std::move(impersonatedUsers), std::move(impersonatedRoles)))
                        .writeToMetadata(metadataBob));
}

}  // namespace audit
}  // namespace mongo
