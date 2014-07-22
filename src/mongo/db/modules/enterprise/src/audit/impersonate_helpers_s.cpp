/**
*    Copyright (C) 2013 MongoDB Inc.
*/

#include "mongo/db/audit.h"

#include "audit_manager_global.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_management_commands_parser.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/jsobj.h"

namespace mongo {
namespace audit {

    void appendImpersonatedRoleNames(RoleNameIterator roles, BSONArrayBuilder& builder) {
        while (roles.more()) {
            const RoleName& role = roles.next();
            BSONObjBuilder roleNameBuilder(builder.subobjStart());
            roleNameBuilder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME,
                                   role.getRole());
            roleNameBuilder.append(AuthorizationManager::ROLE_DB_FIELD_NAME,
                                   role.getDB());
        }
    }

    void appendImpersonatedUserNames(UserNameIterator userNames, BSONArrayBuilder& builder) {
        while (userNames.more()) {
            const UserName& userName = userNames.next();
            BSONObjBuilder userNameBuilder(builder.subobjStart());
            userNameBuilder.append(AuthorizationManager::USER_NAME_FIELD_NAME, userName.getUser());
            userNameBuilder.append(AuthorizationManager::USER_DB_FIELD_NAME, userName.getDB());
        }
    }

    void appendImpersonatedUsers(BSONObjBuilder* cmd) {
        if (!getGlobalAuditManager()->enabled) {
            return;
        }

        BSONArrayBuilder usersArrayBuilder(cmd->subarrayStart(cmdOptionImpersonatedUsers));
        ClientBasic* client(ClientBasic::getCurrent());
        if (!client) {
            return;
        }

        AuthorizationSession* authorizationSession(client->getAuthorizationSession());
        appendImpersonatedUserNames(authorizationSession->getAuthenticatedUserNames(),
                                    usersArrayBuilder);
        usersArrayBuilder.done();

        BSONArrayBuilder rolesArrayBuilder(cmd->subarrayStart(cmdOptionImpersonatedRoles));
        appendImpersonatedRoleNames(authorizationSession->getAuthenticatedRoleNames(),
                                    rolesArrayBuilder);
        rolesArrayBuilder.done();
    }

}
}
