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
        UserNameIterator nameIter = authorizationSession->getAuthenticatedUserNames();
        for (; nameIter.more(); nameIter.next()) {
            BSONObjBuilder user(usersArrayBuilder.subobjStart());
            user.append(saslCommandUserFieldName, nameIter->getUser());
            user.append(saslCommandUserDBFieldName, nameIter->getDB());
        }
    }

}
}
