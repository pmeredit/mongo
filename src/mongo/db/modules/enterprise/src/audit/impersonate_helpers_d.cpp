/**
*    Copyright (C) 2013 MongoDB Inc.
*/

#include "mongo/db/audit.h"

#include "audit_manager_global.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_management_commands_parser.h"
#include "mongo/db/auth/user_set.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/s/d_logic.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace audit {
    /*
     * Look for an 'impersonatedUsers' field.  This field is used by mongos to
     * transmit the usernames of the currently authenticated user when it runs commands
     * on a shard using internal user authentication.  Auditing uses this information
     * to properly ascribe users to actions.  This is necessary only for implicit actions that
     * mongos cannot properly audit itself; examples are implicit collection and database creation.
     * This function requires that the field is the last field in the bson object; it edits the
     * command BSON to efficiently remove the field before returning.
     */
    void parseAndRemoveImpersonatedUsersField(
            BSONObj cmdObj,
            AuthorizationSession* authSession,
            std::vector<UserName>* parsedUserNames,
            bool* fieldIsPresent) {

        *fieldIsPresent = false;
        LOG(4) << "parseAndRemoveImpersonatedUsersField: command: " << cmdObj;

        // Check if auditing is enabled
        if (!getGlobalAuditManager()->enabled) {
            return;
        }

        // Find impersonatedUsers element
        BSONObjIterator boit(cmdObj);
        BSONElement elem;
        for (elem = boit.next(); !elem.eoo(); elem = boit.next()) {
            if ( str::equals(elem.fieldName(), audit::cmdOptionImpersonatedUsers) ) {
                // Ensure this is the last field in the object
                uassert(4293, "impersonatedUsers is not the last field", !boit.more());
                break;
            }
        }

        if (elem.type() != Array) {
            return;
        }

        BSONArray impersonateArr(elem.embeddedObject());

        // Parse out the user/db pairs
        Status status = auth::parseUserNamesFromBSONArray(impersonateArr,
                                                          "", // dbname unused
                                                          parsedUserNames);
        if (!status.isOK()) {
            StringBuilder ss;
            ss << "invalid format of impersonatedUsers: "
               << status.toString();
            uasserted(4291, ss.str());
        }

        // Check priv
        if (!authSession->isAuthorizedForPrivilege(
                Privilege(ResourcePattern::forClusterResource(), ActionType::impersonate))) {
            uasserted(4292, "unauthorized use of impersonatedUsers");
        }

        // Lop off the field.  It must be the last field due to the check above.
        char* rawdata = const_cast<char*> (elem.rawdata());
        *rawdata = EOO;
        int* size = reinterpret_cast<int*>(const_cast<char*>(cmdObj.objdata()));
        *size -= elem.size();
        *fieldIsPresent = true;
    }

    /*
     * Look for an 'impersonatedRoles' field.  This field is used by mongos to
     * transmit the rolenames of the currently authenticated user when it runs commands
     * on a shard using internal user authentication.  Auditing uses this information
     * to properly ascribe user roles to actions.  This is necessary only for implicit actions that
     * mongos cannot properly audit itself; examples are implicit collection and database creation.
     * This function requires that the field is the last field in the bson object; it edits the
     * command BSON to efficiently remove the field before returning.
     */
    void parseAndRemoveImpersonatedRolesField(
            BSONObj cmdObj,
            AuthorizationSession* authSession,
            std::vector<RoleName>* parsedRoleNames,
            bool* fieldIsPresent) {

        *fieldIsPresent = false;
        LOG(4) << "parseAndRemoveImpersonatedRolesField: command: " << cmdObj;

        // Check if auditing is enabled
        if (!getGlobalAuditManager()->enabled) {
            return;
        }

        // Find impersonatedRoles element
        BSONObjIterator boit(cmdObj);
        BSONElement elem;
        for (elem = boit.next(); !elem.eoo(); elem = boit.next()) {
            if ( str::equals(elem.fieldName(), audit::cmdOptionImpersonatedRoles) ) {
                // Ensure this is the last field in the object
                uassert(4293, "impersonatedRoles is not the last field", !boit.more());
                break;
            }
        }

        if (elem.type() != Array) {
            return;
        }

        BSONArray impersonateArr(elem.embeddedObject());

        // Parse out the user/db pairs
        Status status = auth::parseRoleNamesFromBSONArray(impersonateArr,
                                                          "", // dbname unused
                                                          parsedRoleNames);
        if (!status.isOK()) {
            StringBuilder ss;
            ss << "invalid format of impersonatedRoles: "
               << status.toString();
            uasserted(4291, ss.str());
        }

        // Check priv
        if (!authSession->isAuthorizedForPrivilege(
                Privilege(ResourcePattern::forClusterResource(), ActionType::impersonate))) {
            uasserted(4292, "unauthorized use of impersonatedRoles");
        }

        // Lop off the field.  It must be the last field due to the check above.
        char* rawdata = const_cast<char*> (elem.rawdata());
        *rawdata = EOO;
        int* size = reinterpret_cast<int*>(const_cast<char*>(cmdObj.objdata()));
        *size -= elem.size();
        *fieldIsPresent = true;
    }

}
}
