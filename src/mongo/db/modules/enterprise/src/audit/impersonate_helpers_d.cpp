/**
*    Copyright (C) 2013 MongoDB Inc.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/db/audit.h"

#include "mongo/base/data_type_endian.h"
#include "mongo/base/data_view.h"
#include "mongo/db/auth/user_management_commands_parser.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/jsobj.h"
#include "mongo/rpc/metadata/audit_metadata.h"
#include "mongo/util/log.h"
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
void parseAndRemoveImpersonatedUsersField(BSONObj cmdObj,
                                          std::vector<UserName>* parsedUserNames,
                                          bool* fieldIsPresent) {
    *fieldIsPresent = false;
    LOG(4) << "parseAndRemoveImpersonatedUsersField: command: " << cmdObj;

    // Find impersonatedUsers element
    BSONObjIterator boit(cmdObj);
    BSONElement elem;
    for (elem = boit.next(); !elem.eoo(); elem = boit.next()) {
        if (str::equals(elem.fieldName(), rpc::kLegacyImpersonatedUsersFieldName)) {
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
                                                      "",  // dbname unused
                                                      parsedUserNames);
    if (!status.isOK()) {
        StringBuilder ss;
        ss << "invalid format of impersonatedUsers: " << status.toString();
        uasserted(4291, ss.str());
    }

    // Lop off the field.  It must be the last field due to the check above.
    char* rawdata = const_cast<char*>(elem.rawdata());
    *rawdata = EOO;

    DataView dv(const_cast<char*>(cmdObj.objdata()));
    int32_t size = dv.read<LittleEndian<int32_t>>();
    dv.write<LittleEndian<int32_t>>(size - elem.size());

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
void parseAndRemoveImpersonatedRolesField(BSONObj cmdObj,
                                          std::vector<RoleName>* parsedRoleNames,
                                          bool* fieldIsPresent) {
    *fieldIsPresent = false;
    LOG(4) << "parseAndRemoveImpersonatedRolesField: command: " << cmdObj;

    // Find impersonatedRoles element
    BSONObjIterator boit(cmdObj);
    BSONElement elem;
    for (elem = boit.next(); !elem.eoo(); elem = boit.next()) {
        if (str::equals(elem.fieldName(), rpc::kLegacyImpersonatedRolesFieldName)) {
            // Ensure this is the last field in the object
            uassert(18530, "impersonatedRoles is not the last field", !boit.more());
            break;
        }
    }

    if (elem.type() != Array) {
        return;
    }

    BSONArray impersonateArr(elem.embeddedObject());

    // Parse out the user/db pairs
    Status status = auth::parseRoleNamesFromBSONArray(impersonateArr,
                                                      "",  // dbname unused
                                                      parsedRoleNames);
    if (!status.isOK()) {
        StringBuilder ss;
        ss << "invalid format of impersonatedRoles: " << status.toString();
        uasserted(18531, ss.str());
    }

    // Lop off the field.  It must be the last field due to the check above.
    char* rawdata = const_cast<char*>(elem.rawdata());
    *rawdata = EOO;

    DataView dv(const_cast<char*>(cmdObj.objdata()));
    int32_t size = dv.read<LittleEndian<int32_t>>();
    dv.write<LittleEndian<int32_t>>(size - elem.size());

    *fieldIsPresent = true;
}

}  // namespace auth
}  // namespace mongo
