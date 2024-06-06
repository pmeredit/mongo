/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/modules/enterprise/src/audit/mongo/audit_mongo.h"
#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/parsed_privilege_gen.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"

namespace mongo {
namespace audit {

namespace {
constexpr auto kRolesField = "roles"_sd;
constexpr auto kPrivilegesField = "privileges"_sd;
constexpr auto kAuthenticationRestrictionsField = "authenticationRestrictions"_sd;
constexpr auto kDBField = "db"_sd;

void logGrantRevokeRolesToFromUser(Client* client,
                                   const UserName& username,
                                   const std::vector<RoleName>& roles,
                                   AuditEventType aType) {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         aType,
         [&](BSONObjBuilder* builder) {
             username.appendToBSON(builder);

             BSONArrayBuilder roleArray(builder->subarrayStart(kRolesField));
             for (const auto& role : roles) {
                 BSONObjBuilder roleBuilder(roleArray.subobjStart());
                 role.appendToBSON(&roleBuilder);
             }
             roleArray.doneFast();
         },
         ErrorCodes::OK});
}

void logCreateUpdateRole(Client* client,
                         const RoleName& role,
                         const std::vector<RoleName>* roles,
                         const PrivilegeVector* privileges,
                         const boost::optional<BSONArray>& restrictions,
                         AuditEventType aType) {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         aType,
         [&](BSONObjBuilder* builder) {
             const bool isCreate = aType == AuditEventType::kCreateRole;
             role.appendToBSON(builder);

             if (roles && (isCreate || !roles->empty())) {
                 BSONArrayBuilder roleArray(builder->subarrayStart(kRolesField));
                 for (const auto& roleName : *roles) {
                     BSONObjBuilder roleBuilder(roleArray.subobjStart());
                     roleName.appendToBSON(&roleBuilder);
                 }
                 roleArray.doneFast();
             }

             if (privileges && (isCreate || !privileges->empty())) {
                 BSONArrayBuilder privilegeArray(builder->subarrayStart(kPrivilegesField));
                 for (const auto& privilege : *privileges) {
                     BSONObjBuilder privilegeObj(privilegeArray.subobjStart());
                     try {
                         privilege.toParsedPrivilege().serialize(&privilegeObj);
                     } catch (const DBException&) {
                         fassert(4024, false);
                     }
                     privilegeObj.doneFast();
                 }
                 privilegeArray.doneFast();
             }

             if (restrictions && !restrictions->isEmpty()) {
                 builder->append("authenticationRestrictions", restrictions.value());
             }
         },
         ErrorCodes::OK});
}

void logGrantRevokeRolesToFromRole(Client* client,
                                   const RoleName& role,
                                   const std::vector<RoleName>& roles,
                                   AuditEventType aType) {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         aType,
         [&](BSONObjBuilder* builder) {
             role.appendToBSON(builder);
             BSONArrayBuilder rolesArray(builder->subarrayStart(kRolesField));
             for (const auto& rolename : roles) {
                 BSONObjBuilder roleBuilder(rolesArray.subobjStart());
                 rolename.appendToBSON(&roleBuilder);
             }
             rolesArray.doneFast();
         },
         ErrorCodes::OK});
}

void logGrantRevokePrivilegesToFromRole(Client* client,
                                        const RoleName& role,
                                        const PrivilegeVector& privileges,
                                        AuditEventType aType) {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         aType,
         [&](BSONObjBuilder* builder) {
             role.appendToBSON(builder);

             BSONArrayBuilder privilegeArray(builder->subarrayStart(kPrivilegesField));
             for (const auto& privilege : privileges) {
                 BSONObjBuilder privilegeObj(privilegeArray.subobjStart());
                 try {
                     privilege.toParsedPrivilege().serialize(&privilegeObj);
                 } catch (const DBException&) {
                     fassert(4028, false);
                 }
                 privilegeObj.doneFast();
             }
             privilegeArray.doneFast();
         },
         ErrorCodes::OK});
}

}  // namespace
}  // namespace audit

void audit::AuditMongo::logGrantRolesToUser(Client* client,
                                            const UserName& username,
                                            const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromUser(client, username, roles, AuditEventType::kGrantRolesToUser);
}

void audit::AuditMongo::logRevokeRolesFromUser(Client* client,
                                               const UserName& username,
                                               const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromUser(client, username, roles, AuditEventType::kRevokeRolesFromUser);
}

void audit::AuditMongo::logCreateRole(Client* client,
                                      const RoleName& role,
                                      const std::vector<RoleName>& roles,
                                      const PrivilegeVector& privileges,
                                      const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateRole(
        client, role, &roles, &privileges, restrictions, AuditEventType::kCreateRole);
}

void audit::AuditMongo::logUpdateRole(Client* client,
                                      const RoleName& role,
                                      const std::vector<RoleName>* roles,
                                      const PrivilegeVector* privileges,
                                      const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateRole(client, role, roles, privileges, restrictions, AuditEventType::kUpdateRole);
}

void audit::AuditMongo::logDropRole(Client* client, const RoleName& role) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kDropRole,
         [&](BSONObjBuilder* builder) { role.appendToBSON(builder); },
         ErrorCodes::OK});
}

void audit::AuditMongo::logDropAllRolesFromDatabase(Client* client,
                                                    const DatabaseName& dbname) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kDropAllRolesFromDatabase,
         [dbname](BSONObjBuilder* builder) {
             builder->append(
                 kDBField,
                 DatabaseNameUtil::serialize(dbname, SerializationContext::stateDefault()));
         },
         ErrorCodes::OK});
}

void audit::AuditMongo::logGrantRolesToRole(Client* client,
                                            const RoleName& role,
                                            const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromRole(client, role, roles, AuditEventType::kGrantRolesToRole);
}

void audit::AuditMongo::logRevokeRolesFromRole(Client* client,
                                               const RoleName& role,
                                               const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromRole(client, role, roles, AuditEventType::kRevokeRolesFromRole);
}

void audit::AuditMongo::logGrantPrivilegesToRole(Client* client,
                                                 const RoleName& role,
                                                 const PrivilegeVector& privileges) const {
    logGrantRevokePrivilegesToFromRole(
        client, role, privileges, AuditEventType::kGrantPrivilegesToRole);
}

void audit::AuditMongo::logRevokePrivilegesFromRole(Client* client,
                                                    const RoleName& role,
                                                    const PrivilegeVector& privileges) const {
    logGrantRevokePrivilegesToFromRole(
        client, role, privileges, AuditEventType::kRevokePrivilegesFromRole);
}

}  // namespace mongo
