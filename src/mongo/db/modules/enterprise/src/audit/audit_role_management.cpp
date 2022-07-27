/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege_parser.h"
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
    tryLogEvent(client,
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
                ErrorCodes::OK);
}

void logCreateUpdateRole(Client* client,
                         const RoleName& role,
                         const std::vector<RoleName>* roles,
                         const PrivilegeVector* privileges,
                         const boost::optional<BSONArray>& restrictions,
                         AuditEventType aType) {
    tryLogEvent(client,
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
                        ParsedPrivilege printable;
                        std::string trash;
                        for (const auto& privilege : *privileges) {
                            fassert(4024,
                                    ParsedPrivilege::privilegeToParsedPrivilege(
                                        privilege, &printable, &trash));
                            privilegeArray.append(printable.toBSON());
                        }
                        privilegeArray.doneFast();
                    }

                    if (restrictions && !restrictions->isEmpty()) {
                        builder->append("authenticationRestrictions", restrictions.value());
                    }
                },
                ErrorCodes::OK);
}

void logGrantRevokeRolesToFromRole(Client* client,
                                   const RoleName& role,
                                   const std::vector<RoleName>& roles,
                                   AuditEventType aType) {
    tryLogEvent(client,
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
                ErrorCodes::OK);
}

void logGrantRevokePrivilegesToFromRole(Client* client,
                                        const RoleName& role,
                                        const PrivilegeVector& privileges,
                                        AuditEventType aType) {
    tryLogEvent(
        client,
        aType,
        [&](BSONObjBuilder* builder) {
            role.appendToBSON(builder);

            BSONArrayBuilder privilegeArray(builder->subarrayStart(kPrivilegesField));
            ParsedPrivilege printable;
            std::string trash;
            for (const auto& privilege : privileges) {
                fassert(4028,
                        ParsedPrivilege::privilegeToParsedPrivilege(privilege, &printable, &trash));
                privilegeArray.append(printable.toBSON());
            }
            privilegeArray.doneFast();
        },
        ErrorCodes::OK);
}

}  // namespace
}  // namespace audit

void audit::logGrantRolesToUser(Client* client,
                                const UserName& username,
                                const std::vector<RoleName>& roles) {
    logGrantRevokeRolesToFromUser(client, username, roles, AuditEventType::kGrantRolesToUser);
}

void audit::logRevokeRolesFromUser(Client* client,
                                   const UserName& username,
                                   const std::vector<RoleName>& roles) {
    logGrantRevokeRolesToFromUser(client, username, roles, AuditEventType::kRevokeRolesFromUser);
}

void audit::logCreateRole(Client* client,
                          const RoleName& role,
                          const std::vector<RoleName>& roles,
                          const PrivilegeVector& privileges,
                          const boost::optional<BSONArray>& restrictions) {
    logCreateUpdateRole(
        client, role, &roles, &privileges, restrictions, AuditEventType::kCreateRole);
}

void audit::logUpdateRole(Client* client,
                          const RoleName& role,
                          const std::vector<RoleName>* roles,
                          const PrivilegeVector* privileges,
                          const boost::optional<BSONArray>& restrictions) {
    logCreateUpdateRole(client, role, roles, privileges, restrictions, AuditEventType::kUpdateRole);
}

void audit::logDropRole(Client* client, const RoleName& role) {
    tryLogEvent(client,
                AuditEventType::kDropRole,
                [&](BSONObjBuilder* builder) { role.appendToBSON(builder); },
                ErrorCodes::OK);
}

void audit::logDropAllRolesFromDatabase(Client* client, StringData dbname) {
    tryLogEvent(client,
                AuditEventType::kDropAllRolesFromDatabase,
                [dbname](BSONObjBuilder* builder) { builder->append(kDBField, dbname); },
                ErrorCodes::OK);
}

void audit::logGrantRolesToRole(Client* client,
                                const RoleName& role,
                                const std::vector<RoleName>& roles) {
    logGrantRevokeRolesToFromRole(client, role, roles, AuditEventType::kGrantRolesToRole);
}

void audit::logRevokeRolesFromRole(Client* client,
                                   const RoleName& role,
                                   const std::vector<RoleName>& roles) {
    logGrantRevokeRolesToFromRole(client, role, roles, AuditEventType::kRevokeRolesFromRole);
}

void audit::logGrantPrivilegesToRole(Client* client,
                                     const RoleName& role,
                                     const PrivilegeVector& privileges) {
    logGrantRevokePrivilegesToFromRole(
        client, role, privileges, AuditEventType::kGrantPrivilegesToRole);
}

void audit::logRevokePrivilegesFromRole(Client* client,
                                        const RoleName& role,
                                        const PrivilegeVector& privileges) {
    logGrantRevokePrivilegesToFromRole(
        client, role, privileges, AuditEventType::kRevokePrivilegesFromRole);
}

}  // namespace mongo
