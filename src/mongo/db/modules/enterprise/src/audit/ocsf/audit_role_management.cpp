/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/parsed_privilege_gen.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {

constexpr auto kAuthenticationRestrictionsField = "authenticationRestrictions"_sd;
constexpr auto kDBField = "db"_sd;
constexpr auto kPrivilegesField = "privileges"_sd;
constexpr auto kRoleField = "role"_sd;
constexpr auto kRolesField = "roles"_sd;

void _buildPrivilegesArray(BSONObjBuilder* builder, const PrivilegeVector& privs) {
    BSONArrayBuilder privsBuilder(builder->subarrayStart(kPrivilegesField));

    for (const auto& priv : privs) {
        BSONObjBuilder p(privsBuilder.subobjStart());
        priv.toParsedPrivilege().serialize(&p);
        p.doneFast();
    }

    privsBuilder.doneFast();
}

void logCreateUpdateRole(Client* client,
                         const RoleName& role,
                         const std::vector<RoleName>* roles,
                         const PrivilegeVector* privileges,
                         const boost::optional<BSONArray>& restrictions,
                         int activityId) {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         activityId,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             if (activityId == ocsf::kAccountChangeActivityCreate) {
                 unmapped.append(ocsf::kATypeFieldName,
                                 AuditEventType_serializer(AuditEventType::kCreateRole));
             } else {
                 unmapped.append(ocsf::kATypeFieldName,
                                 AuditEventType_serializer(AuditEventType::kUpdateRole));
             }
             unmapped.append(kRoleField, role.getUnambiguousName());
             if (roles) {
                 BSONArrayBuilder rolesBuilder(unmapped.subarrayStart(kRolesField));
                 for (const auto& r : *roles) {
                     rolesBuilder.append(r.getUnambiguousName());
                 }
                 rolesBuilder.doneFast();
             }

             if (privileges) {
                 _buildPrivilegesArray(&unmapped, *privileges);
             }

             if (restrictions && !restrictions->isEmpty()) {
                 unmapped.append(kAuthenticationRestrictionsField, restrictions.value());
             }
         },
         ErrorCodes::OK});
}

struct GrantAction {
    static constexpr int kActivity = ocsf::kAccountChangeActivityAttachPolicy;
    static constexpr auto kRolesToFromUser = "grantRolesToUser"_sd;
    static constexpr auto kRolesToFromRole = "grantRolesToRole"_sd;
    static constexpr auto kPrivilegesToFromRole = "grantPrivilegesToRole"_sd;
};

struct RevokeAction {
    static constexpr int kActivity = ocsf::kAccountChangeActivityDetachPolicy;
    static constexpr auto kRolesToFromUser = "revokeRolesFromUser"_sd;
    static constexpr auto kRolesToFromRole = "revokeRolesFromRole"_sd;
    static constexpr auto kPrivilegesToFromRole = "revokePrivilegesFromRole"_sd;
};

template <class Action>
void logGrantRevokeRolesToFromUser(Client* client,
                                   const UserName& username,
                                   const std::vector<RoleName>& roles) {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         Action::kActivity,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildUser(builder, username);

             {
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 unmapped.append(ocsf::kATypeFieldName, Action::kRolesToFromUser);
                 {
                     BSONArrayBuilder rolesBuilder(unmapped.subarrayStart(kRolesField));
                     for (const auto& r : roles) {
                         rolesBuilder.append(r.getUnambiguousName());
                     }
                     rolesBuilder.doneFast();
                 }
                 unmapped.doneFast();
             }
         },
         ErrorCodes::OK});
}

template <class Action>
void logGrantRevokeRolesToFromRole(Client* client,
                                   const RoleName& role,
                                   const std::vector<RoleName>& roles) {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         Action::kActivity,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             {
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 unmapped.append(ocsf::kATypeFieldName, Action::kRolesToFromRole);
                 unmapped.append(kRoleField, role.getUnambiguousName());
                 {
                     BSONArrayBuilder rolesBuilder(unmapped.subarrayStart(kRolesField));
                     for (const auto& r : roles) {
                         rolesBuilder.append(r.getUnambiguousName());
                     }
                     rolesBuilder.doneFast();
                 }
                 unmapped.doneFast();
             }
         },
         ErrorCodes::OK});
}

template <class Action>
void logGrantRevokePrivilegesToFromRole(Client* client,
                                        const RoleName& role,
                                        const PrivilegeVector& privileges) {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         Action::kActivity,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             {
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 unmapped.append(ocsf::kATypeFieldName, Action::kPrivilegesToFromRole);
                 unmapped.append(kRoleField, role.getUnambiguousName());
                 _buildPrivilegesArray(&unmapped, privileges);
             }
         },
         ErrorCodes::OK});
}

}  // namespace

void AuditOCSF::logCreateRole(Client* client,
                              const RoleName& role,
                              const std::vector<RoleName>& roles,
                              const PrivilegeVector& privileges,
                              const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateRole(
        client, role, &roles, &privileges, restrictions, ocsf::kAccountChangeActivityCreate);
}

void AuditOCSF::logUpdateRole(Client* client,
                              const RoleName& role,
                              const std::vector<RoleName>* roles,
                              const PrivilegeVector* privileges,
                              const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateRole(
        client, role, roles, privileges, restrictions, ocsf::kAccountChangeActivityOther);
}

void AuditOCSF::logDropRole(Client* client, const RoleName& role) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         ocsf::kAccountChangeActivityDelete,
         audit::ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kDropRole));
             unmapped.append(kRoleField, role.getUnambiguousName());
         },
         ErrorCodes::OK});
}

void AuditOCSF::logDropAllRolesFromDatabase(Client* client, const DatabaseName& dbname) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         ocsf::kAccountChangeActivityDelete,
         audit::ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kDropAllRolesFromDatabase));
             unmapped.append(
                 kDBField,
                 DatabaseNameUtil::serialize(dbname, SerializationContext::stateDefault()));
         },
         ErrorCodes::OK});
}

void AuditOCSF::logGrantRolesToUser(Client* client,
                                    const UserName& username,
                                    const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromUser<GrantAction>(client, username, roles);
}

void AuditOCSF::logRevokeRolesFromUser(Client* client,
                                       const UserName& username,
                                       const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromUser<RevokeAction>(client, username, roles);
}

void AuditOCSF::logGrantRolesToRole(Client* client,
                                    const RoleName& role,
                                    const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromRole<GrantAction>(client, role, roles);
}

void AuditOCSF::logRevokeRolesFromRole(Client* client,
                                       const RoleName& role,
                                       const std::vector<RoleName>& roles) const {
    logGrantRevokeRolesToFromRole<RevokeAction>(client, role, roles);
}

void AuditOCSF::logGrantPrivilegesToRole(Client* client,
                                         const RoleName& role,
                                         const PrivilegeVector& privileges) const {
    logGrantRevokePrivilegesToFromRole<GrantAction>(client, role, privileges);
}

void AuditOCSF::logRevokePrivilegesFromRole(Client* client,
                                            const RoleName& role,
                                            const PrivilegeVector& privileges) const {
    logGrantRevokePrivilegesToFromRole<RevokeAction>(client, role, privileges);
}

}  // namespace mongo::audit
