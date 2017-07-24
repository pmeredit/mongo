/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege_parser.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

namespace audit {
namespace {

class GrantRolesToUserEvent : public AuditEvent {
public:
    GrantRolesToUserEvent(const AuditEventEnvelope& envelope,
                          const UserName& username,
                          const std::vector<RoleName>& roles)
        : AuditEvent(envelope), _username(username), _roles(roles) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (const auto& role : _roles) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                  << role.getRole()
                                  << AuthorizationManager::ROLE_DB_FIELD_NAME
                                  << role.getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    const UserName _username;
    const std::vector<RoleName> _roles;
};

class RevokeRolesFromUserEvent : public AuditEvent {
public:
    RevokeRolesFromUserEvent(const AuditEventEnvelope& envelope,
                             const UserName& username,
                             const std::vector<RoleName>& roles)
        : AuditEvent(envelope), _username(username), _roles(roles) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (const auto& role : _roles) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                  << role.getRole()
                                  << AuthorizationManager::ROLE_DB_FIELD_NAME
                                  << role.getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    const UserName _username;
    const std::vector<RoleName> _roles;
};

class CreateRoleEvent : public AuditEvent {
public:
    CreateRoleEvent(const AuditEventEnvelope& envelope,
                    const RoleName& role,
                    const std::vector<RoleName>& roles,
                    const PrivilegeVector& privileges,
                    const boost::optional<BSONArray>& restrictions)
        : AuditEvent(envelope),
          _role(role),
          _roles(roles),
          _privileges(privileges),
          _restrictions(restrictions) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin(); role != _roles.end();
             role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                  << role->getRole()
                                  << AuthorizationManager::ROLE_DB_FIELD_NAME
                                  << role->getDB()));
        }
        roleArray.doneFast();
        BSONArrayBuilder privilegeArray(builder.subarrayStart("privileges"));
        ParsedPrivilege printable;
        std::string trash;
        for (const auto& privilege : _privileges) {
            fassert(4024,
                    ParsedPrivilege::privilegeToParsedPrivilege(privilege, &printable, &trash));
            privilegeArray.append(printable.toBSON());
        }
        privilegeArray.doneFast();
        if (_restrictions && !_restrictions->isEmpty()) {
            builder.append("authenticationRestrictions", _restrictions.get());
        }
        return builder;
    }

    const RoleName _role;
    const std::vector<RoleName> _roles;
    const PrivilegeVector _privileges;
    const boost::optional<BSONArray> _restrictions;
};

class UpdateRoleEvent : public AuditEvent {
public:
    UpdateRoleEvent(const AuditEventEnvelope& envelope,
                    const RoleName& role,
                    const std::vector<RoleName>* roles,
                    const PrivilegeVector* privileges,
                    const boost::optional<BSONArray>& restrictions)
        : AuditEvent(envelope),
          _role(role),
          _roles(roles),
          _privileges(privileges),
          _restrictions(restrictions) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        if (_roles && !_roles->empty()) {
            BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
            for (std::vector<RoleName>::const_iterator role = _roles->begin();
                 role != _roles->end();
                 role++) {
                roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                      << role->getRole()
                                      << AuthorizationManager::ROLE_DB_FIELD_NAME
                                      << role->getDB()));
            }
            roleArray.doneFast();
        }
        if (_privileges && !_privileges->empty()) {
            BSONArrayBuilder privilegeArray(builder.subarrayStart("privileges"));
            ParsedPrivilege printable;
            std::string trash;
            for (PrivilegeVector::const_iterator privilege = _privileges->begin();
                 privilege != _privileges->end();
                 privilege++) {
                fassert(
                    4026,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
                privilegeArray.append(printable.toBSON());
            }
            privilegeArray.doneFast();
        }

        if (_restrictions && !_restrictions->isEmpty()) {
            builder.append("authenticationRestrictions", _restrictions.get());
        }

        return builder;
    }

    const RoleName _role;
    const std::vector<RoleName>* _roles;
    const PrivilegeVector* _privileges;
    const boost::optional<BSONArray> _restrictions;
};


class DropRoleEvent : public AuditEvent {
public:
    DropRoleEvent(const AuditEventEnvelope& envelope, const RoleName& role)
        : AuditEvent(envelope), _role(role) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        return builder;
    }

    const RoleName _role;
    const std::vector<RoleName> _roles;
};

class DropAllRolesFromDatabaseEvent : public AuditEvent {
public:
    DropAllRolesFromDatabaseEvent(const AuditEventEnvelope& envelope, StringData dbname)
        : AuditEvent(envelope), _dbname(dbname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("db", _dbname);
        return builder;
    }

    StringData _dbname;
};

class GrantRolesToRoleEvent : public AuditEvent {
public:
    GrantRolesToRoleEvent(const AuditEventEnvelope& envelope,
                          const RoleName& role,
                          const std::vector<RoleName>& roles)
        : AuditEvent(envelope), _role(role), _roles(roles) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin(); role != _roles.end();
             role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                  << role->getRole()
                                  << AuthorizationManager::ROLE_DB_FIELD_NAME
                                  << role->getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    const RoleName _role;
    const std::vector<RoleName> _roles;
};

class RevokeRolesFromRoleEvent : public AuditEvent {
public:
    RevokeRolesFromRoleEvent(const AuditEventEnvelope& envelope,
                             const RoleName& role,
                             const std::vector<RoleName>& roles)
        : AuditEvent(envelope), _role(role), _roles(roles) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin(); role != _roles.end();
             role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                  << role->getRole()
                                  << AuthorizationManager::ROLE_DB_FIELD_NAME
                                  << role->getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    const RoleName _role;
    const std::vector<RoleName> _roles;
};

class GrantPrivilegesToRoleEvent : public AuditEvent {
public:
    GrantPrivilegesToRoleEvent(const AuditEventEnvelope& envelope,
                               const RoleName& role,
                               const PrivilegeVector& privileges)
        : AuditEvent(envelope), _role(role), _privileges(privileges) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder privilegeArray(builder.subarrayStart("privileges"));
        ParsedPrivilege printable;
        std::string trash;
        for (PrivilegeVector::const_iterator privilege = _privileges.begin();
             privilege != _privileges.end();
             privilege++) {
            fassert(4028,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
            privilegeArray.append(printable.toBSON());
        }
        privilegeArray.doneFast();
        return builder;
    }


    const RoleName _role;
    const PrivilegeVector _privileges;
};

class RevokePrivilegesFromRoleEvent : public AuditEvent {
public:
    RevokePrivilegesFromRoleEvent(const AuditEventEnvelope& envelope,
                                  const RoleName& role,
                                  const PrivilegeVector& privileges)
        : AuditEvent(envelope), _role(role), _privileges(privileges) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder privilegeArray(builder.subarrayStart("privileges"));
        ParsedPrivilege printable;
        std::string trash;
        for (PrivilegeVector::const_iterator privilege = _privileges.begin();
             privilege != _privileges.end();
             privilege++) {
            fassert(4030,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
            privilegeArray.append(printable.toBSON());
        }
        privilegeArray.doneFast();
        return builder;
    }

    const RoleName _role;
    const PrivilegeVector _privileges;
};

}  // namespace
}  // namespace audit

void audit::logGrantRolesToUser(Client* client,
                                const UserName& username,
                                const std::vector<RoleName>& roles) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    GrantRolesToUserEvent event(
        makeEnvelope(client, ActionType::grantRolesToUser, ErrorCodes::OK), username, roles);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logRevokeRolesFromUser(Client* client,
                                   const UserName& username,
                                   const std::vector<RoleName>& roles) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    RevokeRolesFromUserEvent event(
        makeEnvelope(client, ActionType::revokeRolesFromUser, ErrorCodes::OK), username, roles);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logCreateRole(Client* client,
                          const RoleName& role,
                          const std::vector<RoleName>& roles,
                          const PrivilegeVector& privileges,
                          const boost::optional<BSONArray>& restrictions) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    CreateRoleEvent event(makeEnvelope(client, ActionType::createRole, ErrorCodes::OK),
                          role,
                          roles,
                          privileges,
                          restrictions);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logUpdateRole(Client* client,
                          const RoleName& role,
                          const std::vector<RoleName>* roles,
                          const PrivilegeVector* privileges,
                          const boost::optional<BSONArray>& restrictions) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    UpdateRoleEvent event(makeEnvelope(client, ActionType::updateRole, ErrorCodes::OK),
                          role,
                          roles,
                          privileges,
                          restrictions);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logDropRole(Client* client, const RoleName& role) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropRoleEvent event(makeEnvelope(client, ActionType::dropRole, ErrorCodes::OK), role);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logDropAllRolesFromDatabase(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropAllRolesFromDatabaseEvent event(
        makeEnvelope(client, ActionType::dropAllRolesFromDatabase, ErrorCodes::OK), dbname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logGrantRolesToRole(Client* client,
                                const RoleName& role,
                                const std::vector<RoleName>& roles) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    GrantRolesToRoleEvent event(
        makeEnvelope(client, ActionType::grantRolesToRole, ErrorCodes::OK), role, roles);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logRevokeRolesFromRole(Client* client,
                                   const RoleName& role,
                                   const std::vector<RoleName>& roles) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    RevokeRolesFromRoleEvent event(
        makeEnvelope(client, ActionType::revokeRolesFromRole, ErrorCodes::OK), role, roles);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logGrantPrivilegesToRole(Client* client,
                                     const RoleName& role,
                                     const PrivilegeVector& privileges) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    GrantPrivilegesToRoleEvent event(
        makeEnvelope(client, ActionType::grantPrivilegesToRole, ErrorCodes::OK), role, privileges);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logRevokePrivilegesFromRole(Client* client,
                                        const RoleName& role,
                                        const PrivilegeVector& privileges) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    RevokePrivilegesFromRoleEvent event(
        makeEnvelope(client, ActionType::revokePrivilegesFromRole, ErrorCodes::OK),
        role,
        privileges);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo
