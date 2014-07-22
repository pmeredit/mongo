/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
namespace audit {

    class GrantRolesToUserEvent : public AuditEvent {
    public:
        GrantRolesToUserEvent(const AuditEventEnvelope& envelope,
                              const UserName& username,
                              const std::vector<RoleName>& roles)
            : AuditEvent(envelope), _username(username), _roles(roles) {}
        virtual ~GrantRolesToUserEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const UserName _username;
        const std::vector<RoleName> _roles;
    };

    std::ostream& GrantRolesToUserEvent::putTextDescription(std::ostream& os) const {
        os << "Granted to user " << _username << " the roles";
        bool first = true;
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            if (first) {
                os << ": " << *role;
                first = false;
            }
            else {
               os << ", " << *role;
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& GrantRolesToUserEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
                               << AuthorizationManager::ROLE_DB_FIELD_NAME << role->getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    void logGrantRolesToUser(ClientBasic* client,
                             const UserName& username,
                             const std::vector<RoleName>& roles) {

        if (!getGlobalAuditManager()->enabled) return;

        GrantRolesToUserEvent event(
                makeEnvelope(client, ActionType::grantRolesToUser, ErrorCodes::OK),
                username,
                roles);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class RevokeRolesFromUserEvent : public AuditEvent {
    public:
        RevokeRolesFromUserEvent(const AuditEventEnvelope& envelope,
                                 const UserName& username,
                                 const std::vector<RoleName>& roles)
            : AuditEvent(envelope), _username(username), _roles(roles) {}
        virtual ~RevokeRolesFromUserEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const UserName _username;
        const std::vector<RoleName> _roles;
    };

    std::ostream& RevokeRolesFromUserEvent::putTextDescription(std::ostream& os) const {
        os << "Revoked from user " << _username << " the roles";
        bool first = true;
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            if (first) {
                os << ": " << *role;
                first = false;
            }
            else {
               os << ", " << *role;
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& RevokeRolesFromUserEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
                               << AuthorizationManager::ROLE_DB_FIELD_NAME << role->getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    void logRevokeRolesFromUser(ClientBasic* client,
                                const UserName& username,
                                const std::vector<RoleName>& roles) {

        if (!getGlobalAuditManager()->enabled) return;

        RevokeRolesFromUserEvent event(
                makeEnvelope(client, ActionType::revokeRolesFromUser, ErrorCodes::OK),
                username,
                roles);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class CreateRoleEvent : public AuditEvent {
    public:
        CreateRoleEvent(const AuditEventEnvelope& envelope,
                        const RoleName& role,
                        const std::vector<RoleName>& roles,
                        const PrivilegeVector& privileges)
            : AuditEvent(envelope), _role(role), _roles(roles), _privileges(privileges) {}
        virtual ~CreateRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const std::vector<RoleName> _roles;
        const PrivilegeVector _privileges;
    };

    std::ostream& CreateRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Created role " << _role << " with the roles";
        bool first = true;
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            if (first) {
                os << ": " << *role;
                first = false;
            }
            else {
               os << ", " << *role;
            }
        }
        os << " and the privileges";
        ParsedPrivilege printable;
        std::string trash;
        for (PrivilegeVector::const_iterator privilege = _privileges.begin();
                    privilege != _privileges.end();
                    privilege++) {
            fassert(4031,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
            if (first) {
                os << ": " << printable.toString();
                first = false;
            }
            else {
               os << ", " << printable.toString();
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& CreateRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
                               << AuthorizationManager::ROLE_DB_FIELD_NAME << role->getDB()));
        }
        roleArray.doneFast();
        BSONArrayBuilder privilegeArray(builder.subarrayStart("privileges"));
        ParsedPrivilege printable;
        std::string trash;
        for (PrivilegeVector::const_iterator privilege = _privileges.begin();
                    privilege != _privileges.end();
                    privilege++) {
            fassert(4024,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
            privilegeArray.append(printable.toBSON());
        }
        privilegeArray.doneFast();
        return builder;
    }

    void logCreateRole(ClientBasic* client,
                       const RoleName& role,
                       const std::vector<RoleName>& roles,
                       const PrivilegeVector& privileges) {

        if (!getGlobalAuditManager()->enabled) return;

        CreateRoleEvent event(
                makeEnvelope(client, ActionType::createRole, ErrorCodes::OK),
                role,
                roles,
                privileges);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class UpdateRoleEvent : public AuditEvent {
    public:
        UpdateRoleEvent(const AuditEventEnvelope& envelope,
                        const RoleName& role,
                        const std::vector<RoleName>* roles,
                        const PrivilegeVector* privileges)
            : AuditEvent(envelope), _role(role), _roles(roles), _privileges(privileges) {}
        virtual ~UpdateRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const std::vector<RoleName>* _roles;
        const PrivilegeVector* _privileges;
    };

    std::ostream& UpdateRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Updated role " << _role;
        bool first = true;
        if (_roles) {
            for (std::vector<RoleName>::const_iterator role = _roles->begin();
                        role != _roles->end();
                        role++) {
                if (first) {
                    os << " with the roles: " << *role;
                    first = false;
                }
                else {
                   os << ", " << *role;
                }
            }
        }
        if (!first && _privileges && !_privileges->empty()) {
            // if there was at least one role and there is at least one privilege
            os << " and";
            first = true;
        }
        ParsedPrivilege printable;
        std::string trash;
        if (_privileges) {
            for (PrivilegeVector::const_iterator privilege = _privileges->begin();
                        privilege != _privileges->end();
                        privilege++) {
                fassert(4025,
                        ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
                if (first) {
                    os << " with the privileges: " << printable.toString();
                    first = false;
                }
                else {
                   os << ", " << printable.toString();
                }
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& UpdateRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        if (_roles && !_roles->empty()) {
            BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
            for (std::vector<RoleName>::const_iterator role = _roles->begin();
                        role != _roles->end();
                        role++) {
                roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
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
                fassert(4026,
                        ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
                privilegeArray.append(printable.toBSON());
            }
            privilegeArray.doneFast();
        }
        return builder;
    }

    void logUpdateRole(ClientBasic* client,
                       const RoleName& role,
                       const std::vector<RoleName>* roles,
                       const PrivilegeVector* privileges) {

        if (!getGlobalAuditManager()->enabled) return;

        UpdateRoleEvent event(
                makeEnvelope(client, ActionType::updateRole, ErrorCodes::OK),
                role,
                roles,
                privileges);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropRoleEvent : public AuditEvent {
    public:
        DropRoleEvent(const AuditEventEnvelope& envelope,
                      const RoleName& role)
            : AuditEvent(envelope), _role(role) {}
        virtual ~DropRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const std::vector<RoleName> _roles;
    };

    std::ostream& DropRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped role " << _role << '.';
        return os;
    }

    BSONObjBuilder& DropRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        return builder;
    }

    void logDropRole(ClientBasic* client,
                     const RoleName& role) {

        if (!getGlobalAuditManager()->enabled) return;

        DropRoleEvent event(
                makeEnvelope(client, ActionType::dropRole, ErrorCodes::OK),
                role);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropAllRolesFromDatabaseEvent : public AuditEvent {
    public:
        DropAllRolesFromDatabaseEvent(const AuditEventEnvelope& envelope,
                                     const StringData& dbname)
            : AuditEvent(envelope), _dbname(dbname) {}
        virtual ~DropAllRolesFromDatabaseEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _dbname;
    };

    std::ostream& DropAllRolesFromDatabaseEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped all roles from " << _dbname << '.';
        return os;
    }

    BSONObjBuilder& DropAllRolesFromDatabaseEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("db", _dbname);
        return builder;
    }

    void logDropAllRolesFromDatabase(ClientBasic* client,
                                    const StringData& dbname) {

        if (!getGlobalAuditManager()->enabled) return;

        DropAllRolesFromDatabaseEvent event(
                makeEnvelope(client, ActionType::dropAllRolesFromDatabase, ErrorCodes::OK),
                dbname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class GrantRolesToRoleEvent : public AuditEvent {
    public:
        GrantRolesToRoleEvent(const AuditEventEnvelope& envelope,
                              const RoleName& role,
                              const std::vector<RoleName>& roles)
            : AuditEvent(envelope), _role(role), _roles(roles) {}
        virtual ~GrantRolesToRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const std::vector<RoleName> _roles;
    };

    std::ostream& GrantRolesToRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Granted to role " << _role << " the roles";
        bool first = true;
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            if (first) {
                os << ": " << *role;
                first = false;
            }
            else {
               os << ", " << *role;
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& GrantRolesToRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
                               << AuthorizationManager::ROLE_DB_FIELD_NAME << role->getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    void logGrantRolesToRole(ClientBasic* client,
                             const RoleName& role,
                             const std::vector<RoleName>& roles) {

        if (!getGlobalAuditManager()->enabled) return;

        GrantRolesToRoleEvent event(
                makeEnvelope(client, ActionType::grantRolesToRole, ErrorCodes::OK),
                role,
                roles);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class RevokeRolesFromRoleEvent : public AuditEvent {
    public:
        RevokeRolesFromRoleEvent(const AuditEventEnvelope& envelope,
                                 const RoleName& role,
                                 const std::vector<RoleName>& roles)
            : AuditEvent(envelope), _role(role), _roles(roles) {}
        virtual ~RevokeRolesFromRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const std::vector<RoleName> _roles;
    };

    std::ostream& RevokeRolesFromRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Revoked from role " << _role << " the roles";
        bool first = true;
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            if (first) {
                os << ": " << *role;
                first = false;
            }
            else {
               os << ", " << *role;
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& RevokeRolesFromRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, _role.getRole());
        builder.append(AuthorizationManager::ROLE_DB_FIELD_NAME, _role.getDB());
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
                               << AuthorizationManager::ROLE_DB_FIELD_NAME << role->getDB()));
        }
        roleArray.doneFast();
        return builder;
    }

    void logRevokeRolesFromRole(ClientBasic* client,
                                const RoleName& role,
                                const std::vector<RoleName>& roles) {

        if (!getGlobalAuditManager()->enabled) return;

        RevokeRolesFromRoleEvent event(
                makeEnvelope(client, ActionType::revokeRolesFromRole, ErrorCodes::OK),
                role,
                roles);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class GrantPrivilegesToRoleEvent : public AuditEvent {
    public:
        GrantPrivilegesToRoleEvent(const AuditEventEnvelope& envelope,
                                   const RoleName& role,
                                   const PrivilegeVector& privileges)
            : AuditEvent(envelope), _role(role), _privileges(privileges) {}
        virtual ~GrantPrivilegesToRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const PrivilegeVector _privileges;
    };

    std::ostream& GrantPrivilegesToRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Granted to role " << _role << " the privileges";
        bool first = true;
        ParsedPrivilege printable;
        std::string trash;
        for (PrivilegeVector::const_iterator privilege = _privileges.begin();
                    privilege != _privileges.end();
                    privilege++) {
            fassert(4027,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
            if (first) {
                os << ": " << printable.toString();
                first = false;
            }
            else {
               os << ", " << printable.toString();
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& GrantPrivilegesToRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
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

    void logGrantPrivilegesToRole(ClientBasic* client,
                                  const RoleName& role,
                                  const PrivilegeVector& privileges) {

        if (!getGlobalAuditManager()->enabled) return;

        GrantPrivilegesToRoleEvent event(
                makeEnvelope(client, ActionType::grantPrivilegesToRole, ErrorCodes::OK),
                role,
                privileges);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class RevokePrivilegesFromRoleEvent : public AuditEvent {
    public:
        RevokePrivilegesFromRoleEvent(const AuditEventEnvelope& envelope,
                                      const RoleName& role,
                                      const PrivilegeVector& privileges)
            : AuditEvent(envelope), _role(role), _privileges(privileges) {}
        virtual ~RevokePrivilegesFromRoleEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const RoleName _role;
        const PrivilegeVector _privileges;
    };

    std::ostream& RevokePrivilegesFromRoleEvent::putTextDescription(std::ostream& os) const {
        os << "Revoked from role " << _role << " the privileges";
        bool first = true;
        ParsedPrivilege printable;
        std::string trash;
        for (PrivilegeVector::const_iterator privilege = _privileges.begin();
                    privilege != _privileges.end();
                    privilege++) {
            fassert(4029,
                    ParsedPrivilege::privilegeToParsedPrivilege(*privilege, &printable, &trash));
            if (first) {
                os << ": " << printable.toString();
                first = false;
            }
            else {
               os << ", " << printable.toString();
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& RevokePrivilegesFromRoleEvent::putParamsBSON(BSONObjBuilder& builder) const {
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

    void logRevokePrivilegesFromRole(ClientBasic* client,
                                     const RoleName& role,
                                     const PrivilegeVector& privileges) {

        if (!getGlobalAuditManager()->enabled) return;

        RevokePrivilegesFromRoleEvent event(
                makeEnvelope(client, ActionType::revokePrivilegesFromRole, ErrorCodes::OK),
                role,
                privileges);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
