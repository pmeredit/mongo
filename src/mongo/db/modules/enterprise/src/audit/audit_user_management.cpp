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
#include "mongo/db/client_basic.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
namespace audit {

    class CreateUserEvent : public AuditEvent {
    public:
        CreateUserEvent(const AuditEventEnvelope& envelope,
                        const UserName& username,
                        bool password,
                        const BSONObj* customData,
                        const std::vector<RoleName>& roles)
            : AuditEvent(envelope),
              _username(username),
              _password(password),
              _customData(customData),
              _roles(roles) {}
        virtual ~CreateUserEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const UserName _username;
        bool _password;
        const BSONObj* _customData;
        const std::vector<RoleName> _roles;
    };

    std::ostream& CreateUserEvent::putTextDescription(std::ostream& os) const {
        os << "Created user " << _username;
        if (_password) {
            os << " with password,";
        }
        else {
            os << " without password,";
        }
        if (_customData) {
            os << " with customData " << *_customData << ',';
        }
        else {
            os << " without customData,";
        }
        os << " with the following roles:";
        bool first = true;
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            if (first) {
                os << " " << *role;
                first = false;
            }
            else {
               os << ", " << *role;
            }
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& CreateUserEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        if (_customData) {
            builder.append("customData", *_customData);
        }
        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin();
                    role != _roles.end();
                    role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << role->getRole()
                               << AuthorizationManager::ROLE_DB_FIELD_NAME
                               << role->getDB()));
        }
        roleArray.done();
        return builder;
    }

    void logCreateUser(ClientBasic* client,
                       const UserName& username,
                       bool password,
                       const BSONObj* customData,
                       const std::vector<RoleName>& roles) {

        if (!getGlobalAuditManager()->enabled) return;

        CreateUserEvent event(
                makeEnvelope(client, ActionType::createUser, ErrorCodes::OK),
                username,
                password,
                customData,
                roles);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropUserEvent : public AuditEvent {
    public:
        DropUserEvent(const AuditEventEnvelope& envelope,
                      const UserName& username)
            : AuditEvent(envelope), _username(username) {}
        virtual ~DropUserEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const UserName _username;
    };

    std::ostream& DropUserEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped user " << _username << '.';
        return os;
    }

    BSONObjBuilder& DropUserEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());
        return builder;
    }

    void logDropUser(ClientBasic* client, const UserName& username) {

        if (!getGlobalAuditManager()->enabled) return;

        DropUserEvent event(
                makeEnvelope(client, ActionType::dropUser, ErrorCodes::OK),
                username);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropAllUsersFromDatabaseEvent : public AuditEvent {
    public:
        DropAllUsersFromDatabaseEvent(const AuditEventEnvelope& envelope,
                                      const StringData& dbname)
            : AuditEvent(envelope), _dbname(dbname) {}
        virtual ~DropAllUsersFromDatabaseEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _dbname;
    };

    std::ostream& DropAllUsersFromDatabaseEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped all users from " << _dbname << '.';
        return os;
    }

    BSONObjBuilder& DropAllUsersFromDatabaseEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("db", _dbname);
        return builder;
    }

    void logDropAllUsersFromDatabase(ClientBasic* client, const StringData& dbname) {

        if (!getGlobalAuditManager()->enabled) return;

        DropAllUsersFromDatabaseEvent event(
                makeEnvelope(client, ActionType::dropAllUsersFromDatabase, ErrorCodes::OK),
                dbname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class UpdateUserEvent : public AuditEvent {
    public:
        UpdateUserEvent(const AuditEventEnvelope& envelope,
                        const UserName& username,
                        bool password,
                        const BSONObj* customData,
                        const std::vector<RoleName>* roles)
            : AuditEvent(envelope),
              _username(username),
              _password(password),
              _customData(customData),
              _roles(roles) {}
        virtual ~UpdateUserEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const UserName _username;
        bool _password;
        const BSONObj* _customData;
        const std::vector<RoleName>* _roles;
    };

    std::ostream& UpdateUserEvent::putTextDescription(std::ostream& os) const {
        os << "Updated user " << _username;
        if (_password) {
            os << ": password changed";
        }
        if (_customData) {
            os << " with customData " << *_customData << ',';
        }
        bool first = true;
        if (_roles) {
            for (std::vector<RoleName>::const_iterator role = _roles->begin();
                        role != _roles->end();
                        role++) {
                if (first) {
                    os << " with the following roles: " << *role;
                    first = false;
                }
                else {
                   os << ", " << *role;
                }
            }
        }
        os << '.';

        return os;
    }

    BSONObjBuilder& UpdateUserEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        builder.append("passwordChanged", _password);
        if (_customData) {
            builder.append("customData", *_customData);
        }
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
            roleArray.done();
        }
        return builder;
    }

    void logUpdateUser(ClientBasic* client,
                       const UserName& username,
                       bool password,
                       const BSONObj* customData,
                       const std::vector<RoleName>* roles) {

        if (!getGlobalAuditManager()->enabled) return;

        UpdateUserEvent event(
                makeEnvelope(client, ActionType::updateUser, ErrorCodes::OK),
                username,
                password,
                customData,
                roles);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
