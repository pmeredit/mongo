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
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

namespace audit {
namespace {

class CreateUserEvent : public AuditEvent {
public:
    CreateUserEvent(const AuditEventEnvelope& envelope,
                    const UserName& username,
                    bool password,
                    const BSONObj* customData,
                    const std::vector<RoleName>& roles,
                    const boost::optional<BSONArray>& restrictions)
        : AuditEvent(envelope),
          _username(username),
          _password(password),
          _customData(customData),
          _roles(roles),
          _restrictions(restrictions) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());

        if (_customData) {
            builder.append("customData", *_customData);
        }

        BSONArrayBuilder roleArray(builder.subarrayStart("roles"));
        for (std::vector<RoleName>::const_iterator role = _roles.begin(); role != _roles.end();
             role++) {
            roleArray.append(BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME
                                  << role->getRole()
                                  << AuthorizationManager::ROLE_DB_FIELD_NAME
                                  << role->getDB()));
        }
        roleArray.done();

        if (_restrictions && !_restrictions->isEmpty()) {
            builder.append("authenticationRestrictions", _restrictions.get());
        }
        return builder;
    }


    const UserName _username;
    bool _password;
    const BSONObj* _customData;
    const std::vector<RoleName> _roles;
    const boost::optional<BSONArray> _restrictions;
};

class DropUserEvent : public AuditEvent {
public:
    DropUserEvent(const AuditEventEnvelope& envelope, const UserName& username)
        : AuditEvent(envelope), _username(username) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _username.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _username.getDB());
        return builder;
    }

    const UserName _username;
};

class DropAllUsersFromDatabaseEvent : public AuditEvent {
public:
    DropAllUsersFromDatabaseEvent(const AuditEventEnvelope& envelope, StringData dbname)
        : AuditEvent(envelope), _dbname(dbname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("db", _dbname);
        return builder;
    }

    StringData _dbname;
};


class UpdateUserEvent : public AuditEvent {
public:
    UpdateUserEvent(const AuditEventEnvelope& envelope,
                    const UserName& username,
                    bool password,
                    const BSONObj* customData,
                    const std::vector<RoleName>* roles,
                    const boost::optional<BSONArray>& restrictions)
        : AuditEvent(envelope),
          _username(username),
          _password(password),
          _customData(customData),
          _roles(roles),
          _restrictions(restrictions) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
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
        if (_restrictions && !_restrictions->isEmpty()) {
            builder.append("authenticationRestrictions", _restrictions.get());
        }
        return builder;
    }

    const UserName _username;
    bool _password;
    const BSONObj* _customData;
    const std::vector<RoleName>* _roles;
    const boost::optional<BSONArray> _restrictions;
};

}  // namespace
}  // namespace audit

void audit::logCreateUser(Client* client,
                          const UserName& username,
                          bool password,
                          const BSONObj* customData,
                          const std::vector<RoleName>& roles,
                          const boost::optional<BSONArray>& restrictions) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    CreateUserEvent event(makeEnvelope(client, ActionType::createUser, ErrorCodes::OK),
                          username,
                          password,
                          customData,
                          roles,
                          restrictions);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logDropUser(Client* client, const UserName& username) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropUserEvent event(makeEnvelope(client, ActionType::dropUser, ErrorCodes::OK), username);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logDropAllUsersFromDatabase(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropAllUsersFromDatabaseEvent event(
        makeEnvelope(client, ActionType::dropAllUsersFromDatabase, ErrorCodes::OK), dbname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logUpdateUser(Client* client,
                          const UserName& username,
                          bool password,
                          const BSONObj* customData,
                          const std::vector<RoleName>* roles,
                          const boost::optional<BSONArray>& restrictions) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    UpdateUserEvent event(makeEnvelope(client, ActionType::updateUser, ErrorCodes::OK),
                          username,
                          password,
                          customData,
                          roles,
                          restrictions);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo
