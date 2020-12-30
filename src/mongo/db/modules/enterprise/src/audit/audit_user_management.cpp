/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager_global.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/client.h"

namespace mongo {

namespace audit {
namespace {

constexpr auto kPasswordChangedField = "passwordChanged"_sd;
constexpr auto kCustomDataField = "customData"_sd;
constexpr auto kRolesField = "roles"_sd;
constexpr auto kAuthenticationRestrictionsField = "authenticationRestrictions"_sd;
constexpr auto kDBField = "db"_sd;

void logCreateUpdateUser(Client* client,
                         const UserName& username,
                         bool password,
                         const BSONObj* customData,
                         const std::vector<RoleName>* roles,
                         const boost::optional<BSONArray>& restrictions,
                         AuditEventType aType) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, aType, [&](BSONObjBuilder* builder) {
        const bool isCreate = aType == AuditEventType::createUser;
        username.appendToBSON(builder);

        if (!isCreate) {
            builder->append(kPasswordChangedField, password);
        }

        if (customData) {
            builder->append(kCustomDataField, *customData);
        }

        if (roles && (isCreate || !roles->empty())) {
            BSONArrayBuilder roleArray(builder->subarrayStart(kRolesField));
            for (const auto& role : *roles) {
                BSONObjBuilder roleBuilder(roleArray.subobjStart());
                role.appendToBSON(&roleBuilder);
            }
            roleArray.done();
        }

        if (restrictions && !restrictions->isEmpty()) {
            builder->append(kAuthenticationRestrictionsField, restrictions.get());
        }
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}
}  // namespace
}  // namespace audit

void audit::logCreateUser(Client* client,
                          const UserName& username,
                          bool password,
                          const BSONObj* customData,
                          const std::vector<RoleName>& roles,
                          const boost::optional<BSONArray>& restrictions) {
    logCreateUpdateUser(
        client, username, password, customData, &roles, restrictions, AuditEventType::createUser);
}

void audit::logDropUser(Client* client, const UserName& username) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::dropUser, [&](BSONObjBuilder* builder) {
        username.appendToBSON(builder);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logDropAllUsersFromDatabase(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client,
                     AuditEventType::dropAllUsersFromDatabase,
                     [dbname](BSONObjBuilder* builder) { builder->append(kDBField, dbname); });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logUpdateUser(Client* client,
                          const UserName& username,
                          bool password,
                          const BSONObj* customData,
                          const std::vector<RoleName>* roles,
                          const boost::optional<BSONArray>& restrictions) {
    logCreateUpdateUser(
        client, username, password, customData, roles, restrictions, AuditEventType::updateUser);
}

}  // namespace mongo
