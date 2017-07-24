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
#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"
#include "mongo/util/assert_util.h"

namespace mongo {

namespace audit {
namespace {

/**
 * Event representing the result of an authentication activity.
 */
class AuthenticationEvent : public AuditEvent {
public:
    AuthenticationEvent(const AuditEventEnvelope& envelope,
                        StringData mechanism,
                        const UserName& user)
        : AuditEvent(envelope), _mechanism(mechanism), _user(user) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _user.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _user.getDB());
        builder.append("mechanism", _mechanism);
        return builder;
    }

    StringData _mechanism;
    UserName _user;
};

}  // namespace
}  // namespace audit

void audit::logAuthentication(Client* client,
                              StringData mechanism,
                              const UserName& user,
                              ErrorCodes::Error result) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuthenticationEvent event(
        makeEnvelope(client, ActionType::authenticate, result), mechanism, user);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo
