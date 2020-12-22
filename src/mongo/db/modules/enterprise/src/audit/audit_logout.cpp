/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

namespace audit {
namespace {

class LogoutEvent : public AuditEvent {
public:
    LogoutEvent(const AuditEventEnvelope& envelope,
                StringData reason,
                const BSONArray& initialUsers,
                const BSONArray& updatedUsers)
        : AuditEvent(envelope),
          _reason(reason),
          _initialUsers(initialUsers),
          _updatedUsers(updatedUsers) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("reason", _reason);
        builder.append("initialUsers", _initialUsers);
        builder.append("updatedUsers", _updatedUsers);

        return builder;
    }
    const StringData _reason;
    const BSONArray& _initialUsers;
    const BSONArray& _updatedUsers;
};

}  // namespace
}  // namespace audit

void audit::logLogout(Client* client,
                      StringData reason,
                      const BSONArray& initialUsers,
                      const BSONArray& updatedUsers) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    LogoutEvent event(makeEnvelope(client, AuditEventType::logout, ErrorCodes::OK),
                      reason,
                      initialUsers,
                      updatedUsers);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
