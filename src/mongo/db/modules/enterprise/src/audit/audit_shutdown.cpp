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
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

namespace audit {
namespace {

class ShutdownEvent : public AuditEvent {
public:
    ShutdownEvent(const AuditEventEnvelope& envelope) : AuditEvent(envelope) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        return builder;
    }
};

}  // namespace
}  // namespace audit

void audit::logShutdown(Client* client) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    ShutdownEvent event(makeEnvelope(client, ActionType::shutdown, ErrorCodes::OK));
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo
