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

class StartupEvent : public AuditEvent {
public:
    StartupEvent(const AuditEventEnvelope& envelope, const BSONObj& startupOptions)
        : AuditEvent(envelope), _startupOptions(startupOptions) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("options", _startupOptions);
        return builder;
    }

    const BSONObj& _startupOptions;
};
}  // namespace
}  // namespace audit

void audit::logStartupOptions(Client* client, const BSONObj& startupOptions) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    StartupEvent event(makeEnvelope(client, AuditEventType::startup, ErrorCodes::OK),
                       startupOptions);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
