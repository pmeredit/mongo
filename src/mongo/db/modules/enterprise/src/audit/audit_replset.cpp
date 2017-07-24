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

class ReplSetReconfigEvent : public AuditEvent {
public:
    ReplSetReconfigEvent(const AuditEventEnvelope& envelope,
                         const BSONObj* oldConfig,
                         const BSONObj* newConfig)
        : AuditEvent(envelope), _oldConfig(oldConfig), _newConfig(newConfig) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        if (_oldConfig) {
            builder.append("old", *_oldConfig);
        }
        verify(_newConfig);
        builder.append("new", *_newConfig);
        return builder;
    }

    const BSONObj* _oldConfig;
    const BSONObj* _newConfig;
};

}  // namespace
}  // namespace audit

void audit::logReplSetReconfig(Client* client, const BSONObj* oldConfig, const BSONObj* newConfig) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    ReplSetReconfigEvent event(
        makeEnvelope(client, ActionType::replSetReconfig, ErrorCodes::OK), oldConfig, newConfig);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo
