/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kMsgField = "msg"_sd;
}  // namespace

void audit::logApplicationMessage(Client* client, StringData msg) {
    if (!getGlobalAuditManager()->isEnabled()) {
        return;
    }

    AuditEvent event(client, AuditEventType::kApplicationMessage, [msg](BSONObjBuilder* builder) {
        builder->append(kMsgField, msg);
    });

    if (getGlobalAuditManager()->shouldAudit(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
