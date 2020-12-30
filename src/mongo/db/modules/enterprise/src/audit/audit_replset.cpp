/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log.h"
#include "audit_manager_global.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kOldField = "old"_sd;
constexpr auto kNewField = "new"_sd;
}  // namespace

void audit::logReplSetReconfig(Client* client, const BSONObj* oldConfig, const BSONObj* newConfig) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::replSetReconfig, [&](BSONObjBuilder* builder) {
        if (oldConfig) {
            builder->append(kOldField, *oldConfig);
        }
        verify(newConfig);
        builder->append(kNewField, *newConfig);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
