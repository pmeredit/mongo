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

void audit::logShutdown(Client* client) {
    tryLogEvent(client, AuditEventType::kShutdown, [](auto*) {}, ErrorCodes::OK);
}

}  // namespace mongo
