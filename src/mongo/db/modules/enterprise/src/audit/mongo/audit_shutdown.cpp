/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

void audit::AuditMongo::logShutdown(Client* client) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client, AuditEventType::kShutdown, [](auto*) {}, ErrorCodes::OK});
}

}  // namespace mongo
