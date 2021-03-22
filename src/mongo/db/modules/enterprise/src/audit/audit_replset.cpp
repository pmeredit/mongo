/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kOldField = "old"_sd;
constexpr auto kNewField = "new"_sd;
}  // namespace

void audit::logReplSetReconfig(Client* client, const BSONObj* oldConfig, const BSONObj* newConfig) {
    tryLogEvent(client,
                AuditEventType::kReplSetReconfig,
                [&](BSONObjBuilder* builder) {
                    if (oldConfig) {
                        builder->append(kOldField, *oldConfig);
                    }
                    verify(newConfig);
                    builder->append(kNewField, *newConfig);
                },
                ErrorCodes::OK);
}

}  // namespace mongo
