/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_features_gen.h"
#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kOptionsField = "options"_sd;
}  // namespace

void audit::logStartupOptions(Client* client, const BSONObj& startupOptions) {
    if (!gFeatureFlagImprovedAuditing.isEnabledAndIgnoreFCV()) {
        return;
    }

    tryLogEvent(client,
                AuditEventType::kStartup,
                [&](BSONObjBuilder* builder) { builder->append(kOptionsField, startupOptions); },
                ErrorCodes::OK);
}

}  // namespace mongo
