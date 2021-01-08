/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_features_gen.h"
#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager_global.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {
namespace {
constexpr auto kReasonField = "reason"_sd;
constexpr auto kInitialUsersField = "initialUsers"_sd;
constexpr auto kUpdatedUsersField = "updatedUsers"_sd;
}  // namespace

void audit::logLogout(Client* client,
                      StringData reason,
                      const BSONArray& initialUsers,
                      const BSONArray& updatedUsers) {
    if (!getGlobalAuditManager()->enabled ||
        !gFeatureFlagImprovedAuditing.isEnabledAndIgnoreFCV()) {
        return;
    }

    AuditEvent event(client, AuditEventType::logout, [&](BSONObjBuilder* builder) {
        builder->append(kReasonField, reason);
        builder->append(kInitialUsersField, initialUsers);
        builder->append(kUpdatedUsersField, updatedUsers);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
