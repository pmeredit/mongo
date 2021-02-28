/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kMechanismField = "mechanism"_sd;
}  // namespace

void audit::logAuthentication(Client* client,
                              StringData mechanism,
                              const UserName& user,
                              ErrorCodes::Error result) {
    if (!getGlobalAuditManager()->isEnabled()) {
        return;
    }

    AuditEvent event(client,
                     AuditEventType::authenticate,
                     [&](BSONObjBuilder* builder) {
                         user.appendToBSON(builder);
                         builder->append(kMechanismField, mechanism);
                     },
                     result);

    if (getGlobalAuditManager()->shouldAudit(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
