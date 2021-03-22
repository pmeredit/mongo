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
constexpr auto kMechanism = "mechanism"_sd;
constexpr auto kUser = "user"_sd;
constexpr auto kDatabase = "db"_sd;
}  // namespace

void audit::logAuthentication(Client* client, const AuthenticateEvent& authEvent) {
    tryLogEvent(client,
                AuditEventType::kAuthenticate,
                [&](BSONObjBuilder* builder) {
                    authEvent.appendExtraInfo(builder);
                    builder->append(kUser, authEvent.getUser());
                    builder->append(kDatabase, authEvent.getDatabase());
                    builder->append(kMechanism, authEvent.getMechanism());
                },
                authEvent.getResult());
}

}  // namespace mongo
