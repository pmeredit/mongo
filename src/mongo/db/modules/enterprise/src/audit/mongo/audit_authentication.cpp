/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
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

void audit::AuditMongo::logAuthentication(Client* client,
                                          const AuthenticateEvent& authEvent) const {
    tryLogEvent<AuditMongo::AuditEventMongo>({client,
                                              AuditEventType::kAuthenticate,
                                              [&](BSONObjBuilder* builder) {
                                                  const auto& user = authEvent.getUser();
                                                  authEvent.appendExtraInfo(builder);
                                                  builder->append(kUser, user.getUser());
                                                  builder->append(kDatabase, user.getDB());
                                                  builder->append(kMechanism,
                                                                  authEvent.getMechanism());
                                              },
                                              authEvent.getResult()});
}

}  // namespace mongo
