/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {
namespace {
constexpr auto kReasonField = "reason"_sd;
constexpr auto kInitialUsersField = "initialUsers"_sd;
constexpr auto kUpdatedUsersField = "updatedUsers"_sd;
constexpr auto kTimestampField = "loginTime"_sd;
}  // namespace

void audit::AuditMongo::logLogout(Client* client,
                                  StringData reason,
                                  const BSONArray& initialUsers,
                                  const BSONArray& updatedUsers,
                                  const boost::optional<Date_t>& loginTime) const {
    tryLogEvent<AuditMongo::AuditEventMongo>({client,
                                              AuditEventType::kLogout,
                                              [&](BSONObjBuilder* builder) {
                                                  builder->append(kReasonField, reason);
                                                  builder->append(kInitialUsersField, initialUsers);
                                                  builder->append(kUpdatedUsersField, updatedUsers);
                                                  if (loginTime)
                                                      builder->append(kTimestampField, *loginTime);
                                              },
                                              ErrorCodes::OK});
}

}  // namespace mongo
