/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kOldField = "old"_sd;
constexpr auto kNewField = "new"_sd;
}  // namespace

void audit::AuditMongo::logReplSetReconfig(Client* client,
                                           const BSONObj* oldConfig,
                                           const BSONObj* newConfig) const {
    tryLogEvent<AuditMongo::AuditEventMongo>({client,
                                              AuditEventType::kReplSetReconfig,
                                              [&](BSONObjBuilder* builder) {
                                                  if (oldConfig) {
                                                      builder->append(kOldField, *oldConfig);
                                                  }
                                                  invariant(newConfig);
                                                  builder->append(kNewField, *newConfig);
                                              },
                                              ErrorCodes::OK});
}

}  // namespace mongo
