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

namespace mongo::audit {

namespace {
constexpr auto kMsgField = "msg"_sd;
}  // namespace

void AuditMongo::logApplicationMessage(Client* client, StringData msg) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kApplicationMessage,
         [msg](BSONObjBuilder* builder) { builder->append(kMsgField, msg); },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
