/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kPreviousParam = "previous"_sd;
constexpr auto kConfigParam = "config"_sd;
}  // namespace

// Implemented as a helper function to allow AuditOCSF to include the same data
// in its unmapped field for this event type.
void audit::buildMongoConfigEventParams(BSONObjBuilder* builder,
                                        const AuditConfigDocument& config) {
    {
        BSONObjBuilder previous(builder->subobjStart(kPreviousParam));
        auto* am = getGlobalAuditManager();
        auto prevConfig = am->getConfig();

        previous.append(AuditConfigDocument::kClusterParameterTimeFieldName,
                        prevConfig->timestamp.asTimestamp());
        previous.append(AuditConfigDocument::kFilterFieldName, prevConfig->filterBSON);
        previous.append(AuditConfigDocument::kAuditAuthorizationSuccessFieldName,
                        prevConfig->auditAuthorizationSuccess.load());
        previous.doneFast();
    }
    {
        BSONObjBuilder configBuilder(builder->subobjStart(kConfigParam));
        config.serialize(&configBuilder);
        configBuilder.doneFast();
    }
}

void audit::AuditMongo::logConfigEvent(Client* client, const AuditConfigDocument& config) const {
    logEvent(AuditMongo::AuditEventMongo(
        {client,
         AuditEventType::kAuditConfigure,
         [&](BSONObjBuilder* params) { buildMongoConfigEventParams(params, config); },
         ErrorCodes::OK}));
}

}  // namespace mongo
