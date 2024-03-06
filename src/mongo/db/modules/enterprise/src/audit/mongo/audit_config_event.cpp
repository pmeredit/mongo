/**
 *    Copyright (C) 2023 10gen Inc.
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

void logUninitialized(BSONObjBuilder* builder) {
    if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
            serverGlobalParams.featureCompatibility)) {
        builder->append(audit::AuditConfigDocument::kClusterParameterTimeFieldName,
                        LogicalTime::kUninitialized.asTimestamp());
    } else {
        builder->append(audit::AuditConfigDocument::kGenerationFieldName, OID());
    }
}

}  // namespace

// Implemented as a helper function to allow AuditOCSF to include the same data
// in its unmapped field for this event type.
void audit::logMongoConfigEvent(BSONObjBuilder* builder, const AuditConfigDocument& config) {
    {
        BSONObjBuilder previous(builder->subobjStart(kPreviousParam));
        auto* am = getGlobalAuditManager();
        auto prevConfig = am->getConfig();
        stdx::visit(OverloadedVisitor{
                        [&](std::monostate) { logUninitialized(&previous); },
                        [&](const OID& oid) {
                            previous.append(AuditConfigDocument::kGenerationFieldName, oid);
                        },
                        [&](const LogicalTime& time) {
                            previous.append(AuditConfigDocument::kClusterParameterTimeFieldName,
                                            time.asTimestamp());
                        }},
                    prevConfig->generationOrTimestamp);

        previous.append(AuditConfigDocument::kFilterFieldName, prevConfig->filterBSON);
        previous.append(AuditConfigDocument::kAuditAuthorizationSuccessFieldName,
                        prevConfig->auditAuthorizationSuccess.load());
        previous.doneFast();
    }
    {
        BSONObjBuilder configBuilder(builder->subobjStart(kConfigParam));
        config.serialize(&configBuilder);
        stdx::visit(OverloadedVisitor{[&](std::monostate) {
                                          // If this is coming from a resetConfiguration, the new
                                          // config will have empty generation and cluster time. In
                                          // this case, we need to append the uninitialized
                                          // generation/cluster time (based on the feature flag) to
                                          // keep the audit log consistent.
                                          logUninitialized(&configBuilder);
                                      },
                                      [](auto) {}},
                    AuditManager::parseGenerationOrTimestamp(config));
        configBuilder.doneFast();
    }
}

void audit::AuditMongo::logConfigEvent(Client* client, const AuditConfigDocument& config) const {
    logEvent(AuditMongo::AuditEventMongo(
        {client,
         AuditEventType::kAuditConfigure,
         [&](BSONObjBuilder* params) { logMongoConfigEvent(params, config); },
         ErrorCodes::OK}));
}

}  // namespace mongo
