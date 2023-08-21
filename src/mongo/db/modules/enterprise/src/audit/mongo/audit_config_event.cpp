/**
 *    Copyright (C) 2023 10gen Inc.
 */

#include "mongo/db/modules/enterprise/src/audit/mongo/audit_mongo.h"
#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {
constexpr auto kPreviousParam = "previous"_sd;
constexpr auto kConfigParam = "config"_sd;
}  // namespace

void AuditMongo::logConfigEvent(Client* client, const AuditConfigDocument& config) const {
    logEvent(AuditMongo::AuditEventMongo(
        {client,
         AuditEventType::kAuditConfigure,
         [&](BSONObjBuilder* params) {
             BSONObjBuilder previous(params->subobjStart(kPreviousParam));
             auto* am = getGlobalAuditManager();
             auto prevConfig = am->getConfig();
             stdx::visit(
                 OverloadedVisitor{
                     [&](std::monostate) {
                         // Uninitialized
                         if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
                                 serverGlobalParams.featureCompatibility)) {
                             previous.append(AuditConfigDocument::kClusterParameterTimeFieldName,
                                             LogicalTime::kUninitialized.asTimestamp());
                         } else {
                             previous.append(AuditConfigDocument::kGenerationFieldName, OID());
                         }
                     },
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

             BSONObjBuilder configBuilder(params->subobjStart(kConfigParam));
             config.serialize(&configBuilder);
             stdx::visit(
                 OverloadedVisitor{
                     [&](std::monostate) {
                         // If this is coming from a resetConfiguration, the new config will have
                         // empty generation and cluster time. In this case, we need to append the
                         // uninitialized generation/cluster time (based on the feature flag) to
                         // keep the audit log consistent.
                         if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
                                 serverGlobalParams.featureCompatibility)) {
                             configBuilder.append(
                                 AuditConfigDocument::kClusterParameterTimeFieldName,
                                 LogicalTime::kUninitialized.asTimestamp());
                         } else {
                             configBuilder.append(AuditConfigDocument::kGenerationFieldName, OID());
                         }
                     },
                     [](auto) {}},
                 AuditManager::parseGenerationOrTimestamp(config));
             configBuilder.doneFast();
         },
         ErrorCodes::OK}));
}

}  // namespace mongo::audit
