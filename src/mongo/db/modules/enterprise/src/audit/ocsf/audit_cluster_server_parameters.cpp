/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/idl/cluster_server_parameter_gen.h"
#include "mongo/util/overloaded_visitor.h"

namespace mongo::audit {
namespace {
constexpr auto kStartupParametersField = "startupClusterServerParameters"_sd;
constexpr auto kRequestedParametersField = "requestedClusterServerParameters"_sd;
constexpr auto kOriginalParameterField = "originalClusterServerParameter"_sd;
constexpr auto kUpdatedParameterField = "updatedClusterServerParameter"_sd;

void logSetUpdateClusterParameter(Client* client,
                                  const BSONObj& oldValue,
                                  const BSONObj& newValue,
                                  const boost::optional<TenantId>& tenantId,
                                  AuditEventType aType) {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName, AuditEventType_serializer(aType));
             unmapped.append(kOriginalParameterField, oldValue);
             unmapped.append(kUpdatedParameterField, newValue);
         },
         ErrorCodes::OK,
         tenantId});
}

}  // namespace

void AuditOCSF::logGetClusterParameter(
    Client* client,
    const std::variant<std::string, std::vector<std::string>>& requestedParameters) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kApplicationActivity,
         ocsf::OCSFEventClass::kAPIActivity,
         ocsf::kAPIActivityRead,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             visit(OverloadedVisitor{
                       [&](const std::string& strParameterValue) {
                           unmapped.append(kRequestedParametersField, strParameterValue);
                       },
                       [&](const std::vector<std::string>& listParameterNames) {
                           unmapped.append(kRequestedParametersField, listParameterNames);
                       }},
                   requestedParameters);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logSetClusterParameter(Client* client,
                                       const BSONObj& oldValue,
                                       const BSONObj& newValue,
                                       const boost::optional<TenantId>& tenantId) const {
    logSetUpdateClusterParameter(
        client, oldValue, newValue, tenantId, AuditEventType::kSetClusterParameter);
}

void AuditOCSF::logUpdateCachedClusterParameter(Client* client,
                                                const BSONObj& oldValue,
                                                const BSONObj& newValue,
                                                const boost::optional<TenantId>& tenantId) const {
    logSetUpdateClusterParameter(
        client, oldValue, newValue, tenantId, AuditEventType::kUpdateCachedClusterServerParameter);
}

}  // namespace mongo::audit
