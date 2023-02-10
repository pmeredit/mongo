/**
 *    Copyright (C) 2022 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/idl/cluster_server_parameter_gen.h"
#include "mongo/util/overloaded_visitor.h"

namespace mongo {
namespace {
constexpr auto kStartupParametersField = "startupClusterServerParameters"_sd;
constexpr auto kRequestedParametersField = "requestedClusterServerParameters"_sd;
constexpr auto kOriginalParameterField = "originalClusterServerParameter"_sd;
constexpr auto kUpdatedParameterField = "updatedClusterServerParameter"_sd;
}  // namespace

void audit::logGetClusterParameter(
    Client* client,
    const stdx::variant<std::string, std::vector<std::string>>& requestedParameters) {
    if (gFeatureFlagClusterWideConfigM2.isEnabled(serverGlobalParams.featureCompatibility)) {
        tryLogEvent(
            client,
            AuditEventType::kGetClusterParameter,
            [&](BSONObjBuilder* builder) {
                stdx::visit(OverloadedVisitor{
                                [&](const std::string& strParameterValue) {
                                    builder->append(kRequestedParametersField, strParameterValue);
                                },
                                [&](const std::vector<std::string>& listParameterNames) {
                                    builder->append(kRequestedParametersField, listParameterNames);
                                }},
                            requestedParameters);
            },
            ErrorCodes::OK);
    }
}

void audit::logSetClusterParameter(Client* client,
                                   const BSONObj& oldValue,
                                   const BSONObj& newValue,
                                   const boost::optional<TenantId>& tenantId) {
    if (gFeatureFlagClusterWideConfigM2.isEnabled(serverGlobalParams.featureCompatibility)) {
        tryLogEvent(
            client,
            AuditEventType::kSetClusterParameter,
            [&](BSONObjBuilder* builder) {
                builder->append(kOriginalParameterField, oldValue);
                builder->append(kUpdatedParameterField, newValue);
            },
            ErrorCodes::OK,
            true, /* overrideTenant */
            tenantId);
    }
}

void audit::logUpdateCachedClusterParameter(Client* client,
                                            const BSONObj& oldValue,
                                            const BSONObj& newValue,
                                            const boost::optional<TenantId>& tenantId) {
    if (gFeatureFlagClusterWideConfigM2.isEnabled(serverGlobalParams.featureCompatibility)) {
        tryLogEvent(
            client,
            AuditEventType::kUpdateCachedClusterServerParameter,
            [&](BSONObjBuilder* builder) {
                builder->append(kOriginalParameterField, oldValue);
                builder->append(kUpdatedParameterField, newValue);
            },
            ErrorCodes::OK,
            true, /* overrideTenant */
            tenantId);
    }
}

}  // namespace mongo
