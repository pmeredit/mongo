/**
 *    Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
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

void audit::AuditMongo::logGetClusterParameter(
    Client* client,
    const std::variant<std::string, std::vector<std::string>>& requestedParameters) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kGetClusterParameter,
         [&](BSONObjBuilder* builder) {
             visit(OverloadedVisitor{
                       [&](const std::string& strParameterValue) {
                           builder->append(kRequestedParametersField, strParameterValue);
                       },
                       [&](const std::vector<std::string>& listParameterNames) {
                           builder->append(kRequestedParametersField, listParameterNames);
                       }},
                   requestedParameters);
         },
         ErrorCodes::OK});
}

void audit::AuditMongo::logSetClusterParameter(Client* client,
                                               const BSONObj& oldValue,
                                               const BSONObj& newValue,
                                               const boost::optional<TenantId>& tenantId) const {
    tryLogEvent<AuditMongo::AuditEventMongo>({client,
                                              AuditEventType::kSetClusterParameter,
                                              [&](BSONObjBuilder* builder) {
                                                  builder->append(kOriginalParameterField,
                                                                  oldValue);
                                                  builder->append(kUpdatedParameterField, newValue);
                                              },
                                              ErrorCodes::OK,
                                              tenantId});
}

void audit::AuditMongo::logUpdateCachedClusterParameter(
    Client* client,
    const BSONObj& oldValue,
    const BSONObj& newValue,
    const boost::optional<TenantId>& tenantId) const {
    tryLogEvent<AuditMongo::AuditEventMongo>({client,
                                              AuditEventType::kUpdateCachedClusterServerParameter,
                                              [&](BSONObjBuilder* builder) {
                                                  builder->append(kOriginalParameterField,
                                                                  oldValue);
                                                  builder->append(kUpdatedParameterField, newValue);
                                              },
                                              ErrorCodes::OK,
                                              tenantId});
}

}  // namespace mongo
