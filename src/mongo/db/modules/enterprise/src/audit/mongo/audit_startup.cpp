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
#include "mongo/idl/cluster_server_parameter_gen.h"

namespace mongo {

namespace {
constexpr auto kOptionsField = "startupOptions"_sd;
constexpr auto kInitialClusterServerParametersField = "initialClusterServerParameters"_sd;
}  // namespace

void audit::AuditMongo::logStartupOptions(Client* client, const BSONObj& startupOptions) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kStartup,
         [&](BSONObjBuilder* builder) {
             builder->append(kOptionsField, startupOptions);
             const auto& clusterParametersMap =
                 ServerParameterSet::getClusterParameterSet()->getMap();
             std::vector<BSONObj> clusterParametersBSON;
             clusterParametersBSON.reserve(clusterParametersMap.size());

             for (const auto& sp : clusterParametersMap) {
                 if (sp.second->isEnabled()) {
                     BSONObjBuilder bob;
                     sp.second->append(client->getOperationContext(), &bob, sp.first, boost::none);
                     clusterParametersBSON.emplace_back(bob.obj());
                 }
             }
             builder->append(kInitialClusterServerParametersField, clusterParametersBSON);
         },
         ErrorCodes::OK});
}

}  // namespace mongo
