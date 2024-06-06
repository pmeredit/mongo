/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_log.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_constants.h"
#include "audit/ocsf/ocsf_process_activity_constants.h"

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"

namespace mongo::audit {

namespace {

constexpr auto kStartupOptions = "startup_options"_sd;
constexpr auto kClusterParameters = "cluster_parameters"_sd;


}  // namespace

void AuditOCSF::logStartupOptions(Client* client, const BSONObj& startupOptions) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kSystemActivity,
         ocsf::OCSFEventClass::kProcessActivity,
         kProcessActivityLaunch,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildProcess(builder);
             AuditOCSF::AuditEventOCSF::_buildDevice(builder);

             builder->append(kStatusId, ocsf::kStatusSuccess);


             {
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 unmapped.append(ocsf::kATypeFieldName,
                                 AuditEventType_serializer(AuditEventType::kStartup));

                 unmapped.append(kStartupOptions, startupOptions);

                 auto clusterParametersMap = ServerParameterSet::getClusterParameterSet()->getMap();
                 std::vector<BSONObj> clusterParametersBSON;
                 clusterParametersBSON.reserve(clusterParametersMap.size());

                 for (const auto& sp : clusterParametersMap) {
                     if (sp.second->isEnabled()) {
                         BSONObjBuilder bob;
                         sp.second->append(
                             client->getOperationContext(), &bob, sp.first, boost::none);
                         clusterParametersBSON.emplace_back(bob.obj());
                     }
                 }

                 unmapped.append(kClusterParameters, clusterParametersBSON);
             }
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
