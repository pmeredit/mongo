/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/mongo/audit_mongo.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/db/client.h"

namespace mongo::audit {

void AuditOCSF::logConfigEvent(Client* client, const AuditConfigDocument& config) const {
    logEvent(AuditOCSF::AuditEventOCSF(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* params) {
             BSONObjBuilder unmapped(params->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kAuditConfigure));
             buildMongoConfigEventParams(&unmapped, config);
         },
         ErrorCodes::OK}));
}

}  // namespace mongo::audit
