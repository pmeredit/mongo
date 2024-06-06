/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {
constexpr auto kOldField = "old"_sd;
constexpr auto kNewField = "new"_sd;
}  // namespace

void AuditOCSF::logReplSetReconfig(Client* client,
                                   const BSONObj* oldConfig,
                                   const BSONObj* newConfig) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityHigh,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kReplSetReconfig));
             if (oldConfig) {
                 unmapped.append(kOldField, *oldConfig);
             }
             invariant(newConfig);
             unmapped.append(kNewField, *newConfig);
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
