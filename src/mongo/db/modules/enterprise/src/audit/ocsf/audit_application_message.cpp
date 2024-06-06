/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_constants.h"
#include "audit/ocsf/ocsf_process_activity_constants.h"
#include "audit_ocsf.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {

constexpr auto messageId = "msg"_sd;

}  // namespace

void AuditOCSF::logApplicationMessage(Client* client, StringData msg) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kSystemActivity,
         ocsf::OCSFEventClass::kProcessActivity,
         kProcessActivityOther,
         ocsf::kSeverityInformational,
         [msg](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kApplicationMessage));
             unmapped.append(messageId, msg);
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
