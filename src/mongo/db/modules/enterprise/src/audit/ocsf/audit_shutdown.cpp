/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_process_activity_constants.h"
#include "audit_ocsf.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

void AuditOCSF::logShutdown(Client* client) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kSystemActivity,
         ocsf::OCSFEventClass::kProcessActivity,
         kProcessActivityTerminate,
         ocsf::kSeverityInformational,
         [](BSONObjBuilder* builder) { AuditOCSF::AuditEventOCSF::_buildDevice(builder); },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
