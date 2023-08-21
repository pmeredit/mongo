/**
 *    Copyright (C) 2023 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_process_activity_constants.h"
#include "audit_ocsf.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {

constexpr auto messageId = "msg"_sd;

}  // namespace

void AuditOCSF::logApplicationMessage(Client* client, StringData msg) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>({client,
                                            ocsf::OCSFEventCategory::kSystemActivity,
                                            ocsf::OCSFEventClass::kProcessActivity,
                                            kProcessActivityOther,
                                            kSeverityInformational,
                                            [msg](BSONObjBuilder* builder) {
                                                BSONObjBuilder unmapped(
                                                    builder->subobjStart(kUnmappedId));
                                                unmapped.append(messageId, msg);
                                            },
                                            ErrorCodes::OK});
}

}  // namespace mongo::audit
