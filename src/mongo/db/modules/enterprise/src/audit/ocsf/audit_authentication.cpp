/**
 *    Copyright (C) 2023 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"

namespace mongo::audit {
namespace {

constexpr auto kAuthenticationActivityLogon = 1;

constexpr auto kUnmappedField = "unmapped"_sd;
constexpr auto kAuthProtocolField = "auth_protocol"_sd;

}  // namespace

void audit::AuditOCSF::logAuthentication(Client* client, const AuthenticateEvent& authEvent) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAuthentication,
         kAuthenticationActivityLogon,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildUser(builder, authEvent.getUser());
             builder->append(kAuthProtocolField, authEvent.getMechanism());

             {
                 BSONObjBuilder unmapped(builder->subobjStart(kUnmappedField));
                 authEvent.appendExtraInfo(&unmapped);
             }
         },
         authEvent.getResult()});
}

}  // namespace mongo::audit
