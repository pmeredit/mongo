/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_constants.h"
#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"

namespace mongo::audit {
namespace {

constexpr auto kAuthenticationActivityLogon = 1;

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
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 unmapped.append(ocsf::kATypeFieldName,
                                 AuditEventType_serializer(AuditEventType::kAuthenticate));
                 authEvent.appendExtraInfo(&unmapped);
             }
         },
         authEvent.getResult()});
}

}  // namespace mongo::audit
