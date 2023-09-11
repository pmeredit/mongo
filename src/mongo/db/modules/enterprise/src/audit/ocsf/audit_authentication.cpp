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
constexpr auto kSeverityInformational = 1;
constexpr auto kRegularUserTypeInt = 1;
constexpr auto kSystemUserTypeInt = 3;
constexpr auto kMessageField = "message"_sd;
constexpr auto kUserField = "user"_sd;
constexpr auto kUnmappedField = "unmapped"_sd;
constexpr auto kUserUIDField = "uid";
constexpr auto kTypeIDField = "type_id"_sd;
constexpr auto kNameField = "name"_sd;
constexpr auto kGroupField = "group"_sd;
constexpr auto kPrivilegesField = "privileges"_sd;
constexpr auto kAuthProtocolField = "auth_protocol"_sd;

}  // namespace

void audit::AuditOCSF::logAuthentication(Client* client, const AuthenticateEvent& authEvent) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAuthentication,
         kAuthenticationActivityLogon,
         kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             const auto& user = authEvent.getUser();

             // Since the client may or may not be auth-d as the user,
             // we need to build a user ourselves out here.
             {
                 BSONObjBuilder userObj(builder->subobjStart(kUserField));
                 userObj.append(kUserUIDField, client->getUUID().toString());
                 if (client->isFromSystemConnection()) {
                     userObj.append(kTypeIDField, kSystemUserTypeInt);
                 } else {
                     userObj.append(kTypeIDField, kRegularUserTypeInt);
                 }
                 userObj.append(kNameField, user.getUnambiguousName());
             }

             builder->append(kAuthProtocolField, authEvent.getMechanism());

             {
                 BSONObjBuilder unmapped(builder->subobjStart(kUnmappedField));
                 authEvent.appendExtraInfo(&unmapped);
             }
         },
         authEvent.getResult()});
}

}  // namespace mongo::audit
