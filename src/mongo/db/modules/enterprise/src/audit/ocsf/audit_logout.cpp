/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {
namespace {
constexpr auto kAuthenticationActivityLogoff = 2;
constexpr auto kUserField = "user"_sd;
constexpr auto kMessageField = "message"_sd;

}  // namespace

void audit::AuditOCSF::logLogout(Client* client,
                                 StringData reason,
                                 const BSONArray& initialUsers,
                                 const BSONArray& updatedUsers,
                                 const boost::optional<Date_t>& loginTime) const {
    using namespace fmt::literals;
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAuthentication,
         kAuthenticationActivityLogoff,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             // Building actor already takes care of user, but OCSF
             // requires us to have user as a top level attribute so
             // so we have to build it twice.
             AuditOCSF::AuditEventOCSF::_buildUser(builder, client);
             builder->append(kMessageField, "Reason: {}"_format(reason));
             if (loginTime)
                 builder->append(ocsf::kUnmappedFieldName,
                                 BSON("loginTime" << loginTime->toMillisSinceEpoch()));
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
