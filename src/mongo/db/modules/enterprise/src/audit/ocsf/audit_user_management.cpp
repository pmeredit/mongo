/**
 *    Copyright (C) 2023 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_deduplication.h"
#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {


constexpr auto kAccountChangeActivityUnknown = 0;
constexpr auto kAccountChangeActivityCreate = 1;
constexpr auto kAccountChangeActivityDelete = 6;
constexpr auto kAccountChangeActivityOther = 99;

constexpr auto kSeverityCritical = 5;

constexpr auto kUnmappedField = "unmapped"_sd;
constexpr auto kNamespaceField = "namespace"_sd;
constexpr auto kOperationField = "operation"_sd;


}  // namespace

using AuditDeduplicationOCSF = AuditDeduplication<AuditOCSF::AuditEventOCSF>;

void logDirectAuthApplicationOCSF(Client* client,
                                  const NamespaceString& nss,
                                  const BSONObj& doc,
                                  StringData operation) {
    if (!nss.isPrivilegeCollection()) {
        return;
    }

    if (AuditDeduplicationOCSF::wasOperationAlreadyAudited(client)) {
        return;
    }

    if (!isStandaloneOrPrimary(client->getOperationContext())) {
        return;
    }

    ActivityId activityId;

    if (operation == "insert"_sd) {
        activityId = kAccountChangeActivityCreate;
    } else if (operation == "update"_sd) {
        activityId = kAccountChangeActivityOther;
    } else if (operation == "remove"_sd) {
        activityId = kAccountChangeActivityDelete;
    } else {
        activityId = kAccountChangeActivityUnknown;
    }

    AuditDeduplicationOCSF::tryAuditEventAndMark(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         activityId,
         kSeverityCritical,
         [&](BSONObjBuilder* builder) {
             {
                 BSONObjBuilder documentObjectBuilder(builder->subobjStart(kUnmappedField));
                 sanitizeCredentialsAuditDoc(&documentObjectBuilder, doc);
                 documentObjectBuilder.append(kNamespaceField, NamespaceStringUtil::serialize(nss));
                 documentObjectBuilder.append(kOperationField, operation);
             }

             AuditOCSF::AuditEventOCSF::_buildUser(client, builder);
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
         },
         ErrorCodes::OK});
}


}  // namespace mongo::audit
