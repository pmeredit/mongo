/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_deduplication.h"
#include "audit/audit_event_type.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/client.h"
#include "mongo/db/repl/replication_coordinator.h"

namespace mongo::audit {
namespace {

constexpr auto kAuthenticationRestrictionsField = "authenticationRestrictions"_sd;
constexpr auto kCustomDataField = "customData"_sd;
constexpr auto kDirectAuthMutation = "directAuthMutation"_sd;
constexpr auto kDocumentField = "document"_sd;
constexpr auto kNamespaceField = "namespace"_sd;

using AuditDeduplicationOCSF = AuditDeduplication<AuditOCSF::AuditEventOCSF>;

void logCreateUpdateUser(Client* client,
                         const UserName& username,
                         bool password,
                         const BSONObj* customData,
                         const std::vector<RoleName>* roles,
                         const boost::optional<BSONArray>& restrictions,
                         int activityType) {
    AuditDeduplicationOCSF::tryAuditEventAndMark(
        {client,
         audit::ocsf::OCSFEventCategory::kIdentityAndAccess,
         audit::ocsf::OCSFEventClass::kAccountChange,
         activityType,
         audit::ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             if (roles) {
                 AuditOCSF::AuditEventOCSF::_buildUser(builder, username, *roles);
             } else {
                 AuditOCSF::AuditEventOCSF::_buildUser(builder, username);
             }

             if (restrictions || customData) {
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 if (activityType == ocsf::kAccountChangeActivityCreate) {
                     unmapped.append(ocsf::kATypeFieldName,
                                     AuditEventType_serializer(AuditEventType::kCreateUser));
                 } else {
                     unmapped.append(ocsf::kATypeFieldName,
                                     AuditEventType_serializer(AuditEventType::kUpdateUser));
                 }
                 if (customData) {
                     unmapped.append(kCustomDataField, *customData);
                 }
                 if (restrictions) {
                     unmapped.append(kAuthenticationRestrictionsField, *restrictions);
                 }
                 unmapped.doneFast();
             }
         },
         ErrorCodes::OK});
}

}  // namespace

void AuditOCSF::logDirectAuthOperation(Client* client,
                                       const NamespaceString& nss,
                                       const BSONObj& doc,
                                       DirectAuthOperation operation) const {
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
    switch (operation) {
        case DirectAuthOperation::kCreate:
        case DirectAuthOperation::kInsert:
            activityId = ocsf::kAccountChangeActivityCreate;
            break;
        case DirectAuthOperation::kUpdate:
        case DirectAuthOperation::kRename:
            activityId = ocsf::kAccountChangeActivityOther;
            break;
        case DirectAuthOperation::kRemove:
        case DirectAuthOperation::kDrop:
            activityId = ocsf::kAccountChangeActivityDelete;
            break;
    }

    AuditDeduplicationOCSF::tryAuditEventAndMark(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         activityId,
         ocsf::kSeverityCritical,
         [&](BSONObjBuilder* builder) {
             AuditEventOCSF::_buildUser(builder, doc, nss.tenantId());
             AuditEventOCSF::_buildNetwork(client, builder);
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kDirectAuthMutation));
             {
                 BSONObjBuilder dam(unmapped.subobjStart(kDirectAuthMutation));
                 dam.append(
                     kNamespaceField,
                     NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));

                 {
                     BSONObjBuilder docBuilder(dam.subobjStart(kDocumentField));
                     sanitizeCredentialsAuditDoc(&docBuilder, doc);
                     docBuilder.doneFast();
                 }
                 dam.doneFast();
             }
             unmapped.doneFast();
         },
         ErrorCodes::OK});
}

void AuditOCSF::logCreateUser(Client* client,
                              const UserName& username,
                              bool password,
                              const BSONObj* customData,
                              const std::vector<RoleName>& roles,
                              const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateUser(client,
                        username,
                        password,
                        customData,
                        &roles,
                        restrictions,
                        ocsf::kAccountChangeActivityCreate);
}

void AuditOCSF::logDropUser(Client* client, const UserName& username) const {
    AuditDeduplicationOCSF::tryAuditEventAndMark(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         ocsf::kAccountChangeActivityDelete,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) { AuditOCSF::AuditEventOCSF::_buildUser(builder, username); },
         ErrorCodes::OK});
}

void AuditOCSF::logDropAllUsersFromDatabase(Client* client, const DatabaseName& dbname) const {
    AuditDeduplicationOCSF::tryAuditEventAndMark(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kAccountChange,
         ocsf::kAccountChangeActivityDelete,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kDropAllUsersFromDatabase));
             unmapped.append(
                 "allUsersFromDatabase"_sd,
                 DatabaseNameUtil::serialize(dbname, SerializationContext::stateDefault()));
         },
         ErrorCodes::OK});
}

void AuditOCSF::logUpdateUser(Client* client,
                              const UserName& username,
                              bool password,
                              const BSONObj* customData,
                              const std::vector<RoleName>* roles,
                              const boost::optional<BSONArray>& restrictions) const {
    // We should improve the calling semantics of logUpdateUser to better understand what has been
    // changed. With that information, we can choose a better activity than the generic "other".
    auto activity =
        password ? ocsf::kAccountChangeActivityPasswordChange : ocsf::kAccountChangeActivityOther;
    logCreateUpdateUser(client, username, password, customData, roles, restrictions, activity);
}

void AuditOCSF::logInsertOperation(Client* client,
                                   const NamespaceString& nss,
                                   const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, DirectAuthOperation::kCreate);
}

void AuditOCSF::logUpdateOperation(Client* client,
                                   const NamespaceString& nss,
                                   const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, DirectAuthOperation::kUpdate);
}

void AuditOCSF::logRemoveOperation(Client* client,
                                   const NamespaceString& nss,
                                   const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, DirectAuthOperation::kRemove);
}

}  // namespace mongo::audit
