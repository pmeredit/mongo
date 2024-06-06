/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_deduplication.h"
#include "audit/audit_event_type.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/client.h"

namespace mongo::audit {
namespace {

constexpr auto kAuthenticationRestrictionsField = "authenticationRestrictions"_sd;
constexpr auto kCustomDataField = "customData"_sd;
constexpr auto kDBField = "db"_sd;
constexpr auto kDocumentField = "document"_sd;
constexpr auto kNSField = "ns"_sd;
constexpr auto kOperationField = "operation"_sd;
constexpr auto kPasswordChangedField = "passwordChanged"_sd;
constexpr auto kRolesField = "roles"_sd;

void logCreateUpdateUser(Client* client,
                         const UserName& username,
                         bool password,
                         const BSONObj* customData,
                         const std::vector<RoleName>* roles,
                         const boost::optional<BSONArray>& restrictions,
                         AuditEventType aType) {
    AuditDeduplicationMongo::tryAuditEventAndMark(
        {client,
         aType,
         [&](BSONObjBuilder* builder) {
             const bool isCreate = aType == AuditEventType::kCreateUser;
             username.appendToBSON(builder);

             if (!isCreate) {
                 builder->append(kPasswordChangedField, password);
             }

             if (customData) {
                 builder->append(kCustomDataField, *customData);
             }

             if (roles && (isCreate || !roles->empty())) {
                 BSONArrayBuilder roleArray(builder->subarrayStart(kRolesField));
                 for (const auto& role : *roles) {
                     BSONObjBuilder roleBuilder(roleArray.subobjStart());
                     role.appendToBSON(&roleBuilder);
                 }
                 roleArray.done();
             }

             if (restrictions && !restrictions->isEmpty()) {
                 builder->append(kAuthenticationRestrictionsField, restrictions.value());
             }
         },
         ErrorCodes::OK});
}

}  // namespace

void AuditMongo::logDirectAuthOperation(Client* client,
                                        const NamespaceString& nss,
                                        const BSONObj& doc,
                                        DirectAuthOperation operation) const {
    if (!nss.isPrivilegeCollection()) {
        return;
    }

    if (AuditDeduplicationMongo::wasOperationAlreadyAudited(client)) {
        return;
    }

    if (!isStandaloneOrPrimary(client->getOperationContext())) {
        return;
    }

    StringData op = [operation] {
        if (operation == DirectAuthOperation::kInsert) {
            return "insert"_sd;
        } else if (operation == DirectAuthOperation::kUpdate) {
            return "update"_sd;
        } else if (operation == DirectAuthOperation::kRemove) {
            return "remove"_sd;
        } else {
            return "command"_sd;
        }
    }();

    AuditDeduplicationMongo::tryAuditEventAndMark(
        {client,
         AuditEventType::kDirectAuthMutation,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder documentObjectBuilder(builder->subobjStart(audit::kDocumentField));
             sanitizeCredentialsAuditDoc(&documentObjectBuilder, doc);
             documentObjectBuilder.done();

             builder->append(
                 audit::kNSField,
                 NamespaceStringUtil::serialize(nss, SerializationContext::stateDefault()));
             builder->append(audit::kOperationField, op);
         },
         ErrorCodes::OK});
}

void AuditMongo::logCreateUser(Client* client,
                               const UserName& username,
                               bool password,
                               const BSONObj* customData,
                               const std::vector<RoleName>& roles,
                               const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateUser(
        client, username, password, customData, &roles, restrictions, AuditEventType::kCreateUser);
}

void AuditMongo::logDropUser(Client* client, const UserName& username) const {
    AuditDeduplicationMongo::tryAuditEventAndMark(
        {client,
         AuditEventType::kDropUser,
         [&](BSONObjBuilder* builder) { username.appendToBSON(builder); },
         ErrorCodes::OK});
}

void AuditMongo::logDropAllUsersFromDatabase(Client* client, const DatabaseName& dbname) const {
    AuditDeduplicationMongo::tryAuditEventAndMark(
        {client,
         AuditEventType::kDropAllUsersFromDatabase,
         [dbname](BSONObjBuilder* builder) {
             builder->append(
                 kDBField,
                 DatabaseNameUtil::serialize(dbname, SerializationContext::stateDefault()));
         },
         ErrorCodes::OK});
}

void AuditMongo::logUpdateUser(Client* client,
                               const UserName& username,
                               bool password,
                               const BSONObj* customData,
                               const std::vector<RoleName>* roles,
                               const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateUser(
        client, username, password, customData, roles, restrictions, AuditEventType::kUpdateUser);
}

void AuditMongo::logInsertOperation(Client* client,
                                    const NamespaceString& nss,
                                    const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, DirectAuthOperation::kInsert);
}

void AuditMongo::logUpdateOperation(Client* client,
                                    const NamespaceString& nss,
                                    const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, DirectAuthOperation::kUpdate);
}

void AuditMongo::logRemoveOperation(Client* client,
                                    const NamespaceString& nss,
                                    const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, DirectAuthOperation::kRemove);
}

}  // namespace mongo::audit
