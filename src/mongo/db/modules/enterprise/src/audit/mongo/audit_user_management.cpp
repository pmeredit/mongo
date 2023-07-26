/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_deduplication.h"
#include "audit/audit_event_type.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/address_restriction.h"
#include "mongo/db/client.h"
#include "mongo/db/repl/replication_coordinator.h"

namespace mongo {

namespace audit {
namespace {

constexpr auto kAuthenticationRestrictionsField = "authenticationRestrictions"_sd;
constexpr auto kCustomDataField = "customData"_sd;
constexpr auto kDBField = "db"_sd;
constexpr auto kDocumentField = "document"_sd;
constexpr auto kNSField = "ns"_sd;
constexpr auto kOperationField = "operation"_sd;
constexpr auto kPasswordChangedField = "passwordChanged"_sd;
constexpr auto kRolesField = "roles"_sd;

using AuditDeduplicationMongo = AuditDeduplication<AuditMongo::AuditEventMongo>;

void logCreateUpdateUser(Client* client,
                         const UserName& username,
                         bool password,
                         const BSONObj* customData,
                         const std::vector<RoleName>* roles,
                         const boost::optional<BSONArray>& restrictions,
                         AuditEventType aType) {
    AuditDeduplicationMongo::tryAuditEventAndMark(
        client,
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
        ErrorCodes::OK);
}

void sanitizeCredentials(BSONObjBuilder* builder, const BSONObj& doc) {
    constexpr StringData kCredentials = "credentials"_sd;

    for (const auto& it : doc) {
        if (kCredentials == it.fieldName()) {
            BSONArrayBuilder cb(builder->subarrayStart(kCredentials));
            for (const auto& it2 : it.Obj()) {
                cb.append(it2.fieldName());
            }
        } else {
            builder->append(it);
        }
    }
}

bool isStandaloneOrPrimary(OperationContext* opCtx) {
    auto replCoord = repl::ReplicationCoordinator::get(opCtx);
    return !replCoord->getSettings().isReplSet() || replCoord->getMemberState().primary();
}

}  // namespace

void logDirectAuthOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc,
                            StringData operation) {
    if (!nss.isPrivilegeCollection()) {
        return;
    }

    if (AuditDeduplicationMongo::wasOperationAlreadyAudited(client)) {
        return;
    }

    if (!isStandaloneOrPrimary(client->getOperationContext())) {
        return;
    }

    AuditDeduplicationMongo::tryAuditEventAndMark(
        client,
        AuditEventType::kDirectAuthMutation,
        [&](BSONObjBuilder* builder) {
            BSONObjBuilder documentObjectBuilder(builder->subobjStart(audit::kDocumentField));
            sanitizeCredentials(&documentObjectBuilder, doc);
            documentObjectBuilder.done();

            builder->append(audit::kNSField, NamespaceStringUtil::serialize(nss));
            builder->append(audit::kOperationField, operation);
        },
        ErrorCodes::OK);
}

}  // namespace audit

void audit::AuditMongo::logCreateUser(Client* client,
                                      const UserName& username,
                                      bool password,
                                      const BSONObj* customData,
                                      const std::vector<RoleName>& roles,
                                      const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateUser(
        client, username, password, customData, &roles, restrictions, AuditEventType::kCreateUser);
}

void audit::AuditMongo::logDropUser(Client* client, const UserName& username) const {
    AuditDeduplicationMongo::tryAuditEventAndMark(
        client,
        AuditEventType::kDropUser,
        [&](BSONObjBuilder* builder) { username.appendToBSON(builder); },
        ErrorCodes::OK);
}

void audit::AuditMongo::logDropAllUsersFromDatabase(Client* client,
                                                    const DatabaseName& dbname) const {
    AuditDeduplicationMongo::tryAuditEventAndMark(
        client,
        AuditEventType::kDropAllUsersFromDatabase,
        [dbname](BSONObjBuilder* builder) {
            builder->append(kDBField, DatabaseNameUtil::serialize(dbname));
        },
        ErrorCodes::OK);
}

void audit::AuditMongo::logUpdateUser(Client* client,
                                      const UserName& username,
                                      bool password,
                                      const BSONObj* customData,
                                      const std::vector<RoleName>* roles,
                                      const boost::optional<BSONArray>& restrictions) const {
    logCreateUpdateUser(
        client, username, password, customData, roles, restrictions, AuditEventType::kUpdateUser);
}

void audit::AuditMongo::logInsertOperation(Client* client,
                                           const NamespaceString& nss,
                                           const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, "insert"_sd);
}

void audit::AuditMongo::logUpdateOperation(Client* client,
                                           const NamespaceString& nss,
                                           const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, "update"_sd);
}

void audit::AuditMongo::logRemoveOperation(Client* client,
                                           const NamespaceString& nss,
                                           const BSONObj& doc) const {
    logDirectAuthOperation(client, nss, doc, "remove"_sd);
}


}  // namespace mongo
