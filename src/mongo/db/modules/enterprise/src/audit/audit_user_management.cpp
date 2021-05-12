/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
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

// Instance of this class should be attached to OperationContext during regular user management
// functions. When a CRUD operation is detected on one of privilege collections and context
// is decorated with said instance, consider it a duplicate event and pass
class AuditDeduplication {
public:
    static const OperationContext::Decoration<AuditDeduplication> get;

    static bool wasOperationAlreadyAudited(Client* client) {
        const AuditDeduplication& ad(AuditDeduplication::get(client->getOperationContext()));
        return ad._auditIsDone;
    }

    static void markOperationAsAudited(Client* client) {
        AuditDeduplication& ad(AuditDeduplication::get(client->getOperationContext()));
        ad._auditIsDone = true;
    }

    static void tryAuditEventAndMark(Client* client,
                                     AuditEventType type,
                                     AuditEvent::Serializer serializer,
                                     ErrorCodes::Error code) {
        const auto wasLogged = tryLogEvent(client, type, std::move(serializer), code);
        if (wasLogged) {
            markOperationAsAudited(client);
        }
    }

private:
    bool _auditIsDone = false;
};

const OperationContext::Decoration<AuditDeduplication> AuditDeduplication::get =
    OperationContext::declareDecoration<AuditDeduplication>();

void logCreateUpdateUser(Client* client,
                         const UserName& username,
                         bool password,
                         const BSONObj* customData,
                         const std::vector<RoleName>* roles,
                         const boost::optional<BSONArray>& restrictions,
                         AuditEventType aType) {
    AuditDeduplication::tryAuditEventAndMark(
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
                builder->append(kAuthenticationRestrictionsField, restrictions.get());
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
    const bool isReplSet =
        replCoord->getReplicationMode() == repl::ReplicationCoordinator::modeReplSet;
    return !isReplSet ||
        (repl::ReplicationCoordinator::get(opCtx)->getMemberState() ==
         repl::MemberState::RS_PRIMARY);
}

}  // namespace

void logDirectAuthOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc,
                            StringData operation) {
    if (!nss.isPrivilegeCollection()) {
        return;
    }

    if (AuditDeduplication::wasOperationAlreadyAudited(client)) {
        return;
    }

    if (!isStandaloneOrPrimary(client->getOperationContext())) {
        return;
    }

    AuditDeduplication::tryAuditEventAndMark(client,
                                             AuditEventType::kDirectAuthMutation,
                                             [&](BSONObjBuilder* builder) {
                                                 BSONObjBuilder documentObjectBuilder(
                                                     builder->subobjStart(audit::kDocumentField));
                                                 sanitizeCredentials(&documentObjectBuilder, doc);
                                                 documentObjectBuilder.done();

                                                 builder->append(audit::kNSField, nss.toString());
                                                 builder->append(audit::kOperationField, operation);
                                             },
                                             ErrorCodes::OK);
}

}  // namespace audit

void audit::logCreateUser(Client* client,
                          const UserName& username,
                          bool password,
                          const BSONObj* customData,
                          const std::vector<RoleName>& roles,
                          const boost::optional<BSONArray>& restrictions) {
    logCreateUpdateUser(
        client, username, password, customData, &roles, restrictions, AuditEventType::kCreateUser);
}

void audit::logDropUser(Client* client, const UserName& username) {
    AuditDeduplication::tryAuditEventAndMark(
        client,
        AuditEventType::kDropUser,
        [&](BSONObjBuilder* builder) { username.appendToBSON(builder); },
        ErrorCodes::OK);
}

void audit::logDropAllUsersFromDatabase(Client* client, StringData dbname) {
    AuditDeduplication::tryAuditEventAndMark(
        client,
        AuditEventType::kDropAllUsersFromDatabase,
        [dbname](BSONObjBuilder* builder) { builder->append(kDBField, dbname); },
        ErrorCodes::OK);
}

void audit::logUpdateUser(Client* client,
                          const UserName& username,
                          bool password,
                          const BSONObj* customData,
                          const std::vector<RoleName>* roles,
                          const boost::optional<BSONArray>& restrictions) {
    logCreateUpdateUser(
        client, username, password, customData, roles, restrictions, AuditEventType::kUpdateUser);
}

void audit::logInsertOperation(Client* client, const NamespaceString& nss, const BSONObj& doc) {
    logDirectAuthOperation(client, nss, doc, "insert"_sd);
}

void audit::logUpdateOperation(Client* client, const NamespaceString& nss, const BSONObj& doc) {
    logDirectAuthOperation(client, nss, doc, "update"_sd);
}

void audit::logRemoveOperation(Client* client, const NamespaceString& nss, const BSONObj& doc) {
    logDirectAuthOperation(client, nss, doc, "remove"_sd);
}


}  // namespace mongo
