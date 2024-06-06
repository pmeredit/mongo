/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/util/namespace_string_util.h"

namespace mongo {

namespace audit {
namespace {

constexpr auto kNSField = "ns"_sd;
constexpr auto kIndexNameField = "indexName"_sd;
constexpr auto kIndexSpecField = "indexSpec"_sd;
constexpr auto kIndexBuildStateField = "indexBuildState"_sd;
constexpr auto kViewOnField = "viewOn"_sd;
constexpr auto kPipelineField = "pipeline"_sd;
constexpr auto kOldField = "old"_sd;
constexpr auto kNewField = "new"_sd;

void logNSEvent(Client* client, const NamespaceString& nsname, AuditEventType aType) {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         aType,
         [nsname](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
         },
         ErrorCodes::OK});
}

}  // namespace

bool isDDLAuditingAllowed(Client* client,
                          const NamespaceString& nsname,
                          boost::optional<const NamespaceString&> renameTarget) {
    auto replCoord = repl::ReplicationCoordinator::get(client->getOperationContext());
    if (replCoord) {
        // If the collection is being renamed, audit if operating on a standalone, a primary or if
        // both the source and target namespaces are unreplicated.
        if (renameTarget) {
            return (!replCoord->getSettings().isReplSet() ||
                    replCoord->getMemberState().primary() ||
                    (!nsname.isReplicated() && !renameTarget.value().isReplicated()));
        }
        // For all other DDL operations, audit if operating on a standalone, primary, or the
        // namespace is unreplicated.
        return (!replCoord->getSettings().isReplSet() || replCoord->getMemberState().primary() ||
                !nsname.isReplicated());
    }

    // If the replCoord is unavailable for some reason, don't audit.
    return false;
}

}  // namespace audit

void audit::AuditMongo::logCreateIndex(Client* client,
                                       const BSONObj* indexSpec,
                                       StringData indexname,
                                       const NamespaceString& nsname,
                                       StringData indexBuildState,
                                       ErrorCodes::Error result) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kCreateIndex,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
             builder->append(kIndexNameField, indexname);
             builder->append(kIndexSpecField, *indexSpec);
             builder->append(kIndexBuildStateField, indexBuildState);
         },
         result});
}

void audit::AuditMongo::logCreateCollection(Client* client, const NamespaceString& nsname) const {
    logNSEvent(client, nsname, AuditEventType::kCreateCollection);
}

void audit::AuditMongo::logCreateView(Client* client,
                                      const NamespaceString& nsname,
                                      const NamespaceString& viewOn,
                                      BSONArray pipeline,
                                      ErrorCodes::Error code) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    // Intentional: createView is audited as createCollection with viewOn/pipeline params. */
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kCreateCollection,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
             builder->append(
                 kViewOnField,
                 NamespaceStringUtil::serialize(viewOn, SerializationContext::stateDefault()));
             builder->append(kPipelineField, pipeline);
         },
         code});
}

void audit::AuditMongo::logImportCollection(Client* client, const NamespaceString& nsname) const {
    // An import is similar to a create, except that we use an importCollection action type.
    logNSEvent(client, nsname, AuditEventType::kImportCollection);
}

void audit::AuditMongo::logCreateDatabase(Client* client, const DatabaseName& dbname) const {
    logNSEvent(client, NamespaceString(dbname), AuditEventType::kCreateDatabase);
}

void audit::AuditMongo::logDropIndex(Client* client,
                                     StringData indexname,
                                     const NamespaceString& nsname) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kDropIndex,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
             builder->append(kIndexNameField, indexname);
         },
         ErrorCodes::OK});
}

void audit::AuditMongo::logDropCollection(Client* client, const NamespaceString& nsname) const {
    logNSEvent(client, nsname, AuditEventType::kDropCollection);

    NamespaceString nss(nsname);
    if (nss.isPrivilegeCollection()) {
        BSONObjBuilder builder;
        builder.append(
            "dropCollection",
            NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
        const auto cmdObj = builder.done();
        logDirectAuthOperation(client, nss, cmdObj, DirectAuthOperation::kDrop);
    }
}

void audit::AuditMongo::logDropView(Client* client,
                                    const NamespaceString& nsname,
                                    const NamespaceString& viewOn,
                                    const std::vector<BSONObj>& pipeline,
                                    ErrorCodes::Error code) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    // Intentional: dropView is audited as dropCollection with viewOn/pipeline params.
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kDropCollection,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
             builder->append(
                 kViewOnField,
                 NamespaceStringUtil::serialize(viewOn, SerializationContext::stateDefault()));
             builder->append(kPipelineField, pipeline);
         },
         code});
}

void audit::AuditMongo::logDropDatabase(Client* client, const DatabaseName& dbname) const {
    logNSEvent(client, NamespaceString(dbname), AuditEventType::kDropDatabase);
}

void audit::AuditMongo::logRenameCollection(Client* client,
                                            const NamespaceString& source,
                                            const NamespaceString& target) const {
    if (!isDDLAuditingAllowed(client, source, target)) {
        return;
    }

    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kRenameCollection,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kOldField,
                 NamespaceStringUtil::serialize(source, SerializationContext::stateDefault()));
             builder->append(
                 kNewField,
                 NamespaceStringUtil::serialize(target, SerializationContext::stateDefault()));
         },
         ErrorCodes::OK});

    BSONObjBuilder builder;
    builder.append("renameCollection",
                   NamespaceStringUtil::serialize(source, SerializationContext::stateDefault()));
    builder.append("to",
                   NamespaceStringUtil::serialize(target, SerializationContext::stateDefault()));
    const auto cmdObj = builder.done();

    logDirectAuthOperation(client, source, cmdObj, DirectAuthOperation::kRename);
    logDirectAuthOperation(client, target, cmdObj, DirectAuthOperation::kRename);
}

}  // namespace mongo
