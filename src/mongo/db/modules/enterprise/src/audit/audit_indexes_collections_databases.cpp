/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
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

bool isDDLAuditingAllowed(Client* client,
                          const NamespaceString& nsname,
                          boost::optional<const NamespaceString&> renameTarget = boost::none) {
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

void logNSEvent(Client* client, const NamespaceString& nsname, AuditEventType aType) {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent(
        client,
        aType,
        [nsname](BSONObjBuilder* builder) {
            builder->append(kNSField, NamespaceStringUtil::serialize(nsname));
        },
        ErrorCodes::OK);
}

}  // namespace
}  // namespace audit

void audit::logCreateIndex(Client* client,
                           const BSONObj* indexSpec,
                           StringData indexname,
                           const NamespaceString& nsname,
                           StringData indexBuildState,
                           ErrorCodes::Error result) {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent(
        client,
        AuditEventType::kCreateIndex,
        [&](BSONObjBuilder* builder) {
            builder->append(kNSField, NamespaceStringUtil::serialize(nsname));
            builder->append(kIndexNameField, indexname);
            builder->append(kIndexSpecField, *indexSpec);
            builder->append(kIndexBuildStateField, indexBuildState);
        },
        result);
}

void audit::logCreateCollection(Client* client, const NamespaceString& nsname) {
    logNSEvent(client, nsname, AuditEventType::kCreateCollection);
}

void audit::logCreateView(Client* client,
                          const NamespaceString& nsname,
                          StringData viewOn,
                          BSONArray pipeline,
                          ErrorCodes::Error code) {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    // Intentional: createView is audited as createCollection with viewOn/pipeline params. */
    tryLogEvent(
        client,
        AuditEventType::kCreateCollection,
        [&](BSONObjBuilder* builder) {
            builder->append(kNSField, NamespaceStringUtil::serialize(nsname));
            builder->append(kViewOnField, viewOn);
            builder->append(kPipelineField, pipeline);
        },
        ErrorCodes::OK);
}

void audit::logImportCollection(Client* client, const NamespaceString& nsname) {
    // An import is similar to a create, except that we use an importCollection action type.
    logNSEvent(client, nsname, AuditEventType::kImportCollection);
}

void audit::logCreateDatabase(Client* client, const DatabaseName& dbname) {
    logNSEvent(client, NamespaceString(dbname), AuditEventType::kCreateDatabase);
}

void audit::logDropIndex(Client* client, StringData indexname, const NamespaceString& nsname) {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent(
        client,
        AuditEventType::kDropIndex,
        [&](BSONObjBuilder* builder) {
            builder->append(kNSField, NamespaceStringUtil::serialize(nsname));
            builder->append(kIndexNameField, indexname);
        },
        ErrorCodes::OK);
}

void audit::logDropCollection(Client* client, const NamespaceString& nsname) {
    logNSEvent(client, nsname, AuditEventType::kDropCollection);

    NamespaceString nss(nsname);
    if (nss.isPrivilegeCollection()) {
        BSONObjBuilder builder;
        builder.append("dropCollection", NamespaceStringUtil::serialize(nsname));
        const auto cmdObj = builder.done();
        logDirectAuthOperation(client, nss, cmdObj, "command"_sd);
    }
}

void audit::logDropView(Client* client,
                        const NamespaceString& nsname,
                        StringData viewOn,
                        const std::vector<BSONObj>& pipeline,
                        ErrorCodes::Error code) {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    // Intentional: dropView is audited as dropCollection with viewOn/pipeline params.
    tryLogEvent(
        client,
        AuditEventType::kDropCollection,
        [&](BSONObjBuilder* builder) {
            builder->append(kNSField, NamespaceStringUtil::serialize(nsname));
            builder->append(kViewOnField, viewOn);
            builder->append(kPipelineField, pipeline);
        },
        code);
}

void audit::logDropDatabase(Client* client, const DatabaseName& dbname) {
    logNSEvent(client, NamespaceString(dbname), AuditEventType::kDropDatabase);
}

void audit::logRenameCollection(Client* client,
                                const NamespaceString& source,
                                const NamespaceString& target) {
    if (!isDDLAuditingAllowed(client, source, target)) {
        return;
    }

    tryLogEvent(
        client,
        AuditEventType::kRenameCollection,
        [&](BSONObjBuilder* builder) {
            builder->append(kOldField, NamespaceStringUtil::serialize(source));
            builder->append(kNewField, NamespaceStringUtil::serialize(target));
        },
        ErrorCodes::OK);

    BSONObjBuilder builder;
    builder.append("renameCollection", NamespaceStringUtil::serialize(source));
    builder.append("to", NamespaceStringUtil::serialize(target));
    const auto cmdObj = builder.done();

    logDirectAuthOperation(client, source, cmdObj, "command"_sd);
    logDirectAuthOperation(client, target, cmdObj, "command"_sd);
}

}  // namespace mongo
