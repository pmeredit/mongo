/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_features_gen.h"
#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager_global.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace audit {
namespace {

constexpr auto kNSField = "ns"_sd;
constexpr auto kIndexNameField = "indexName"_sd;
constexpr auto kIndexSpecField = "indexSpec"_sd;
constexpr auto kViewOnField = "viewOn"_sd;
constexpr auto kPipelineField = "pipeline"_sd;
constexpr auto kOldField = "old"_sd;
constexpr auto kNewField = "new"_sd;

void logNSEvent(Client* client, StringData nsname, AuditEventType aType) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(
        client, aType, [nsname](BSONObjBuilder* builder) { builder->append(kNSField, nsname); });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace
}  // namespace audit

void audit::logCreateIndex(Client* client,
                           const BSONObj* indexSpec,
                           StringData indexname,
                           StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::createIndex, [&](BSONObjBuilder* builder) {
        builder->append(kNSField, nsname);
        builder->append(kIndexNameField, indexname);
        builder->append(kIndexSpecField, *indexSpec);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logCreateCollection(Client* client, StringData nsname) {
    logNSEvent(client, nsname, AuditEventType::createCollection);
}

void audit::logCreateView(Client* client,
                          StringData nsname,
                          StringData viewOn,
                          BSONArray pipeline,
                          ErrorCodes::Error code) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    // Intentional: createView is audited as createCollection with viewOn/pipeline params. */
    AuditEvent event(client, AuditEventType::createCollection, [&](BSONObjBuilder* builder) {
        builder->append(kNSField, nsname);
        if (gFeatureFlagImprovedAuditing.isEnabledAndIgnoreFCV()) {
            builder->append(kViewOnField, viewOn);
            builder->append(kPipelineField, pipeline);
        }
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logImportCollection(Client* client, StringData nsname) {
    // An import is similar to a create, except that we use an importCollection action type.
    logNSEvent(client, nsname, AuditEventType::importCollection);
}

void audit::logCreateDatabase(Client* client, StringData dbname) {
    logNSEvent(client, dbname, AuditEventType::createDatabase);
}

void audit::logDropIndex(Client* client, StringData indexname, StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::dropIndex, [&](BSONObjBuilder* builder) {
        builder->append(kNSField, nsname);
        builder->append(kIndexNameField, indexname);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logDropCollection(Client* client, StringData nsname) {
    logNSEvent(client, nsname, AuditEventType::dropCollection);
}

void audit::logDropDatabase(Client* client, StringData dbname) {
    logNSEvent(client, dbname, AuditEventType::dropDatabase);
}

void audit::logRenameCollection(Client* client, StringData source, StringData target) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::renameCollection, [&](BSONObjBuilder* builder) {
        builder->append(kOldField, source);
        builder->append(kNewField, target);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
