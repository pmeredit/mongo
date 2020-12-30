/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager_global.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {

namespace {
constexpr auto kNSField = "ns"_sd;
constexpr auto kShardField = "shard"_sd;
constexpr auto kConnectionStringField = "connectionString"_sd;
constexpr auto kMaxSizeField = "maxSize"_sd;
constexpr auto kKeyField = "key"_sd;
constexpr auto kOptionsField = "options"_sd;
constexpr auto kUniqueField = "unique"_sd;
}  // namespace

void audit::logEnableSharding(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::enableSharding, [dbname](BSONObjBuilder* builder) {
        builder->append(kNSField, dbname);
    });
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logAddShard(Client* client,
                        StringData name,
                        const std::string& servers,
                        long long maxSize) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::addShard, [&](BSONObjBuilder* builder) {
        builder->append(kShardField, name);
        builder->append(kConnectionStringField, servers);
        builder->append(kMaxSizeField, maxSize);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logShardCollection(Client* client,
                               StringData ns,
                               const BSONObj& keyPattern,
                               bool unique) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(client, AuditEventType::shardCollection, [&](BSONObjBuilder* builder) {
        builder->append(kNSField, ns);
        builder->append(kKeyField, keyPattern);
        {
            BSONObjBuilder optionsBuilder(builder->subobjStart(kOptionsField));
            optionsBuilder.append(kUniqueField, unique);
        }
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logRemoveShard(Client* client, StringData shardname) {
    if (!getGlobalAuditManager()->enabled)
        return;

    AuditEvent event(client, AuditEventType::removeShard, [shardname](BSONObjBuilder* builder) {
        builder->append(kShardField, shardname);
    });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logRefineCollectionShardKey(Client* client, StringData ns, const BSONObj& keyPattern) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AuditEvent event(
        client, AuditEventType::refineCollectionShardKey, [&](BSONObjBuilder* builder) {
            builder->append(kNSField, ns);
            builder->append(kKeyField, keyPattern);
        });

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
