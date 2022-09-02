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
    tryLogEvent(client,
                AuditEventType::kEnableSharding,
                [dbname](BSONObjBuilder* builder) { builder->append(kNSField, dbname); },
                ErrorCodes::OK);
}

void audit::logAddShard(Client* client,
                        StringData name,
                        const std::string& servers,
                        long long maxSize) {
    tryLogEvent(client,
                AuditEventType::kAddShard,
                [&](BSONObjBuilder* builder) {
                    builder->append(kShardField, name);
                    builder->append(kConnectionStringField, servers);
                    builder->append(kMaxSizeField, maxSize);
                },
                ErrorCodes::OK);
}

void audit::logShardCollection(Client* client,
                               StringData ns,
                               const BSONObj& keyPattern,
                               bool unique) {
    tryLogEvent(client,
                AuditEventType::kShardCollection,
                [&](BSONObjBuilder* builder) {
                    builder->append(kNSField, ns);
                    builder->append(kKeyField, keyPattern);
                    {
                        BSONObjBuilder optionsBuilder(builder->subobjStart(kOptionsField));
                        optionsBuilder.append(kUniqueField, unique);
                    }
                },
                ErrorCodes::OK);
}

void audit::logRemoveShard(Client* client, StringData shardname) {
    tryLogEvent(client,
                AuditEventType::kRemoveShard,
                [shardname](BSONObjBuilder* builder) { builder->append(kShardField, shardname); },
                ErrorCodes::OK);
}

void audit::logRefineCollectionShardKey(Client* client, StringData ns, const BSONObj& keyPattern) {
    tryLogEvent(client,
                AuditEventType::kRefineCollectionShardKey,
                [&](BSONObjBuilder* builder) {
                    builder->append(kNSField, ns);
                    builder->append(kKeyField, keyPattern);
                },
                ErrorCodes::OK);
}

}  // namespace mongo
