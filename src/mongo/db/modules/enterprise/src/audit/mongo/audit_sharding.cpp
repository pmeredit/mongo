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

void audit::AuditMongo::logEnableSharding(Client* client, StringData dbname) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kEnableSharding,
         [dbname](BSONObjBuilder* builder) { builder->append(kNSField, dbname); },
         ErrorCodes::OK});
}

void audit::AuditMongo::logAddShard(Client* client,
                                    StringData name,
                                    const std::string& servers) const {
    tryLogEvent<AuditMongo::AuditEventMongo>({client,
                                              AuditEventType::kAddShard,
                                              [&](BSONObjBuilder* builder) {
                                                  builder->append(kShardField, name);
                                                  builder->append(kConnectionStringField, servers);
                                              },
                                              ErrorCodes::OK});
}

void audit::AuditMongo::logShardCollection(Client* client,
                                           const NamespaceString& ns,
                                           const BSONObj& keyPattern,
                                           bool unique) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kShardCollection,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()));
             builder->append(kKeyField, keyPattern);
             {
                 BSONObjBuilder optionsBuilder(builder->subobjStart(kOptionsField));
                 optionsBuilder.append(kUniqueField, unique);
             }
         },
         ErrorCodes::OK});
}

void audit::AuditMongo::logRemoveShard(Client* client, StringData shardname) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kRemoveShard,
         [shardname](BSONObjBuilder* builder) { builder->append(kShardField, shardname); },
         ErrorCodes::OK});
}

void audit::AuditMongo::logRefineCollectionShardKey(Client* client,
                                                    const NamespaceString& ns,
                                                    const BSONObj& keyPattern) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kRefineCollectionShardKey,
         [&](BSONObjBuilder* builder) {
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()));
             builder->append(kKeyField, keyPattern);
         },
         ErrorCodes::OK});
}

}  // namespace mongo
