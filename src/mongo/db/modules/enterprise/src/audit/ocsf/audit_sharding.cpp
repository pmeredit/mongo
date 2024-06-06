/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {
constexpr auto kNSField = "ns"_sd;
constexpr auto kShardField = "shard"_sd;
constexpr auto kConnectionStringField = "connectionString"_sd;
constexpr auto kMaxSizeField = "maxSize"_sd;
constexpr auto kKeyField = "key"_sd;
constexpr auto kOptionsField = "options"_sd;
constexpr auto kUniqueField = "unique"_sd;
}  // namespace

void AuditOCSF::logEnableSharding(Client* client, StringData dbname) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityHigh,
         [dbname](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kEnableSharding));
             unmapped.append(kNSField, dbname);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logAddShard(Client* client, StringData name, const std::string& servers) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityMedium,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kAddShard));
             unmapped.append(kShardField, name);
             unmapped.append(kConnectionStringField, servers);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logShardCollection(Client* client,
                                   const NamespaceString& ns,
                                   const BSONObj& keyPattern,
                                   bool unique) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kShardCollection));
             unmapped.append(
                 kNSField,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()));
             unmapped.append(kKeyField, keyPattern);
             {
                 BSONObjBuilder optionsBuilder(unmapped.subobjStart(kOptionsField));
                 optionsBuilder.append(kUniqueField, unique);
             }
         },
         ErrorCodes::OK});
}

void AuditOCSF::logRemoveShard(Client* client, StringData shardname) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityHigh,
         [shardname](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kRemoveShard));
             unmapped.append(kShardField, shardname);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logRefineCollectionShardKey(Client* client,
                                            const NamespaceString& ns,
                                            const BSONObj& keyPattern) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kDiscovery,
         ocsf::OCSFEventClass::kDeviceConfigState,
         ocsf::kDeviceConfigStateLog,
         ocsf::kSeverityMedium,
         [&](BSONObjBuilder* builder) {
             BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
             unmapped.append(ocsf::kATypeFieldName,
                             AuditEventType_serializer(AuditEventType::kRefineCollectionShardKey));
             unmapped.append(
                 kNSField,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()));
             unmapped.append(kKeyField, keyPattern);
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
