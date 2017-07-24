/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

namespace audit {
namespace {

class EnableShardingEvent : public AuditEvent {
public:
    EnableShardingEvent(const AuditEventEnvelope& envelope, StringData dbname)
        : AuditEvent(envelope), _dbname(dbname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _dbname);

        return builder;
    }

    StringData _dbname;
};

class AddShardEvent : public AuditEvent {
public:
    AddShardEvent(const AuditEventEnvelope& envelope,
                  StringData name,
                  const std::string& servers,
                  long long maxSize)
        : AuditEvent(envelope), _name(name), _servers(servers), _maxSize(maxSize) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("shard", _name);
        builder.append("connectionString", _servers);
        builder.append("maxSize", _maxSize);
        return builder;
    }

    StringData _name;
    const std::string& _servers;
    long long _maxSize;
};

class RemoveShardEvent : public AuditEvent {
public:
    RemoveShardEvent(const AuditEventEnvelope& envelope, StringData shardname)
        : AuditEvent(envelope), _shardname(shardname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("shard", _shardname);
        return builder;
    }

    StringData _shardname;
};

class ShardCollectionEvent : public AuditEvent {
public:
    ShardCollectionEvent(const AuditEventEnvelope& envelope,
                         StringData ns,
                         const BSONObj& keyPattern,
                         bool unique)
        : AuditEvent(envelope), _ns(ns), _keyPattern(keyPattern), _unique(unique) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _ns);
        builder.append("key", _keyPattern);
        builder.append("options", BSON("unique" << _unique));
        return builder;
    }

    StringData _ns;
    const BSONObj& _keyPattern;
    bool _unique;
};

}  // namespace
}  // namespace audit

void audit::logEnableSharding(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    EnableShardingEvent event(makeEnvelope(client, ActionType::enableSharding, ErrorCodes::OK),
                              dbname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logAddShard(Client* client,
                        StringData name,
                        const std::string& servers,
                        long long maxSize) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    AddShardEvent event(
        makeEnvelope(client, ActionType::addShard, ErrorCodes::OK), name, servers, maxSize);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logShardCollection(Client* client,
                               StringData ns,
                               const BSONObj& keyPattern,
                               bool unique) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    ShardCollectionEvent event(
        makeEnvelope(client, ActionType::shardCollection, ErrorCodes::OK), ns, keyPattern, unique);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

void audit::logRemoveShard(Client* client, StringData shardname) {
    if (!getGlobalAuditManager()->enabled)
        return;

    RemoveShardEvent event(makeEnvelope(client, ActionType::removeShard, ErrorCodes::OK),
                           shardname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo
