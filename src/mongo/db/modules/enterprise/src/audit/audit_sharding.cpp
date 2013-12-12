/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
namespace audit {

    class EnableShardingEvent : public AuditEvent {
    public:
        EnableShardingEvent(const AuditEventEnvelope& envelope,
                            const StringData& dbname)
            : AuditEvent(envelope),
              _dbname(dbname) {}
        virtual ~EnableShardingEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _dbname;
    };

    std::ostream& EnableShardingEvent::putTextDescription(std::ostream& os) const {
        os << "Enabled sharding on " << _dbname << '.';
        return os;
    }

    BSONObjBuilder& EnableShardingEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _dbname);
        return builder;
    }

    void logEnableSharding(ClientBasic* client,
                           const StringData& dbname) {
        if (!getGlobalAuditManager()->enabled) return;

        EnableShardingEvent event(makeEnvelope(client, ActionType::enableSharding, ErrorCodes::OK),
                                  dbname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class AddShardEvent : public AuditEvent {
    public:
        AddShardEvent(const AuditEventEnvelope& envelope,
                      const StringData& name,
                      const std::string& servers,
                      long long maxSize)
            : AuditEvent(envelope),
              _name(name),
              _servers(servers),
              _maxSize(maxSize) {}
        virtual ~AddShardEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _name;
        const std::string& _servers;
        long long _maxSize;
    };

    std::ostream& AddShardEvent::putTextDescription(std::ostream& os) const {
        os << "Added shard " << _name
           << " with connection string: " << _servers;
        if (_maxSize) {
            os << " with a maximum size of " << _maxSize << "MB.";
        }
        else {
            os << " without a maxmium size.";
        }
        os << '.';
        return os;
    }

    BSONObjBuilder& AddShardEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("shard", _name);
        builder.append("connectionString", _servers);
        builder.append("maxSize", _maxSize);
        return builder;
    }

    void logAddShard(ClientBasic* client,
                     const StringData& name,
                     const std::string& servers,
                     long long maxSize) {
        if (!getGlobalAuditManager()->enabled) return;

        AddShardEvent event(makeEnvelope(client, ActionType::addShard, ErrorCodes::OK),
                            name,
                            servers,
                            maxSize);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class RemoveShardEvent : public AuditEvent {
    public:
        RemoveShardEvent(const AuditEventEnvelope& envelope,
                         const StringData& shardname)
            : AuditEvent(envelope),
              _shardname(shardname) {}
        virtual ~RemoveShardEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _shardname;
    };

    std::ostream& RemoveShardEvent::putTextDescription(std::ostream& os) const {
        os << "Removed shard " << _shardname << '.';
        return os;
    }

    BSONObjBuilder& RemoveShardEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("shard", _shardname);
        return builder;
    }

    void logRemoveShard(ClientBasic* client,
                        const StringData& shardname) {
        if (!getGlobalAuditManager()->enabled) return;

        RemoveShardEvent event(makeEnvelope(client, ActionType::removeShard, ErrorCodes::OK),
                               shardname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class ShardCollectionEvent : public AuditEvent {
    public:
        ShardCollectionEvent(const AuditEventEnvelope& envelope,
                             const StringData& ns,
                             const BSONObj& keyPattern,
                             bool unique)
            : AuditEvent(envelope),
              _ns(ns),
              _keyPattern(keyPattern),
              _unique(unique) {}
        virtual ~ShardCollectionEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _ns;
        const BSONObj& _keyPattern;
        bool _unique;
    };

    std::ostream& ShardCollectionEvent::putTextDescription(std::ostream& os) const {
        os << "Collection " << _ns << " sharded on ";
        if (_unique) {
            os << "unique ";
        }
        os << "shard key pattern " << _keyPattern.toString() << '.';
        return os;
    }

    BSONObjBuilder& ShardCollectionEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _ns);
        builder.append("key", _keyPattern);
        builder.append("options", BSON("unique" << _unique));
        return builder;
    }

    void logShardCollection(ClientBasic* client,
                            const StringData& ns,
                            const BSONObj& keyPattern,
                            bool unique) {
        if (!getGlobalAuditManager()->enabled) return;

        ShardCollectionEvent event(makeEnvelope(client,
                                                ActionType::shardCollection,
                                                ErrorCodes::OK),
                                   ns,
                                   keyPattern,
                                   unique);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
