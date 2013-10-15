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

    class CreateIndexEvent : public AuditEvent {
    public:
        CreateIndexEvent(const AuditEventEnvelope& envelope,
                         const BSONObj* indexSpec,
                         const StringData& indexname,
                         const StringData& nsname)
            : AuditEvent(envelope),
              _indexSpec(indexSpec),
              _indexname(indexname),
              _nsname(nsname) {}
        virtual ~CreateIndexEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const BSONObj* _indexSpec;
        const StringData& _indexname;
        const StringData& _nsname;
    };

    std::ostream& CreateIndexEvent::putTextDescription(std::ostream& os) const {
        os << "Created index " << _indexname
           << " on " << _nsname
           << " as " << *_indexSpec << '.';
        return os;
    }

    BSONObjBuilder& CreateIndexEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _nsname);
        builder.append("indexName", _indexname);
        builder.append("key", *_indexSpec);
        return builder;
    }

    void logCreateIndex(ClientBasic* client,
                        const BSONObj* indexSpec,
                        const StringData& indexname,
                        const StringData& nsname) {

        if (!getGlobalAuditManager()->enabled) return;

        CreateIndexEvent event(
                makeEnvelope(client, ActionType::createIndex, ErrorCodes::OK),
                indexSpec,
                indexname,
                nsname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class CreateCollectionEvent : public AuditEvent {
    public:
        CreateCollectionEvent(const AuditEventEnvelope& envelope,
                              const StringData& nsname)
            : AuditEvent(envelope),
              _nsname(nsname) {}
        virtual ~CreateCollectionEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _nsname;
    };

    std::ostream& CreateCollectionEvent::putTextDescription(std::ostream& os) const {
        os << "Created collection " << _nsname << '.';
        return os;
    }

    BSONObjBuilder& CreateCollectionEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _nsname);
        return builder;
    }

    void logCreateCollection(ClientBasic* client,
                             const StringData& nsname) {

        if (!getGlobalAuditManager()->enabled) return;

        // Do not log index namespace creation.
        if (!NamespaceString::normal(nsname)) return;

        CreateCollectionEvent event(
                makeEnvelope(client, ActionType::createCollection, ErrorCodes::OK),
                nsname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class CreateDatabaseEvent : public AuditEvent {
    public:
        CreateDatabaseEvent(const AuditEventEnvelope& envelope,
                            const StringData& dbname)
            : AuditEvent(envelope),
              _dbname(dbname) {}
        virtual ~CreateDatabaseEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _dbname;
    };

    std::ostream& CreateDatabaseEvent::putTextDescription(std::ostream& os) const {
        os << "Created database " << _dbname << '.';
        return os;
    }

    BSONObjBuilder& CreateDatabaseEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _dbname);
        return builder;
    }

    void logCreateDatabase(ClientBasic* client,
                           const StringData& dbname) {

        if (!getGlobalAuditManager()->enabled) return;

        CreateDatabaseEvent event(
                makeEnvelope(client, ActionType::createDatabase, ErrorCodes::OK),
                dbname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropIndexEvent : public AuditEvent {
    public:
        DropIndexEvent(const AuditEventEnvelope& envelope,
                       const StringData& indexname,
                       const StringData& nsname)
            : AuditEvent(envelope),
              _indexname(indexname),
              _nsname(nsname) {}
        virtual ~DropIndexEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _indexname;
        const StringData& _nsname;
    };

    std::ostream& DropIndexEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped index " << _indexname << " from " << _nsname << '.';
        return os;
    }

    BSONObjBuilder& DropIndexEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _nsname);
        builder.append("indexName", _indexname);
        return builder;
    }

    void logDropIndex(ClientBasic* client,
                      const StringData& indexname,
                      const StringData& nsname) {

        if (!getGlobalAuditManager()->enabled) return;

        DropIndexEvent event(
                makeEnvelope(client, ActionType::dropIndex, ErrorCodes::OK),
                indexname,
                nsname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropCollectionEvent : public AuditEvent {
    public:
        DropCollectionEvent(const AuditEventEnvelope& envelope,
                            const StringData& nsname)
            : AuditEvent(envelope),
              _nsname(nsname) {}
        virtual ~DropCollectionEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _nsname;
    };

    std::ostream& DropCollectionEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped collection " << _nsname << '.';
        return os;
    }

    BSONObjBuilder& DropCollectionEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _nsname);
        return builder;
    }

    void logDropCollection(ClientBasic* client,
                           const StringData& nsname) {

        if (!getGlobalAuditManager()->enabled) return;

        DropCollectionEvent event(
                makeEnvelope(client, ActionType::dropCollection, ErrorCodes::OK),
                nsname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }


    class DropDatabaseEvent : public AuditEvent {
    public:
        DropDatabaseEvent(const AuditEventEnvelope& envelope,
                          const StringData& dbname)
            : AuditEvent(envelope),
              _dbname(dbname) {}
        virtual ~DropDatabaseEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _dbname;
    };

    std::ostream& DropDatabaseEvent::putTextDescription(std::ostream& os) const {
        os << "Dropped database " << _dbname << '.';
        return os;
    }

    BSONObjBuilder& DropDatabaseEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("ns", _dbname);
        return builder;
    }

    void logDropDatabase(ClientBasic* client,
                         const StringData& dbname) {

        if (!getGlobalAuditManager()->enabled) return;

        DropDatabaseEvent event(
                makeEnvelope(client, ActionType::dropDatabase, ErrorCodes::OK),
                dbname);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }

    
    class RenameCollectionEvent : public AuditEvent {
    public:
        RenameCollectionEvent(const AuditEventEnvelope& envelope,
                              const StringData& source,
                              const StringData& target)
            : AuditEvent(envelope),
              _source(source),
              _target(target) {}
        virtual ~RenameCollectionEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData& _source;
        const StringData& _target;
    };

    std::ostream& RenameCollectionEvent::putTextDescription(std::ostream& os) const {
        os << "Renamed collection " << _source << " to " << _target << '.';
        return os;
    }

    BSONObjBuilder& RenameCollectionEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("original", _source);
        builder.append("new", _target);
        return builder;
    }

    void logRenameCollection(ClientBasic* client,
                             const StringData& source,
                             const StringData& target) {

        if (!getGlobalAuditManager()->enabled) return;

        RenameCollectionEvent event(
                makeEnvelope(client, ActionType::renameCollection, ErrorCodes::OK),
                source,
                target);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
