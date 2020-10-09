/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log.h"
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

class CreateIndexEvent : public AuditEvent {
public:
    CreateIndexEvent(const AuditEventEnvelope& envelope,
                     const BSONObj* indexSpec,
                     StringData indexname,
                     StringData nsname)
        : AuditEvent(envelope), _indexSpec(indexSpec), _indexname(indexname), _nsname(nsname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _nsname);
        builder.append("indexName", _indexname);
        builder.append("indexSpec", *_indexSpec);
        return builder;
    }

    const BSONObj* _indexSpec;
    StringData _indexname;
    StringData _nsname;
};

class CreateCollectionEvent : public AuditEvent {
public:
    CreateCollectionEvent(const AuditEventEnvelope& envelope, StringData nsname)
        : AuditEvent(envelope), _nsname(nsname) {}

    /* Used for creating views. */
    CreateCollectionEvent(const AuditEventEnvelope& envelope,
                          StringData nsname,
                          StringData viewOn,
                          BSONArray pipeline)
        : AuditEvent(envelope), _nsname(nsname), _viewOn(viewOn), _pipeline(pipeline) {
        dassert(!_viewOn.empty());
    }

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _nsname);
        if (!_viewOn.empty()) {
            builder.append("viewOn", _viewOn);
            builder.append("pipeline", _pipeline);
        }
        return builder;
    }

    StringData _nsname;
    StringData _viewOn;
    BSONArray _pipeline;
};

class CreateDatabaseEvent : public AuditEvent {
public:
    CreateDatabaseEvent(const AuditEventEnvelope& envelope, StringData dbname)
        : AuditEvent(envelope), _dbname(dbname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _dbname);
        return builder;
    }

    StringData _dbname;
};

class DropIndexEvent : public AuditEvent {
public:
    DropIndexEvent(const AuditEventEnvelope& envelope, StringData indexname, StringData nsname)
        : AuditEvent(envelope), _indexname(indexname), _nsname(nsname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _nsname);
        builder.append("indexName", _indexname);
        return builder;
    }

    StringData _indexname;
    StringData _nsname;
};

class DropCollectionEvent : public AuditEvent {
public:
    DropCollectionEvent(const AuditEventEnvelope& envelope, StringData nsname)
        : AuditEvent(envelope), _nsname(nsname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _nsname);
        return builder;
    }

    StringData _nsname;
};

class DropDatabaseEvent : public AuditEvent {
public:
    DropDatabaseEvent(const AuditEventEnvelope& envelope, StringData dbname)
        : AuditEvent(envelope), _dbname(dbname) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("ns", _dbname);
        return builder;
    }

    StringData _dbname;
};

class RenameCollectionEvent : public AuditEvent {
public:
    RenameCollectionEvent(const AuditEventEnvelope& envelope, StringData source, StringData target)
        : AuditEvent(envelope), _source(source), _target(target) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("old", _source);
        builder.append("new", _target);
        return builder;
    }

    StringData _source;
    StringData _target;
};

}  // namespace
}  // namespace audit

void audit::logCreateIndex(Client* client,
                           const BSONObj* indexSpec,
                           StringData indexname,
                           StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    CreateIndexEvent event(makeEnvelope(client, ActionType::createIndex, ErrorCodes::OK),
                           indexSpec,
                           indexname,
                           nsname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logCreateCollection(Client* client, StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    CreateCollectionEvent event(makeEnvelope(client, ActionType::createCollection, ErrorCodes::OK),
                                nsname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
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
    CreateCollectionEvent event(
        makeEnvelope(client, ActionType::createCollection, code), nsname, viewOn, pipeline);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logImportCollection(Client* client, StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    // An import is similar to a create, except that we use an importCollection action type.
    CreateCollectionEvent event(makeEnvelope(client, ActionType::importCollection, ErrorCodes::OK),
                                nsname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logCreateDatabase(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    CreateDatabaseEvent event(makeEnvelope(client, ActionType::createDatabase, ErrorCodes::OK),
                              dbname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logDropIndex(Client* client, StringData indexname, StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropIndexEvent event(
        makeEnvelope(client, ActionType::dropIndex, ErrorCodes::OK), indexname, nsname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logDropCollection(Client* client, StringData nsname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropCollectionEvent event(makeEnvelope(client, ActionType::dropCollection, ErrorCodes::OK),
                              nsname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logDropDatabase(Client* client, StringData dbname) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    DropDatabaseEvent event(makeEnvelope(client, ActionType::dropDatabase, ErrorCodes::OK), dbname);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

void audit::logRenameCollection(Client* client, StringData source, StringData target) {
    if (!getGlobalAuditManager()->enabled) {
        return;
    }

    RenameCollectionEvent event(
        makeEnvelope(client, ActionType::renameCollection, ErrorCodes::OK), source, target);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        logEvent(event);
    }
}

}  // namespace mongo
