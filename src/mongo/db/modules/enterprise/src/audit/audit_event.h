/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <cstdint>

#include "audit_event_type.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/oid.h"
#include "mongo/db/auth/user_set.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/matchable.h"
#include "mongo/logv2/log_severity.h"

namespace mongo {
namespace audit {

/**
 * Base class of types representing events for writing to the audit log.
 */
class AuditEvent : public MatchableDocument {
public:
    using Serializer = std::function<void(BSONObjBuilder*)>;
    AuditEvent(Client* client,
               AuditEventType aType,
               Serializer serializer = nullptr,
               ErrorCodes::Error result = ErrorCodes::OK);

    Date_t getTimestamp() const {
        return _ts;
    }

    BSONObj toBSON() const final {
        return _obj;
    }

    ElementIterator* allocateIterator(const ElementPath* path) const final;
    void releaseIterator(ElementIterator* iterator) const final;

private:
    AuditEvent() = delete;
    AuditEvent(const AuditEvent&) = delete;
    AuditEvent& operator=(const AuditEvent&) = delete;

    static void serializeClient(Client* client, BSONObjBuilder* builder);

    BSONObj _obj;
    mutable BSONElementIterator _iterator;
    mutable bool _iteratorUsed = false;

    Date_t _ts;
};

}  // namespace audit
}  // namespace mongo
