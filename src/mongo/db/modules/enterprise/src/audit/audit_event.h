/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include "mongo/base/disallow_copying.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/oid.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/user_set.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/logger/log_severity.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/net/sock.h"

namespace mongo {
namespace audit {

    struct AuditEventEnvelope {
        Date_t timestamp;
        SockAddr localAddr;
        SockAddr remoteAddr;
        UserNameIterator authenticatedUserNames;
        RoleNameIterator authenticatedRoleNames;
        UserNameIterator impersonatedUserNames;
        RoleNameIterator impersonatedRoleNames;
        ActionType actionType;
        ErrorCodes::Error result;
    };

    /**
     * Base class of types representing events for writing to the audit log.
     *
     * Instances of subclasses of AuditEvent will typically be ephemeral, and built on the stack for
     * immediate writing into the audit log domain.  They are not intended to be stored, and may not
     * own all of the data they refernence.
     */
    class AuditEvent : public MatchableDocument {
        MONGO_DISALLOW_COPYING(AuditEvent);
    public:
        Date_t getTimestamp() const { return _envelope.timestamp; }
        const UserNameIterator getAuthenticatedUserNames() const {
            return _envelope.authenticatedUserNames;
        }
        const RoleNameIterator getAuthenticatedRoleNames() const {
            return _envelope.authenticatedRoleNames;
        }
        const UserNameIterator getImpersonatedUserNames() const {
            return _envelope.impersonatedUserNames;
        }
        const RoleNameIterator getImpersonatedRoleNames() const {
            return _envelope.impersonatedRoleNames;
        }
        const SockAddr& getLocalAddr() const { return _envelope.localAddr; }
        const SockAddr& getRemoteAddr() const { return _envelope.remoteAddr; }
        ActionType getActionType() const { return _envelope.actionType; }
        ErrorCodes::Error getResultCode() const { return _envelope.result; }

        /**
         * Puts human-readable description of this event into "os".
         */
        std::ostream& putText(std::ostream& os) const { return putTextDescription(os); }

        /**
         *  Virtual functions from MatchableDocument
         */
        virtual BSONObj toBSON() const;

        virtual ElementIterator* allocateIterator( const ElementPath* path ) const;
        virtual void releaseIterator( ElementIterator* iterator ) const;

        /**
         * Audit messages get classed as Info when written to syslog 
         */
        logger::LogSeverity getSeverity() const { return logger::LogSeverity::Info(); }

    protected:
        explicit AuditEvent(const AuditEventEnvelope& envelope) 
            : _envelope(envelope),
            _bsonGenerated(false),
            _iteratorUsed(false) {}

        /**
         * Destructor.  Should not be virtual.  Do not attempt to delete a pointer to AuditEvent.
         *
         * This destructor is virtual due to GCC 4.1.2 erroneously complaining about a non-virtual
         * protected destructor.
         */
        virtual ~AuditEvent() {}


    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const = 0;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const = 0;

        /**
         * Builds BSON describing this event and store in _obj
         */
        void generateBSON() const;

        AuditEventEnvelope _envelope;

        mutable BSONObj _obj;
        mutable bool _bsonGenerated;
        mutable BSONElementIterator _iterator;
        mutable bool _iteratorUsed;
    };

}  // namespace audit
}  // namespace mongo
