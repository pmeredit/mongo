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

    class ReplSetReconfigEvent : public AuditEvent {
    public:
        ReplSetReconfigEvent(const AuditEventEnvelope& envelope,
                             const BSONObj* oldConfig,
                             const BSONObj* newConfig)
            : AuditEvent(envelope),
              _oldConfig(oldConfig),
              _newConfig(newConfig) {}
        virtual ~ReplSetReconfigEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const BSONObj* _oldConfig;
        const BSONObj* _newConfig;
    };

    std::ostream& ReplSetReconfigEvent::putTextDescription(std::ostream& os) const {
        if (_oldConfig) {
            os << "Reconfiguring replica set: ";
        }
        else {
            os << "Configuring replica set: ";
        }
        verify(_newConfig);
        os << *_newConfig;
        os << '.';
        return os;
    }

    BSONObjBuilder& ReplSetReconfigEvent::putParamsBSON(BSONObjBuilder& builder) const {
        if (_oldConfig) {
            builder.append("old", *_oldConfig);
        }
        verify(_newConfig);
        builder.append("new", *_newConfig);
        return builder;
    }

    void logReplSetReconfig(ClientBasic* client,
                            const BSONObj* oldConfig,
                            const BSONObj* newConfig) {

        if (!getGlobalAuditManager()->enabled) return;

        ReplSetReconfigEvent event(
                makeEnvelope(client, ActionType::userAdmin, ErrorCodes::OK),
                oldConfig,
                newConfig);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
