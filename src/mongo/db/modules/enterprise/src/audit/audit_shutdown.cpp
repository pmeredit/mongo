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

    class ShutdownEvent : public AuditEvent {
    public:
        ShutdownEvent(const AuditEventEnvelope& envelope) : AuditEvent(envelope) {}
        virtual ~ShutdownEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;
    };

    std::ostream& ShutdownEvent::putTextDescription(std::ostream& os) const {
        os << "Shutdown commenced.";
        return os;
    }

    BSONObjBuilder& ShutdownEvent::putParamsBSON(BSONObjBuilder& builder) const {
        return builder;
    }

    void logShutdown(ClientBasic* client) {

        if (!getGlobalAuditManager()->enabled) return;

        ShutdownEvent event(makeEnvelope(client, ActionType::shutdown, ErrorCodes::OK));
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
