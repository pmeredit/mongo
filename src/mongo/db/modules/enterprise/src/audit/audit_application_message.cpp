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

    class ApplicationMessageEvent : public AuditEvent {
    public:
        ApplicationMessageEvent(const AuditEventEnvelope& envelope,
                                const StringData& msg)
            : AuditEvent(envelope), _msg(msg) {}
        virtual ~ApplicationMessageEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData _msg;
    };

    std::ostream& ApplicationMessageEvent::putTextDescription(std::ostream& os) const {
        os << "Client application message: " << _msg;
        return os;
    }

    BSONObjBuilder& ApplicationMessageEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("msg", _msg);
        return builder;
    }

    void logApplicationMessage(ClientBasic* client,
                               const StringData& msg) {

        if (!getGlobalAuditManager()->enabled) return;

        ApplicationMessageEvent event(
                makeEnvelope(client, ActionType::applicationMessage, ErrorCodes::OK),
                msg);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
