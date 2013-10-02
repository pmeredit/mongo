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

    class AuditLogRotateEvent : public AuditEvent {
    public:
        AuditLogRotateEvent(const AuditEventEnvelope& envelope,
                            const StringData& file)
            : AuditEvent(envelope),
              _file(file) {}
        virtual ~AuditLogRotateEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        const StringData _file;
    };

    std::ostream& AuditLogRotateEvent::putTextDescription(std::ostream& os) const {
        os << "Audit log rotated. Older events in " << _file << '.';
        return os;
    }

    BSONObjBuilder& AuditLogRotateEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append("renamedLogFile", _file);
        return builder;
    }

    void logAuditLogRotate(ClientBasic* client,
                           const StringData& file) {

        if (!getGlobalAuditManager()->enabled) return;

        AuditLogRotateEvent event(
                makeEnvelope(client, ActionType::auditLogRotate, ErrorCodes::OK),
                file);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }
}
}
