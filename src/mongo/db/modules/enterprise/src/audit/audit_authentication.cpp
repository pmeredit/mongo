/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client_basic.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace audit {

    /**
     * Event representing the result of an authentication activity.
     */
    class AuthenticationEvent : public AuditEvent {
    public:
        AuthenticationEvent(const AuditEventEnvelope& envelope,
                            const StringData& mechanism,
                            const UserName& user)
            : AuditEvent(envelope), _mechanism(mechanism), _user(user) {
        }
        virtual ~AuthenticationEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

        StringData _mechanism;
        UserName _user;
    };

    std::ostream& AuthenticationEvent::putTextDescription(std::ostream& os) const {
        if (getResultCode() == ErrorCodes::OK) {
            os << "Authentication succeeded for ";
        }
        else {
            os << "Authentication failed for ";
        }
        return os << _user.getFullName() << " using mechanism " << _mechanism << '.';
    }

    BSONObjBuilder& AuthenticationEvent::putParamsBSON(BSONObjBuilder& builder) const {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, _user.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, _user.getDB());
        builder.append("mechanism", _mechanism);
        return builder;
    }

    void logAuthentication(ClientBasic* client,
                           const StringData& mechanism,
                           const UserName& user,
                           ErrorCodes::Error result) {

        if (!getGlobalAuditManager()->enabled) return;

        AuthenticationEvent event(makeEnvelope(client, ActionType::authenticate, result),
                                  mechanism,
                                  user);
        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }

}  // namespace audit
}  // namespace mongo
