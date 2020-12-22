/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_private.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/transport/session.h"

namespace mongo {
namespace audit {

void initializeEnvelope(AuditEventEnvelope* envelope,
                        Client* client,
                        AuditEventType auditEventType,
                        ErrorCodes::Error result) {
    envelope->timestamp = Date_t::now();
    if (client) {
        auto session = client->session();
        if (session) {
            invariant(session->localAddr().isValid() && session->remoteAddr().isValid());
            envelope->localAddr = session->localAddr();
            envelope->remoteAddr = session->remoteAddr();
        }
        if (AuthorizationSession::exists(client)) {
            auto authzSession = AuthorizationSession::get(client);
            envelope->authenticatedUserNames = authzSession->getAuthenticatedUserNames();
            envelope->authenticatedRoleNames = authzSession->getAuthenticatedRoleNames();
            envelope->impersonatedUserNames = authzSession->getImpersonatedUserNames();
            envelope->impersonatedRoleNames = authzSession->getImpersonatedRoleNames();
        }
    }

    envelope->auditEventType = auditEventType;
    envelope->result = result;
}

}  // namespace audit
}  // namespace mongo
