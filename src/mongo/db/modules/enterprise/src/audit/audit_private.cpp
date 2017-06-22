/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "audit_private.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/transport/session.h"
#include "mongo/util/log.h"

namespace mongo {
namespace audit {

void initializeEnvelope(AuditEventEnvelope* envelope,
                        Client* client,
                        ActionType actionType,
                        ErrorCodes::Error result) {
    envelope->timestamp = Date_t::now();
    auto session = client->session();
    if (session) {
        invariant(session->local().sockAddr() && session->remote().sockAddr());
        envelope->localAddr = *session->local().sockAddr();
        envelope->remoteAddr = *session->remote().sockAddr();
    }
    envelope->authenticatedUserNames =
        AuthorizationSession::get(client)->getAuthenticatedUserNames();
    envelope->authenticatedRoleNames =
        AuthorizationSession::get(client)->getAuthenticatedRoleNames();
    envelope->impersonatedUserNames = AuthorizationSession::get(client)->getImpersonatedUserNames();
    envelope->impersonatedRoleNames = AuthorizationSession::get(client)->getImpersonatedRoleNames();
    envelope->actionType = actionType;
    envelope->result = result;
}

}  // namespace audit
}  // namespace mongo
