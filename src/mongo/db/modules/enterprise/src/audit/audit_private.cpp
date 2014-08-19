/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "audit_private.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client_basic.h"
#include "mongo/util/log.h"

namespace mongo {
namespace audit {

    void fassertStatusOK(const Status& status) {
        if (MONGO_unlikely(!status.isOK())) {
            error() << status;
            fassertFailed(status.code());
        }
    }

    void initializeEnvelope(
            AuditEventEnvelope* envelope,
            ClientBasic* client,
            ActionType actionType,
            ErrorCodes::Error result) {

        envelope->timestamp = Date_t(curTimeMillis64());
        if (client->port()) {
            envelope->localAddr = client->port()->localAddr();
            envelope->remoteAddr = client->port()->remoteAddr();
        }
        envelope->authenticatedUserNames =
            client->getAuthorizationSession()->getAuthenticatedUserNames();
        envelope->authenticatedRoleNames =
            client->getAuthorizationSession()->getAuthenticatedRoleNames();
        envelope->impersonatedUserNames =
            client->getAuthorizationSession()->getImpersonatedUserNames();
        envelope->impersonatedRoleNames =
            client->getAuthorizationSession()->getImpersonatedRoleNames();
        envelope->actionType = actionType;
        envelope->result = result;
    }

}  // namespace audit
}  // namespace mongo
