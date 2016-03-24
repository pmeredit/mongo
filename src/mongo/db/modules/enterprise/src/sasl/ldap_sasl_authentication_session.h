/**
 *    Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <cstdint>
#include <string>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/sasl_authentication_session.h"

namespace mongo {

/**
 * Authentication session data for the server side of SASL authentication.
 */
class LDAPSaslAuthenticationSession : public SaslAuthenticationSession {
    MONGO_DISALLOW_COPYING(LDAPSaslAuthenticationSession);

public:
    explicit LDAPSaslAuthenticationSession(AuthorizationSession* authSession);

    virtual Status start(StringData authenticationDatabase,
                         StringData mechanism,
                         StringData serviceName,
                         StringData serviceHostname,
                         int64_t conversationId,
                         bool autoAuthorize) final;

    virtual Status step(StringData inputData, std::string* outputData) final;

    virtual std::string getPrincipalId() const final;

    virtual const char* getMechanism() const final;

private:
    std::string _user;
};
}  // namespace mongo
