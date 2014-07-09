/*
 * Copyright (C) 2014 10gen, Inc.  All Rights Reserved.
 */

#pragma once

#include <string>
#include <vector>

#include "sasl_authentication_session.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authentication_session.h"
#include "mongo/platform/cstdint.h"

namespace mongo {
    
    /**
     * Authentication session data for the server side of SASL authentication.
     */
    class NativeSaslAuthenticationSession : public SaslAuthenticationSession {
        MONGO_DISALLOW_COPYING(NativeSaslAuthenticationSession);
    public:

        explicit NativeSaslAuthenticationSession(AuthorizationSession* authSession);
        virtual ~NativeSaslAuthenticationSession();

        virtual Status start(const StringData& authenticationDatabase,
                             const StringData& mechanism,
                             const StringData& serviceName,
                             const StringData& serviceHostname,
                             int64_t conversationId,
                             bool autoAuthorize);

        virtual Status step(const StringData& inputData, std::string* outputData);

        virtual std::string getPrincipalId() const;
    
        virtual const char* getMechanism() const;

    private:
    };
}  // namespace mongo
