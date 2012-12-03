/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#pragma once

#include <gsasl.h>
#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authentication_session.h"
#include "mongo/db/client_common.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/gsasl_session.h"

struct Gsasl;

namespace mongo {

    class DBClientBase;

    /**
     * Authentication session data for the server side of SASL authentication.
     */
    class SaslAuthenticationSession : public AuthenticationSession {
        MONGO_DISALLOW_COPYING(SaslAuthenticationSession);
    public:
        /**
         * Returns the list of SASL mechanisms supported by SaslAuthenticationSession.
         */
        static std::vector<std::string> getSupportedMechanisms();

        explicit SaslAuthenticationSession(ClientBasic* client);
        virtual ~SaslAuthenticationSession();

        /**
         * Start the server side of a SASL authentication session.
         *
         * "mechanism" is the SASL mechanism to use.
         * "conversationId" is the conversation identifier to use for this session.
         *
         * If "autoAuthorize" is set to true, the server will automatically acquire all privileges
         * for a successfully authenticated user.  If it is false, the client will need to
         * explicilty acquire privileges on resources it wishes to access.
         *
         * Must be called only once on an instance.
         */
        Status start(const StringData& mechanism,
                     int64_t conversationId,
                     bool autoAuthorize);

        /**
         * Perform one step of the server side of the authentication session,
         * consuming "inputData" and producing "*outputData".
         *
         * A return of Status::OK() indiciates succesful progress towards authentication.
         * Any other return code indicates that authentication has failed.
         *
         * Must not be called before start().
         */
        Status step(const StringData& inputData, std::string* outputData);

        ClientBasic* getClient() const { return _client; }

        /**
         * Get the conversation id for this authentication session.
         *
         * Must not be called before start().
         */
        int64_t getConversationId() const { return _conversationId; }

        /**
         * If the last call to step() returned Status::OK(), this method returns true if the
         * authentication conversation has completed, from the server's perspective.  If it returns
         * false, the server expects more input from the client.
         *
         * Behavior is undefined if step() has not been called, or has returned a failing status.
         */
        bool isDone() const { return _gsaslSession.isDone(); }

        /**
         * Gets the string identifier of the principal being authenticated.
         */
        std::string getPrincipalId() const;

    private:
        ClientBasic* _client;
        GsaslSession _gsaslSession;
        int64_t _conversationId;
        bool _autoAuthorize;
        Gsasl_property _principalIdProperty;
    };

}  // namespace mongo
