/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#pragma once

#include <sasl/sasl.h>
#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authentication_session.h"
#include "mongo/platform/cstdint.h"

namespace mongo {

    class AuthorizationSession;

    /**
     * Authentication session data for the server side of SASL authentication.
     */
    class SaslAuthenticationSession : public AuthenticationSession {
        MONGO_DISALLOW_COPYING(SaslAuthenticationSession);
    public:
        struct SaslMechanismInfo;

        /**
         * Callback ID of a dummy callback that always returns SASL_FAIL, but whose associated
         * context pointer points to the SaslAuthenticationSession associated with a given
         * sasl_conn_t.
         *
         * This is used to allow other MongoDB plugins to get a pointer to the correct
         * SaslAuthenticationSession object.
         */
        static const int mongoSessionCallbackId;

        /**
         * Perform basic smoke testing of SASL mechanism "mechanism", to see if it is available for
         * use in this server.
         *
         * Use this method for startup-time verification that "mechanism" is available, supposing
         * that "serviceName" is the SASL service name and serviceHostname is the hostname of this
         * server.
         *
         * Returns Status::OK() if the mechanism is available.
         */
        static Status smokeTestMechanism(const StringData& mechanism,
                                         const StringData& serviceName,
                                         const StringData& serviceHostname);

        explicit SaslAuthenticationSession(AuthorizationSession* authSession);
        virtual ~SaslAuthenticationSession();

        /**
         * Start the server side of a SASL authentication.
         *
         * "authenticationDatabase" is the database against which the user is authenticating.
         * "mechanism" is the SASL mechanism to use.
         * "serviceName" is the SASL service name to use.
         * "serviceHostname" is the FQDN of this server.
         * "conversationId" is the conversation identifier to use for this session.
         *
         * If "autoAuthorize" is set to true, the server will automatically acquire all privileges
         * for a successfully authenticated user.  If it is false, the client will need to
         * explicilty acquire privileges on resources it wishes to access.
         *
         * Must be called only once on an instance.
         */
        Status start(const StringData& authenticationDatabase,
                     const StringData& mechanism,
                     const StringData& serviceName,
                     const StringData& serviceHostname,
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

        /**
         * Gets the name of the database against which this authentication conversation is running.
         *
         * Not meaningful before a successful call to start().
         */
        StringData getAuthenticationDatabase() const;

        /**
         * Get the conversation id for this authentication session.
         *
         * Must not be called before start().
         */
        int64_t getConversationId() const { return _conversationId; }

        /**
         * If the last call to step() returned Status::OK(), this method returns true if the
         * authentication conversation has completed, from the server's perspective.  If it returns
         * false, the server expects more input from the client.  If the last call to step() did not
         * return Status::OK(), returns true.
         *
         * Behavior is undefined if step() has not been called.
         */
        bool isDone() const { return _done; }

        /**
         * Gets the string identifier of the principal being authenticated.
         *
         * Returns the empty string if the session does not yet know the identity being
         * authenticated.
         */
        std::string getPrincipalId() const;

        /**
         * Gets the name of the SASL mechanism in use.
         *
         * Returns "" if start() has not been called or if start() did not return Status::OK().
         */
        const char* getMechanism() const;

        /**
         * Returns true if automatic privilege acquisition should be used for this principal, after
         * authentication.  Not meaningful before a successful call to start().
         */
        bool shouldAutoAuthorize() const { return _autoAuthorize; }

        /**
         * Returns a pointer to the opaque SaslMechanismInfo object for the mechanism in use.
         *
         * Not meaningful before a successful call to start().
         */
        const SaslMechanismInfo* getMechInfo() const { return _mechInfo; }

        AuthorizationSession* getAuthorizationSession() { return _authzSession; }

    private:
        static const int maxCallbacks = 4;
        AuthorizationSession* _authzSession;
        std::string _authenticationDatabase;
        std::string _serviceName;
        std::string _serviceHostname;
        sasl_conn_t* _saslConnection;
        int _saslStep;
        sasl_callback_t _callbacks[maxCallbacks];
        const SaslMechanismInfo* _mechInfo;
        int64_t _conversationId;
        bool _autoAuthorize;
        bool _done;
    };

}  // namespace mongo
