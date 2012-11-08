/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_authentication_session.h"

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client_common.h"
#include "mongo/util/assert_util.h"

namespace {
    const std::string MECH_GSSAPI = "GSSAPI";
    const std::string MECH_CRAMMD5 = "CRAM-MD5";
    const std::string MECH_PLAIN = "PLAIN";
}  // namespace

namespace mongo {

    SaslAuthenticationSession::SaslAuthenticationSession(ClientBasic* client) :
        AuthenticationSession(AuthenticationSession::SESSION_TYPE_SASL),
        _client(client),
        _conversationId(0),
        _autoAuthorize(false),
        _principalIdProperty() {
    }

    SaslAuthenticationSession::~SaslAuthenticationSession() {}

    Status SaslAuthenticationSession::start(Gsasl* gsasl,
                                            const StringData& mechanism,
                                            int64_t conversationId,
                                            bool autoAuthorize) {
        fassert(0, conversationId > 0);
        if (_conversationId != 0) {
            return Status(ErrorCodes::InternalError,
                          "Cannot call start twice on same SaslAuthenticationSession.");
        }

        _conversationId = conversationId;
        _autoAuthorize = autoAuthorize;

        if (mechanism == MECH_GSSAPI)
            _principalIdProperty = GSASL_AUTHZID;
        else if (mechanism == MECH_CRAMMD5)
            _principalIdProperty = GSASL_AUTHID;
        else if (mechanism == MECH_PLAIN)
            _principalIdProperty = GSASL_AUTHID;
        else
            return Status(ErrorCodes::InternalError,
                          "Unsupported mechanism; should have caught it earlier: " +
                          std::string(mechanism.data(), mechanism.data() + mechanism.size()));
        return _gsaslSession.initializeServerSession(gsasl, mechanism, this);
    }

    Status SaslAuthenticationSession::step(const StringData& inputData, std::string* outputData) {
        Status status = _gsaslSession.step(inputData, outputData);
        if (!status.isOK())
            return status;

        if (isDone()) {
            std::string principalName = getPrincipalId();
            Principal* principal = new Principal(principalName);
            // TODO: check if session->_autoAuthorize is true and if so inform the
            // AuthorizationManager to implicitly acquire privileges for this principal.
            getClient()->getAuthorizationManager()->addAuthorizedPrincipal(principal);
        }

        return status;
    }

    std::string SaslAuthenticationSession::getPrincipalId() const {
        return _gsaslSession.getProperty(_principalIdProperty);
    }

}  // namespace mongo
