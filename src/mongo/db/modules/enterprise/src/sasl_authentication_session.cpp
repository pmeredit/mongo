/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_authentication_session.h"

#include "mongo/util/assert_util.h"

namespace mongo {

    SaslAuthenticationSession::SaslAuthenticationSession() :
        AuthenticationSession(AuthenticationSession::SESSION_TYPE_SASL),
        _conversationId(0),
        _autoAuthorize(false) {
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
        return _gsaslSession.initializeServerSession(gsasl, mechanism, this);
    }

    Status SaslAuthenticationSession::step(const StringData& inputData, std::string* outputData) {
        return _gsaslSession.step(inputData, outputData);
    }

    const std::string SaslAuthenticationSession::getSaslProperty(Gsasl_property property) const {
        return _gsaslSession.getProperty(property);
    }

}  // namespace mongo
