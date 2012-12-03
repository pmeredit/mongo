/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_authentication_session.h"

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client_common.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/stringutils.h"

namespace {
    const std::string MECH_GSSAPI = "GSSAPI";
    const std::string MECH_CRAMMD5 = "CRAM-MD5";
    const std::string MECH_PLAIN = "PLAIN";
}  // namespace

namespace mongo {

namespace {

    Gsasl* _gsaslLibraryContext = NULL;

    std::vector<std::string> _supportedServerMechanisms;

}  // namespace

    SaslAuthenticationSession::SaslAuthenticationSession(ClientBasic* client) :
        AuthenticationSession(AuthenticationSession::SESSION_TYPE_SASL),
        _client(client),
        _conversationId(0),
        _autoAuthorize(false),
        _principalIdProperty() {
    }

    SaslAuthenticationSession::~SaslAuthenticationSession() {}

    Status SaslAuthenticationSession::start(const StringData& mechanism,
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
        return _gsaslSession.initializeServerSession(_gsaslLibraryContext, mechanism, this);
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

    const std::vector<std::string>& SaslAuthenticationSession::getSupportedServerMechanisms() {
        return _supportedServerMechanisms;
    }

namespace {

    int gsaslCallbackFunction(Gsasl* gsasl, Gsasl_session* gsession, Gsasl_property property) {

        SaslAuthenticationSession* session = static_cast<SaslAuthenticationSession*>(
                gsasl_session_hook_get(gsession));

        switch (property) {
        case GSASL_SERVICE:
            gsasl_property_set(gsession, GSASL_SERVICE, saslDefaultServiceName);
            return GSASL_OK;
        case GSASL_HOSTNAME:
            gsasl_property_set(gsession, GSASL_HOSTNAME, getHostNameCached().c_str());
            return GSASL_OK;
        case GSASL_PASSWORD: {
            fassert(0, NULL != session);
            std::string principal = session->getPrincipalId();
            std::string dbname;
            std::string username;
            if (!str::splitOn(principal, '$', dbname, username) ||
                dbname.empty() ||
                username.empty()) {

                log() << "sasl Bad principal \"" << principal << '"' << endl;
                return GSASL_NO_CALLBACK;
            }
            BSONObj privilegeDocument;
            Status status = session->getClient()->getAuthorizationManager()->getPrivilegeDocument(
                    dbname, username, &privilegeDocument);
            if (!status.isOK()) {
                log() << status.reason() << endl;
                return GSASL_NO_CALLBACK;
            }
            std::string hashedPassword;
            status = bsonExtractStringField(privilegeDocument, "pwd", &hashedPassword);
            if (!status.isOK()) {
                log() << "sasl No password data for " << principal << endl;
                return GSASL_NO_CALLBACK;
            }
            gsasl_property_set(gsession, GSASL_PASSWORD, hashedPassword.c_str());
            return GSASL_OK;
        }
        case GSASL_VALIDATE_GSSAPI:
            if (!str::equals(gsasl_property_fast(gsession, GSASL_GSSAPI_DISPLAY_NAME),
                             gsasl_property_fast(gsession, GSASL_AUTHZID))) {
                return GSASL_AUTHENTICATION_ERROR;
            }
            return GSASL_OK;
        default:
            return GSASL_NO_CALLBACK;
        }
    }

    std::vector<std::string> getGsaslSupportedServerMechanisms(Gsasl* gsasl) {
        char* mechsString;
        fassert(0, !gsasl_server_mechlist(gsasl, &mechsString));
        std::vector<std::string> result;
        splitStringDelim(mechsString, &result, ' ');
        free(mechsString);
        return result;
    }

    MONGO_INITIALIZER(SaslAuthenticationLibrary)(InitializerContext* context) {
        fassert(0, _gsaslLibraryContext == NULL);

        if (!gsasl_check_version(GSASL_VERSION))
            return Status(ErrorCodes::UnknownError, "Incompatible gsasl library.");

        int rc = gsasl_init(&_gsaslLibraryContext);
        if (GSASL_OK != rc)
            return Status(ErrorCodes::UnknownError, gsasl_strerror(rc));

        gsasl_callback_set(_gsaslLibraryContext, &gsaslCallbackFunction);

        _supportedServerMechanisms = getGsaslSupportedServerMechanisms(_gsaslLibraryContext);

        return Status::OK();
    }
}  // namespace
}  // namespace mongo
