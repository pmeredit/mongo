/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_authentication_session.h"

#include <map>
#include <set>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/map_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/stringutils.h"

namespace mongo {

namespace {

    std::vector<std::string> stringSplit(const std::string& s, char delim) {
        std::vector<std::string> result;
        splitStringDelim(s, &result, delim);
        return result;
    }

    MONGO_EXPORT_SERVER_PARAMETER(
            authenticationMechanisms, std::vector<std::string>, stringSplit("MONGO-CR", ','));

    MONGO_EXPORT_SERVER_PARAMETER(saslHostName, std::string, "");
    MONGO_EXPORT_SERVER_PARAMETER(saslServiceName, std::string, "");

    struct SaslMechanismInfo {
        const char* name;
        Gsasl_property principalIdProperty;
    };
    typedef std::map<std::string, const SaslMechanismInfo*> SaslMechanismInfoMap;

    // SASL Mechanisms
    const char MECH_CRAMMD5[] = "CRAM-MD5";
    const char MECH_DIGESTMD5[] = "DIGEST-MD5";
    const char MECH_GSSAPI[] = "GSSAPI";
    const char MECH_PLAIN[] = "PLAIN";

    // The name we give to the nonce-authenticate mechanism in the free product.
    const char MECH_MONGOCR[] = "MONGO-CR";

    SaslMechanismInfo _mongoKnownMechanisms[] = {
        { MECH_DIGESTMD5, GSASL_AUTHID },
        { MECH_CRAMMD5, GSASL_AUTHID },
        { MECH_GSSAPI, GSASL_AUTHZID },
        { MECH_PLAIN, GSASL_AUTHID },
        { NULL }
    };

    Gsasl* _gsaslLibraryContext = NULL;

    SaslMechanismInfoMap _supportedSaslMechanisms;

}  // namespace

    SaslAuthenticationSession::SaslAuthenticationSession(ClientBasic* client,
                                                         const std::string& principalSource) :
        AuthenticationSession(AuthenticationSession::SESSION_TYPE_SASL),
        _client(client),
        _principalSource(principalSource),
        _conversationId(0),
        _autoAuthorize(false),
        _principalIdProperty() {
    }

    SaslAuthenticationSession::~SaslAuthenticationSession() {}

    Status SaslAuthenticationSession::start(const StringData& mechanism,
                                            int64_t conversationId,
                                            bool autoAuthorize) {
        fassert(4001, conversationId > 0);
        if (_conversationId != 0) {
            return Status(ErrorCodes::InternalError,
                          "Cannot call start twice on same SaslAuthenticationSession.");
        }

        const SaslMechanismInfo* mechInfo = mapFindWithDefault(
                _supportedSaslMechanisms,
                mechanism.toString(),
                static_cast<const SaslMechanismInfo*>(NULL));

        if (NULL == mechInfo) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Unsupported mechanism " << mechanism);
        }

        _conversationId = conversationId;
        _autoAuthorize = autoAuthorize;
        _principalIdProperty = mechInfo->principalIdProperty;
        return _gsaslSession.initializeServerSession(_gsaslLibraryContext, mechanism, this);
    }

    Status SaslAuthenticationSession::step(const StringData& inputData, std::string* outputData) {
        Status status = _gsaslSession.step(inputData, outputData);
        if (!status.isOK())
            return status;

        if (isDone()) {
            std::string principalName = getPrincipalId();
            Principal* principal = new Principal(PrincipalName(principalName, getPrincipalSource()));
            principal->setImplicitPrivilegeAcquisition(_autoAuthorize);
            getClient()->getAuthorizationManager()->addAuthorizedPrincipal(principal);
        }

        return status;
    }

    std::string SaslAuthenticationSession::getPrincipalId() const {
        return _gsaslSession.getProperty(_principalIdProperty);
    }

    std::vector<std::string> SaslAuthenticationSession::getSupportedMechanisms() {
        std::vector<std::string> result;
        for (SaslMechanismInfoMap::const_iterator iter = _supportedSaslMechanisms.begin(),
                 end = _supportedSaslMechanisms.end(); iter != end; ++iter) {

            result.push_back(iter->first);
        }
        return result;
    }

namespace {

    bool isExternalUserSource(const StringData& userSource) {
        return userSource == StringData("$external", StringData::LiteralTag()) ||
            userSource == StringData("$sasl", StringData::LiteralTag());
    }

    int gsaslCallbackFunction(Gsasl* gsasl, Gsasl_session* gsession, Gsasl_property property) {

        SaslAuthenticationSession* session = static_cast<SaslAuthenticationSession*>(
                gsasl_session_hook_get(gsession));

        switch (property) {
        case GSASL_SERVICE:
            gsasl_property_set(gsession, GSASL_SERVICE, saslServiceName.c_str());
            return GSASL_OK;
        case GSASL_HOSTNAME:
            gsasl_property_set(gsession, GSASL_HOSTNAME, saslHostName.c_str());
            return GSASL_OK;
        case GSASL_PASSWORD: {
            if (NULL == session)
                return GSASL_NO_CALLBACK;
            std::string dbname = session->getPrincipalSource();
            std::string username = session->getPrincipalId();
            if (isExternalUserSource(dbname)) {
                log() << "Server does not have password data for externally credentialed user "
                      << username << '.' << endl;
                return GSASL_NO_CALLBACK;
            }
            if (dbname.empty() || username.empty()) {

                log() << "sasl Bad database/user information: \"" << dbname << '/' << username
                      << '"' << endl;
                return GSASL_NO_CALLBACK;
            }
            BSONObj privilegeDocument;
            Status status = session->getClient()->getAuthorizationManager()->getPrivilegeDocument(
                    dbname, PrincipalName(username, dbname), &privilegeDocument);
            if (!status.isOK()) {
                log() << status.reason() << endl;
                return GSASL_NO_CALLBACK;
            }
            std::string hashedPassword;
            status = bsonExtractStringField(privilegeDocument, "pwd", &hashedPassword);
            if (!status.isOK()) {
                log() << "sasl No password data for " << username << " on database " << dbname
                      << endl;
                return GSASL_NO_CALLBACK;
            }
            gsasl_property_set(gsession, GSASL_PASSWORD, hashedPassword.c_str());
            return GSASL_OK;
        }
        case GSASL_VALIDATE_GSSAPI: {
            if (NULL == session)
                return GSASL_NO_CALLBACK;

            std::string dbname = session->getPrincipalSource();
            if (!isExternalUserSource(session->getPrincipalSource())) {

                log() << "GSSAPI/Kerberos authentication not allowed except against "
                    "$external or $sasl database" << endl;
                return GSASL_AUTHENTICATION_ERROR;
            }

            if (!str::equals(gsasl_property_fast(gsession, GSASL_GSSAPI_DISPLAY_NAME),
                             gsasl_property_fast(gsession, GSASL_AUTHZID))) {
                return GSASL_AUTHENTICATION_ERROR;
            }
            return GSASL_OK;
        }
        default:
            return GSASL_NO_CALLBACK;
        }
    }

    Status initializeSupportedMechanismMap() {
        std::set<std::string> desiredMechanisms(authenticationMechanisms.begin(),
                                                authenticationMechanisms.end());
        if (desiredMechanisms.erase(MECH_MONGOCR) == 0) {
            CmdAuthenticate::disableCommand();
        }

        for (std::set<std::string>::const_iterator iter = desiredMechanisms.begin(),
                 end = desiredMechanisms.end(); iter != end; ++iter) {
            GsaslSession gsession;
            Status status = gsession.initializeServerSession(_gsaslLibraryContext, *iter, NULL);
            if (!status.isOK()) {
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() <<
                              "Unsupported authenticationMechanism: \"" << *iter << "\": " <<
                              status.reason());
            }
        }

        for (SaslMechanismInfo* mechInfo = _mongoKnownMechanisms; mechInfo->name; ++mechInfo) {
            if (desiredMechanisms.count(mechInfo->name)) {
                _supportedSaslMechanisms[mechInfo->name] = mechInfo;
            }
        }

        return Status::OK();
    }

    MONGO_INITIALIZER(SaslAuthenticationLibrary)(InitializerContext* context) {
        fassert(4002, _gsaslLibraryContext == NULL);

        if (saslHostName.empty())
            saslHostName = getHostNameCached();
        if (saslServiceName.empty())
            saslServiceName = saslDefaultServiceName;

        if (!gsasl_check_version(GSASL_VERSION)) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() <<
                          "Incompatible gsasl library.  Minimum  version required is " GSASL_VERSION
                          ", but found " << gsasl_check_version(NULL));
        }

        int rc = gsasl_init(&_gsaslLibraryContext);
        if (GSASL_OK != rc)
            return Status(ErrorCodes::UnknownError, gsasl_strerror(rc));

        gsasl_callback_set(_gsaslLibraryContext, &gsaslCallbackFunction);

        Status status = initializeSupportedMechanismMap();
        if (!status.isOK())
            return status;

        return Status::OK();
    }
}  // namespace
}  // namespace mongo
