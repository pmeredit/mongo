/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include <gsasl.h>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/mongo_authentication_session.h"
#include "mongo/db/auth/principal.h"
#include "mongo/db/client_common.h"
#include "mongo/db/commands.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/stringutils.h"
#include "sasl_authentication_session.h"

namespace mongo {
namespace {

    using namespace mongoutils;

    const bool autoAuthorizeDefault = true;

    Gsasl* _gsaslLibraryContext = NULL;

    std::vector<std::string> getSupportedServerMechanisms();

    class CmdSaslStart : public Command {
    public:
        CmdSaslStart();
        virtual ~CmdSaslStart();

        virtual bool run(const std::string& db,
                         BSONObj& cmdObj,
                         int options,
                         std::string& errmsg,
                         BSONObjBuilder& result,
                         bool fromRepl);

        virtual void help(stringstream& help) const;
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return true; }
        virtual bool requiresAuth() { return false; }

    };

    class CmdSaslContinue : public Command {
    public:
        CmdSaslContinue();
        virtual ~CmdSaslContinue();

        virtual bool run(const std::string& db,
                         BSONObj& cmdObj,
                         int options,
                         std::string& errmsg,
                         BSONObjBuilder& result,
                         bool fromRepl);

        virtual void help(stringstream& help) const;
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return true; }
        virtual bool requiresAuth() { return false; }
    };

    CmdSaslStart cmdSaslStart;
    CmdSaslContinue cmdSaslContinue;

    Status buildResponse(const SaslAuthenticationSession* session,
                         const std::string& responsePayload,
                         BSONType responsePayloadType,
                         BSONObjBuilder* result) {
        result->appendIntOrLL(saslCommandConversationIdFieldName, session->getConversationId());
        result->appendBool(saslCommandDoneFieldName, session->isDone());

        if (responsePayload.size() > size_t(std::numeric_limits<int>::max())) {
            return Status(ErrorCodes::InvalidLength, "Response payload too long");
        }
        if (responsePayloadType == BinData) {
            result->appendBinData(saslCommandPayloadFieldName,
                                  int(responsePayload.size()),
                                  BinDataGeneral,
                                  responsePayload.data());
        }
        else if (responsePayloadType == String) {
            result->append(saslCommandPayloadFieldName, base64::encode(responsePayload));
        }
        else {
            fassertFailed(0);
        }

        return Status::OK();
    }

    Status extractConversationId(const BSONObj& cmdObj, int64_t* conversationId) {
        BSONElement element;
        Status status = bsonExtractField(cmdObj, saslCommandConversationIdFieldName, &element);
        if (!status.isOK())
            return status;

        if (!element.isNumber()) {
            return Status(ErrorCodes::TypeMismatch,
                          str::stream() << "Wrong type for field; expected number for " << element);
        }
        *conversationId = element.numberLong();
        return Status::OK();
    }

    Status extractMechanism(const BSONObj& cmdObj, std::string* mechanism) {
        return bsonExtractStringField(cmdObj, saslCommandMechanismFieldName, mechanism);
    }

    void addStatus(const Status& status, BSONObjBuilder* builder) {
        builder->append(saslCommandCodeFieldName, status.code());
        if (!status.reason().empty())
            builder->append(saslCommandErrmsgFieldName, status.reason());
    }

    Status doSaslStep(SaslAuthenticationSession* session,
                      const BSONObj& cmdObj,
                      BSONObjBuilder* result) {

        std::string payload;
        BSONType type = EOO;
        Status status = saslExtractPayload(cmdObj, &payload, &type);
        if (!status.isOK())
            return status;

        std::string responsePayload;
        status = session->step(payload, &responsePayload);
        if (!status.isOK()) {
            log() << "sasl " << status.codeString() << ": " << status.reason() << endl;
            return status;
        }

        status = buildResponse(session, responsePayload, type, result);
        if (!status.isOK())
            return status;

        if (session->isDone()) {
            log() << "SASL: Successfully authenticated as principal: " << session->getPrincipalId()
                    << std::endl;

        }
        return Status::OK();
    }


    Status doSaslStart(SaslAuthenticationSession* session,
                       const BSONObj& cmdObj,
                       BSONObjBuilder* result) {

        bool autoAuthorize = false;
        Status status = bsonExtractBooleanFieldWithDefault(cmdObj,
                                                           saslCommandAutoAuthorizeFieldName,
                                                           autoAuthorizeDefault,
                                                           &autoAuthorize);
        if (!status.isOK())
            return status;

        std::string mechanism;
        status = extractMechanism(cmdObj, &mechanism);
        if (!status.isOK())
            return status;

        status = session->start(_gsaslLibraryContext, mechanism, 1, autoAuthorize);
        if (status == ErrorCodes::BadValue) {
            result->append(saslCommandMechanismListFieldName, getSupportedServerMechanisms());
            return status;
        }
        else if (!status.isOK()) {
            return status;
        }

        return doSaslStep(session, cmdObj, result);
    }

    Status doSaslContinue(SaslAuthenticationSession* session,
                          const BSONObj& cmdObj,
                          BSONObjBuilder* result) {

        int64_t conversationId = 0;
        Status status = extractConversationId(cmdObj, &conversationId);
        if (!status.isOK())
            return status;
        if (conversationId != session->getConversationId())
            return Status(ErrorCodes::ProtocolError, "sasl: Mismatched conversation id");

        return doSaslStep(session, cmdObj, result);
    }

    CmdSaslStart::CmdSaslStart() : Command(saslStartCommandName) {}
    CmdSaslStart::~CmdSaslStart() {}

    void CmdSaslStart::help(std::stringstream& os) const {
        os << "First step in a SASL authentication conversation.";
    }

    bool CmdSaslStart::run(const std::string& db,
                           BSONObj& cmdObj,
                           int options,
                           std::string& errmsg,
                           BSONObjBuilder& result,
                           bool fromRepl) {

        ClientBasic* client = ClientBasic::getCurrent();
        client->resetAuthenticationSession(NULL);

        SaslAuthenticationSession* session =
            new SaslAuthenticationSession(ClientBasic::getCurrent());
        boost::scoped_ptr<AuthenticationSession> sessionGuard(session);

        Status status = doSaslStart(session, cmdObj, &result);
        addStatus(status, &result);

        if (status.isOK() && !session->isDone())
            client->swapAuthenticationSession(sessionGuard);

        return true;
    }

    CmdSaslContinue::CmdSaslContinue() : Command(saslContinueCommandName) {}
    CmdSaslContinue::~CmdSaslContinue() {}

    void CmdSaslContinue::help(std::stringstream& os) const {
        os << "Subsequent steps in a SASL authentication conversation.";
    }


    bool CmdSaslContinue::run(const std::string& db,
                              BSONObj& cmdObj,
                              int options,
                              std::string& errmsg,
                              BSONObjBuilder& result,
                              bool fromRepl) {

        ClientBasic* client = ClientBasic::getCurrent();
        boost::scoped_ptr<AuthenticationSession> sessionGuard(NULL);
        client->swapAuthenticationSession(sessionGuard);

        if (!sessionGuard || sessionGuard->getType() != AuthenticationSession::SESSION_TYPE_SASL) {
            addStatus(Status(ErrorCodes::ProtocolError, "sasl: No session state found"), &result);
            return true;
        }

        SaslAuthenticationSession* session =
            static_cast<SaslAuthenticationSession*>(sessionGuard.get());

        Status status = doSaslContinue(session, cmdObj, &result);
        addStatus(status, &result);

        if (status.isOK() && !session->isDone())
            client->swapAuthenticationSession(sessionGuard);

        return true;
    }

    std::vector<std::string> getSupportedServerMechanisms() {
        char* mechsString;
        fassert(0, !gsasl_server_mechlist(_gsaslLibraryContext, &mechsString));
        std::vector<std::string> result;
        splitStringDelim(mechsString, &result, ' ');
        free(mechsString);
        return result;
    }

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

    MONGO_INITIALIZER(SaslCommands)(InitializerContext* context) {
        fassert(0, _gsaslLibraryContext == NULL);

        if (!gsasl_check_version(GSASL_VERSION))
            return Status(ErrorCodes::UnknownError, "Incompatible gsasl library.");

        int rc = gsasl_init(&_gsaslLibraryContext);
        if (GSASL_OK != rc)
            return Status(ErrorCodes::UnknownError, gsasl_strerror(rc));

        gsasl_callback_set(_gsaslLibraryContext, &gsaslCallbackFunction);

        return Status::OK();
    }

}  // namespace
}  // namespace mongo
