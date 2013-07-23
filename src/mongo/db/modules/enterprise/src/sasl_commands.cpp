/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include <algorithm>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/mongo_authentication_session.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/stringutils.h"
#include "sasl_authentication_session.h"

namespace mongo {
namespace {

    using namespace mongoutils;

    /// Split string "s" at instances of "delim" into a vector of strings, and return the result.
    std::vector<std::string> stringSplit(const std::string& s, char delim) {
        std::vector<std::string> result;
        splitStringDelim(s, &result, delim);
        return result;
    }

    MONGO_EXPORT_STARTUP_SERVER_PARAMETER(
            authenticationMechanisms, std::vector<std::string>, stringSplit("MONGODB-CR", ','));

    MONGO_EXPORT_STARTUP_SERVER_PARAMETER(saslHostName, std::string, "");
    MONGO_EXPORT_STARTUP_SERVER_PARAMETER(saslServiceName, std::string, "");

    const bool autoAuthorizeDefault = true;

    // The name we give to the nonce-authenticate mechanism in the free product.
    const char mechanismMONGODBCR[] = "MONGODB-CR";

    class CmdSaslStart : public Command {
    public:
        CmdSaslStart();
        virtual ~CmdSaslStart();

        virtual void addRequiredPrivileges(
                const std::string&, const BSONObj&, std::vector<Privilege>*) {}

        virtual bool run(const std::string& db,
                         BSONObj& cmdObj,
                         int options,
                         std::string& ignored,
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

        virtual void addRequiredPrivileges(
                const std::string&, const BSONObj&, std::vector<Privilege>*) {}

        virtual bool run(const std::string& db,
                         BSONObj& cmdObj,
                         int options,
                         std::string& ignored,
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
            fassertFailed(4003);
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
        builder->append("ok", status.isOK() ? 1.0: 0.0);
        if (!status.isOK())
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
            log() << session->getMechanism() << " authentication failed for " <<
                session->getPrincipalId() << " on " <<
                session->getAuthenticationDatabase() << " ; " << status.toString() << std::endl;
            // All the client needs to know is that authentication has failed.
            return Status(ErrorCodes::AuthenticationFailed, "Authentication failed.");
        }

        status = buildResponse(session, responsePayload, type, result);
        if (!status.isOK())
            return status;

        if (session->isDone()) {
            Principal* principal = new Principal(
                    UserName(session->getPrincipalId(), session->getAuthenticationDatabase()));
            session->getAuthorizationSession()->addAndAuthorizePrincipal(principal);

            log() << "Successfully authenticated as principal " <<
                session->getPrincipalId() << " on " << session->getAuthenticationDatabase() <<
                std::endl;
        }
        return Status::OK();
    }


    Status doSaslStart(SaslAuthenticationSession* session,
                       const std::string& db, 
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

        if (!sequenceContains(authenticationMechanisms, mechanism)) {
            result->append(saslCommandMechanismListFieldName, authenticationMechanisms);
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Unsupported mechanism " << mechanism);
        }

        status = session->start(db,
                                mechanism,
                                saslServiceName,
                                saslHostName,
                                1,
                                autoAuthorize);
        if (!status.isOK())
            return status;

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
                           std::string& ignored,
                           BSONObjBuilder& result,
                           bool fromRepl) {

        ClientBasic* client = ClientBasic::getCurrent();
        client->resetAuthenticationSession(NULL);

        SaslAuthenticationSession* session = new SaslAuthenticationSession(
                client->getAuthorizationSession());
        boost::scoped_ptr<AuthenticationSession> sessionGuard(session);

        Status status = doSaslStart(session, db, cmdObj, &result);
        addStatus(status, &result);

        if (status.isOK() && !session->isDone())
            client->swapAuthenticationSession(sessionGuard);

        return status.isOK();
    }

    CmdSaslContinue::CmdSaslContinue() : Command(saslContinueCommandName) {}
    CmdSaslContinue::~CmdSaslContinue() {}

    void CmdSaslContinue::help(std::stringstream& os) const {
        os << "Subsequent steps in a SASL authentication conversation.";
    }


    bool CmdSaslContinue::run(const std::string& db,
                              BSONObj& cmdObj,
                              int options,
                              std::string& ignored,
                              BSONObjBuilder& result,
                              bool fromRepl) {

        ClientBasic* client = ClientBasic::getCurrent();
        boost::scoped_ptr<AuthenticationSession> sessionGuard(NULL);
        client->swapAuthenticationSession(sessionGuard);

        if (!sessionGuard || sessionGuard->getType() != AuthenticationSession::SESSION_TYPE_SASL) {
            addStatus(Status(ErrorCodes::ProtocolError, "No SASL session state found"), &result);
            return false;
        }

        SaslAuthenticationSession* session =
            static_cast<SaslAuthenticationSession*>(sessionGuard.get());

        if (session->getAuthenticationDatabase() != db) {
            addStatus(Status(ErrorCodes::ProtocolError,
                             "Attempt to switch database target during SASL authentication."),
                      &result);
            return false;
        }

        Status status = doSaslContinue(session, cmdObj, &result);
        addStatus(status, &result);

        if (status.isOK() && !session->isDone())
            client->swapAuthenticationSession(sessionGuard);

        return status.isOK();
    }

    MONGO_INITIALIZER_WITH_PREREQUISITES(SaslCommands, ("CyrusSaslServerLibrary"))(
            InitializerContext*) {

        if (saslHostName.empty())
            saslHostName = getHostNameCached();
        if (saslServiceName.empty())
            saslServiceName = saslDefaultServiceName;

        if (!sequenceContains(authenticationMechanisms, mechanismMONGODBCR))
            CmdAuthenticate::disableCommand();

        for (size_t i = 0; i < authenticationMechanisms.size(); ++i) {
            const std::string& mechanism = authenticationMechanisms[i];
            if (mechanism == mechanismMONGODBCR) {
                // Not a SASL mechanism; no need to smoke test the built-in mechanism.
                continue;
            }
            Status status = SaslAuthenticationSession::smokeTestMechanism(
                    authenticationMechanisms[i],
                    saslServiceName,
                    saslHostName);
            if (!status.isOK())
                return status;
        }
        return Status::OK();
    }

}  // namespace
}  // namespace mongo
