/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_authentication_session.h"

#include <boost/range/size.hpp>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/commands.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo_gssapi.h"
#include "sasl_options.h"

namespace mongo {

namespace {

    /**
     * Signature of a function that can be used to smoke-test a SASL mechanism at
     * startup, to see if it is likely to work in the current environment.
     */
    typedef Status (*SmokeTestMechanismFn)(const StringData& mechanismName,
                                           const StringData& serviceName,
                                           const StringData& serviceHostname);

    /**
     * Signature of a function used to determine if "authenticatedUser" is authorized to act as
     * "requestedUser".  This is the final step in completing an authentication.
     *
     * The "session" and "conn" objects are made available for context.
     */
    typedef bool (*AuthorizeUserFn)(SaslAuthenticationSession* session,
                                    const StringData& requestedUser,
                                    const StringData& authenticatedUser);

}  // namespace

    /**
     * POD Structure describing a supported SASL mechanism.
     */
    struct SaslAuthenticationSession::SaslMechanismInfo {
        /// Mechanism name.
        const char* name;

        /// Function to use to smoke test the mechanism at startup.
        SmokeTestMechanismFn smokeTestMechanism;

        /// Function to answer whether or not a user is authorized to access the system.
        AuthorizeUserFn isUserAuthorized;
    };

namespace {

    // Mechanism name constants.
    const char mechanismCRAMMD5[] = "CRAM-MD5";
    const char mechanismDIGESTMD5[] = "DIGEST-MD5";
    const char mechanismGSSAPI[] = "GSSAPI";
    const char mechanismPLAIN[] = "PLAIN";

    /**
     * Basic smoke test of SASL mechanism functionality, that any requested mechanism should pass.
     */
    Status smokeCommonMechanism(const StringData& mechanismName,
                                const StringData& serviceName,
                                const StringData& serviceHostname) {
        AuthorizationManager authzManager(new AuthzManagerExternalStateMock());
        AuthorizationSession authzSession(new AuthzSessionExternalStateMock(&authzManager));
        SaslAuthenticationSession session(&authzSession);
        OperationContextNoop txn;
        Status status = session.start("test",
                                      mechanismName,
                                      serviceName,
                                      serviceHostname,
                                      1,
                                      true);
        session.setOpCtxt(&txn);
        if (status.isOK()) {
            std::string ignored;
            status = session.step("", &ignored);
        }
        return status;
    }

    /**
     * Smoke test of GSSAPI functionality in addition to basic SASL mechanism functionality.
     */
    Status smokeGssapiMechanism(const StringData& mechanismName,
                                const StringData& serviceName,
                                const StringData& serviceHostname) {
        Status status = smokeCommonMechanism(mechanismName, serviceName, serviceHostname);
        if (!status.isOK())
            return status;
        return gssapi::tryAcquireServerCredential(
                static_cast<std::string>(mongoutils::str::stream() << serviceName <<  "@" <<
                                         serviceHostname));
    }

    /**
     * Standard method in mongodb for determining if "authenticatedUser" may act as "requestedUser."
     *
     * The standard rule in MongoDB is simple.  The authenticated user name must be the same as the
     * requested user name.
     */
    bool isAuthorizedCommon(SaslAuthenticationSession* session,
                            const StringData& requestedUser,
                            const StringData& authenticatedUser) {

        return requestedUser == authenticatedUser;
    }

    /**
     * GSSAPI-specific method for determining if "authenticatedUser" may act as "requestedUser."
     *
     * The GSSAPI mechanism in Cyrus SASL strips the kerberos realm from the authenticated user
     * name, if it matches the server realm.  So, for GSSAPI authentication, we must re-canonicalize
     * the authenticated user name before validating it..
     */
    bool isAuthorizedGssapi(SaslAuthenticationSession* session,
                            const StringData& requestedUser,
                            const StringData& authenticatedUser) {

        std::string canonicalAuthenticatedUser;
        if (!gssapi::canonicalizeUserName(authenticatedUser, &canonicalAuthenticatedUser).isOK())
            return false;
        return isAuthorizedCommon(session, requestedUser, canonicalAuthenticatedUser);
    }

    /// NULL-terminated list of SaslMechanismInfos describing the mechanisms MongoDB knows how to
    /// support.
    SaslAuthenticationSession::SaslMechanismInfo _mongoKnownMechanisms[] = {
        { mechanismCRAMMD5, smokeCommonMechanism, isAuthorizedCommon },
        { mechanismGSSAPI, smokeGssapiMechanism, isAuthorizedGssapi },
        { mechanismPLAIN, smokeCommonMechanism, isAuthorizedCommon },
        { NULL }
    };

    /**
     * Returns the SaslMechanismInfo for "mechanism", or NULL if there is none.
     */
    const SaslAuthenticationSession::SaslMechanismInfo* _findMechanismInfo(
            const StringData& mechanism) {

        for (SaslAuthenticationSession::SaslMechanismInfo* mechInfo = _mongoKnownMechanisms;
             mechInfo->name != NULL; ++mechInfo) {

            if (mechanism == mechInfo->name)
                return mechInfo;
        }
        return NULL;
    }

    /**
     * Callback registered on the sasl_conn_t underlying a SaslAuthenticationSession that allows
     * the Cyrus SASL library to read runtime configuration options.
     *
     * Implements the sasl_getopt_t interface, which requires that the memory behind the result
     * stored into *outResult stay in scope until the underlying sasl_conn_t is destroyed.
     */
    int saslServerConnGetOpt(void* context,
                             const char* pluginNameRaw,
                             const char* optionRaw,
                             const char** outResult,
                             unsigned* outLen) throw () {

        static const char mongodbAuxpropMechanism[] = "MongoDBInternalAuxprop";
        static const char mongodbCanonMechanism[] = "MongoDBInternalCanon";

        unsigned ignored;
        if (!outLen)
            outLen = &ignored;

        SaslAuthenticationSession* session = static_cast<SaslAuthenticationSession*>(context);
        if (!session || !optionRaw || !outResult)
            return SASL_BADPARAM;

        const StringData option = optionRaw;

        if (option == StringData("log_level", StringData::LiteralTag())) {
            // Returns the log verbosity level for the SASL library.
            static const char saslLogLevel[] = "3";  // 3 is SASL_LOG_WARN.
            *outResult = saslLogLevel;
            *outLen = static_cast<unsigned>(boost::size(saslLogLevel));
            return SASL_OK;
        }

        if (option == StringData("auxprop_plugin", StringData::LiteralTag())) {
            // Returns the name of the plugin to use to look up user properties.  We use a custom
            // one that extracts the information from user privilege documents.
            *outResult = mongodbAuxpropMechanism;
            *outLen = static_cast<unsigned>(boost::size(mongodbAuxpropMechanism));
            return SASL_OK;
        }

        if (option == StringData("canon_user_plugin", StringData::LiteralTag())) {
            // Returns the name of the plugin to use to canonicalize user names.  We use a custome
            // plugin that only strips leading and trailing whitespace.  The default plugin also
            // appends realm information, which MongoDB does not expect.
            *outResult = mongodbCanonMechanism;
            *outLen = static_cast<unsigned>(boost::size(mongodbCanonMechanism));
            return SASL_OK;
        }

        if (option == StringData("pwcheck_method", StringData::LiteralTag())) {
            static const char pwcheckAuxprop[] = "auxprop";
            static const char pwcheckAuthd[] = "saslauthd";
            if (session->getAuthenticationDatabase() == "$external") {
                *outResult = pwcheckAuthd;
                *outLen = boost::size(pwcheckAuthd);
            }
            else {
                *outResult = pwcheckAuxprop;
                *outLen = boost::size(pwcheckAuxprop);
            }
            return SASL_OK;
        }

        if (option == StringData("saslauthd_path", StringData::LiteralTag())) {
            if (saslGlobalParams.authdPath.empty())
                return SASL_FAIL;
            *outResult = saslGlobalParams.authdPath.c_str();
            *outLen = static_cast<unsigned>(saslGlobalParams.authdPath.size());
            return SASL_OK;
        }

        return SASL_FAIL;
    }

    /**
     * Callback registered on the sasl_conn_t underlying a SaslAuthenticationSession that confirms
     * the authenticated user is allowed to act as the requested user.
     *
     * Implements the interface sasl_authorize_t.
     */
    int saslServerConnAuthorize(sasl_conn_t* conn,
                                void* context,
                                const char* requestedUserRaw,
                                unsigned requestedUserLen,
                                const char* authenticatedIdentityRaw,
                                unsigned authenticatedIdentityLen,
                                const char* defaultRealmRaw,
                                unsigned defaultRealmLen,
                                struct propctx* properties) throw () {

        if (!conn || !context || !requestedUserRaw || !authenticatedIdentityRaw)
            return SASL_BADPARAM;

        SaslAuthenticationSession* session = static_cast<SaslAuthenticationSession*>(context);

        StringData requestedUser(requestedUserRaw, requestedUserLen);
        StringData authenticatedIdentity(authenticatedIdentityRaw, authenticatedIdentityLen);
        if (!session->getMechInfo()->isUserAuthorized(session,
                                                      requestedUser,
                                                      authenticatedIdentity)) {
            sasl_seterror(conn, 0, "saslServerConnAuthorize: "
                          "Requested identity not authenticated identity");
            return SASL_BADAUTH;
        }
        return SASL_OK;
    }

    int saslAlwaysFailCallback() throw () {
        return SASL_FAIL;
    }

    /**
     * Type of pointer used to store SASL callback functions.
     */
    typedef int (*SaslCallbackFn)();
}  // namespace

    /// This value chosen because it is unused, and unlikely to be used by the SASL library.

    // static
    const int SaslAuthenticationSession::mongoSessionCallbackId = 0xF00F;

    // static
    Status SaslAuthenticationSession::smokeTestMechanism(const StringData& mechanism,
                                                         const StringData& serviceName,
                                                         const StringData& serviceHostname) {

        const SaslMechanismInfo* mechInfo = _findMechanismInfo(mechanism);
        if (NULL == mechInfo) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Unsupported mechanism " << mechanism);
        }
        return mechInfo->smokeTestMechanism(mechanism, serviceName, serviceHostname);
    }

    SaslAuthenticationSession::SaslAuthenticationSession(AuthorizationSession* authzSession) :
        AuthenticationSession(AuthenticationSession::SESSION_TYPE_SASL),
        _authzSession(authzSession),
        _saslConnection(NULL),
        _saslStep(0),
        _mechInfo(NULL),
        _conversationId(0),
        _autoAuthorize(false),
        _done(false) {

        const sasl_callback_t callbackTemplate[maxCallbacks] = {
            { SASL_CB_GETOPT, SaslCallbackFn(saslServerConnGetOpt), this },
            { SASL_CB_PROXY_POLICY, SaslCallbackFn(saslServerConnAuthorize), this },
            { mongoSessionCallbackId, saslAlwaysFailCallback, this },
            { SASL_CB_LIST_END }
        };
        std::copy(callbackTemplate, callbackTemplate + maxCallbacks, _callbacks);
    }

    SaslAuthenticationSession::~SaslAuthenticationSession() {
        if (_saslConnection)
            sasl_dispose(&_saslConnection);
    }

    StringData SaslAuthenticationSession::getAuthenticationDatabase() const {
        if (Command::testCommandsEnabled &&
                _authenticationDatabase == "admin" &&
                getPrincipalId() == internalSecurity.user->getName().getUser()) {
            // Allows authenticating as the internal user against the admin database.  This is to
            // support the auth passthrough test framework on mongos (since you can't use the local
            // database on a mongos, so you can't auth as the internal user without this).
            return internalSecurity.user->getName().getDB();
        } else {
            return _authenticationDatabase;
        }
    }

    Status SaslAuthenticationSession::start(const StringData& authenticationDatabase,
                                            const StringData& mechanism,
                                            const StringData& serviceName,
                                            const StringData& serviceHostname,
                                            int64_t conversationId,
                                            bool autoAuthorize) {
        fassert(4001, conversationId > 0);

        if (_conversationId != 0) {
            return Status(ErrorCodes::AlreadyInitialized,
                          "Cannot call start() twice on same SaslAuthenticationSession.");
        }

        _authenticationDatabase = authenticationDatabase.toString();
        _serviceName = serviceName.toString();
        _serviceHostname = serviceHostname.toString();
        _conversationId = conversationId;
        _autoAuthorize = autoAuthorize;
        _mechInfo = _findMechanismInfo(mechanism);

        if (NULL == _mechInfo) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Unsupported mechanism " << mechanism);
        }

        int result = sasl_server_new(_serviceName.c_str(),             // service
                                     _serviceHostname.c_str(),         // serviceFQDN
                                     NULL,                             // user_realm
                                     NULL,                             // iplocalport
                                     NULL,                             // ipremoteport
                                     _callbacks,                       // callbacks
                                     0,                                // flags
                                     &_saslConnection);                // pconn
        if (SASL_OK != result) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() << sasl_errstring(result, NULL, NULL));
        }

        return Status::OK();
    }

    Status SaslAuthenticationSession::step(const StringData& inputData, std::string* outputData) {
        int result;
        const char* output;
        unsigned outputLen;
        const char* const input = inputData.empty() ? NULL : inputData.rawData();
        const unsigned inputLen = static_cast<unsigned>(inputData.size());
        if (0 == _saslStep) {
            result = sasl_server_start(_saslConnection,
                                       _mechInfo->name,
                                       input,
                                       inputLen,
                                       &output,
                                       &outputLen);
        }
        else {
            result = sasl_server_step(_saslConnection,
                                      input,
                                      inputLen,
                                      &output,
                                      &outputLen);
        }
        _done = (SASL_CONTINUE != result);
        switch (result) {
        case SASL_OK: {
            _done = true;
            *outputData = std::string();
            ++_saslStep;
            return Status::OK();
        }
        case SASL_CONTINUE:
            *outputData = std::string(output, outputLen);
            ++_saslStep;
            return Status::OK();
        case SASL_NOMECH:
            return Status(ErrorCodes::BadValue, sasl_errdetail(_saslConnection));
        case SASL_BADAUTH:
            return Status(ErrorCodes::AuthenticationFailed, sasl_errdetail(_saslConnection));
        default:
            return Status(ErrorCodes::ProtocolError, sasl_errdetail(_saslConnection));
        }
    }

    std::string SaslAuthenticationSession::getPrincipalId() const {
        const void* principalId;
        if (SASL_OK == sasl_getprop(_saslConnection, SASL_USERNAME, &principalId))
            return static_cast<const char*>(principalId);
        return std::string();
    }

    const char* SaslAuthenticationSession::getMechanism() const {
        if (_mechInfo && _mechInfo->name)
            return _mechInfo->name;
        return "";
    }

namespace {

    /**
     * Implementation of sasl_log_t for handling log messages generated by the SASL library.
     */
    int saslServerGlobalLog(void* context, int level, const char* message) throw () {
        switch(level) {
        case SASL_LOG_NONE:
            break;
        case SASL_LOG_ERR:
            error() << message << endl;
            break;
        case SASL_LOG_FAIL:
            // Logged elsewhere.
            break;
        case SASL_LOG_WARN:
            warning() << message << endl;
            break;
        case SASL_LOG_NOTE:
            log() << message << endl;
            break;
        case SASL_LOG_DEBUG:
        case SASL_LOG_TRACE:
            LOG(1) << message << endl;
            break;
        case SASL_LOG_PASS:
            // Don't log trace data that includes passwords.
            break;
        default:
            error() << "Unexpected sasl log level " << level << endl;
            break;
        }
        return SASL_OK;
    }

    // This group is used to ensure that all the plugins are registered before we attempt
    // the smoke test in SaslCommands.
    MONGO_INITIALIZER_GROUP(CyrusSaslAllPluginsRegistered, MONGO_NO_PREREQUISITES, 
                            MONGO_NO_DEPENDENTS);

    MONGO_INITIALIZER_WITH_PREREQUISITES(CyrusSaslServerCore,
                                         ("CyrusSaslAllocatorsAndMutexes",
                                          "SaslClientContext"))
        (InitializerContext* context) {

        static const sasl_callback_t saslServerGlobalCallbacks[] = {
            { SASL_CB_LOG, SaslCallbackFn(saslServerGlobalLog), NULL },
            { SASL_CB_LIST_END }
        };

        int result = sasl_server_init(saslServerGlobalCallbacks, "mongodb");
        if (result != SASL_OK) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() <<
                          "Could not initialize sasl server components (" <<
                          sasl_errstring(result, NULL, NULL) <<
                          ")");
        }
        return Status::OK();
    }

}  // namespace
}  // namespace mongo
