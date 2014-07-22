/*
 * Copyright (C) 2014 10gen, Inc.  All Rights Reserved.
 */

#include "cyrus_sasl_authentication_session.h"

#include <boost/range/size.hpp>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/commands.h"
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
    typedef bool (*AuthorizeUserFn)(CyrusSaslAuthenticationSession* session,
                                    const StringData& requestedUser,
                                    const StringData& authenticatedUser);

}  // namespace

    /**
     * POD Structure describing a supported SASL mechanism.
     */
    struct CyrusSaslAuthenticationSession::SaslMechanismInfo {
        /// Mechanism name.
        const char* name;

        /// Function to use to smoke test the mechanism at startup.
        SmokeTestMechanismFn smokeTestMechanism;

        /// Function to answer whether or not a user is authorized to access the system.
        AuthorizeUserFn isUserAuthorized;
    };

namespace {

    /**
     * Basic smoke test of SASL mechanism functionality, that any requested mechanism should pass.
     */
    Status smokeCommonMechanism(const StringData& mechanismName,
                                const StringData& serviceName,
                                const StringData& serviceHostname) {
        AuthorizationManager authzManager(new AuthzManagerExternalStateMock());
        AuthorizationSession authzSession(new AuthzSessionExternalStateMock(&authzManager));
        CyrusSaslAuthenticationSession session(&authzSession);
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
    bool isAuthorizedCommon(CyrusSaslAuthenticationSession* session,
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
    bool isAuthorizedGssapi(CyrusSaslAuthenticationSession* session,
                            const StringData& requestedUser,
                            const StringData& authenticatedUser) {

        std::string canonicalAuthenticatedUser;
        if (!gssapi::canonicalizeUserName(authenticatedUser, &canonicalAuthenticatedUser).isOK())
            return false;
        return isAuthorizedCommon(session, requestedUser, canonicalAuthenticatedUser);
    }

    /**
     * Callback registered on the sasl_conn_t underlying a CyrusSaslAuthenticationSession that 
     * allows the Cyrus SASL library to read runtime configuration options.
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

        CyrusSaslAuthenticationSession* session = 
            static_cast<CyrusSaslAuthenticationSession*>(context);
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
     * Callback registered on the sasl_conn_t underlying a CyrusSaslAuthenticationSession that 
     * confirms the authenticated user is allowed to act as the requested user.
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

        CyrusSaslAuthenticationSession* session = 
            static_cast<CyrusSaslAuthenticationSession*>(context);

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
    const int CyrusSaslAuthenticationSession::mongoSessionCallbackId = 0xF00F;

    /// NULL-terminated list of SaslMechanismInfos describing the mechanisms MongoDB knows how to
    /// support.
    CyrusSaslAuthenticationSession::SaslMechanismInfo _mongoKnownMechanisms[] = {
        { SaslAuthenticationSession::mechanismCRAMMD5, smokeCommonMechanism, isAuthorizedCommon },
        { SaslAuthenticationSession::mechanismSCRAMSHA1, smokeCommonMechanism, isAuthorizedCommon },
        { SaslAuthenticationSession::mechanismGSSAPI, smokeGssapiMechanism, isAuthorizedGssapi },
        { SaslAuthenticationSession::mechanismPLAIN, smokeCommonMechanism, isAuthorizedCommon },
        { NULL }
    };

    /**
     * Returns the SaslMechanismInfo for "mechanism", or NULL if there is none.
     */
    const CyrusSaslAuthenticationSession::SaslMechanismInfo* _findMechanismInfo(
            const StringData& mechanism) {

        for (CyrusSaslAuthenticationSession::SaslMechanismInfo* mechInfo = _mongoKnownMechanisms;
             mechInfo->name != NULL; ++mechInfo) {

            if (mechanism == mechInfo->name)
                return mechInfo;
        }
        return NULL;
    }

    // static
    Status CyrusSaslAuthenticationSession::smokeTestMechanism(const StringData& mechanism,
                                                         const StringData& serviceName,
                                                         const StringData& serviceHostname) {

        const SaslMechanismInfo* mechInfo = _findMechanismInfo(mechanism);
        if (NULL == mechInfo) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Unsupported mechanism " << mechanism);
        }
        return mechInfo->smokeTestMechanism(mechanism, serviceName, serviceHostname);
    }

    CyrusSaslAuthenticationSession::CyrusSaslAuthenticationSession(
        AuthorizationSession* authzSession) :
        SaslAuthenticationSession(authzSession),
        _saslConnection(NULL),
        _mechInfo(NULL){

        const sasl_callback_t callbackTemplate[maxCallbacks] = {
            { SASL_CB_GETOPT, SaslCallbackFn(saslServerConnGetOpt), this },
            { SASL_CB_PROXY_POLICY, SaslCallbackFn(saslServerConnAuthorize), this },
            { mongoSessionCallbackId, saslAlwaysFailCallback, this },
            { SASL_CB_LIST_END }
        };
        std::copy(callbackTemplate, callbackTemplate + maxCallbacks, _callbacks);
    }

    CyrusSaslAuthenticationSession::~CyrusSaslAuthenticationSession() {
        if (_saslConnection)
            sasl_dispose(&_saslConnection);
    }

    Status CyrusSaslAuthenticationSession::start(const StringData& authenticationDatabase,
                                                 const StringData& mechanism,
                                                 const StringData& serviceName,
                                                 const StringData& serviceHostname,
                                                 int64_t conversationId,
                                                 bool autoAuthorize) {
        fassert(4001, conversationId > 0);

        if (_conversationId != 0) {
            return Status(ErrorCodes::AlreadyInitialized,
                          "Cannot call start() twice on same CyrusSaslAuthenticationSession.");
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

    Status CyrusSaslAuthenticationSession::step(const StringData& inputData,
                                                std::string* outputData) {
        int result;
        const char* output;
        unsigned outputLen;
        const char* const input = inputData.empty() ? NULL : inputData.rawData();
        const unsigned inputLen = static_cast<unsigned>(inputData.size());
        if (0 == _saslStep) {
            // Cyrus SASL uses "SCRAM" as the internal mechanism name
            std::string mechName = strcmp(_mechInfo->name, "SCRAM-SHA-1") == 0 ?
                "SCRAM" : _mechInfo->name;
            log() << "calling sasl_server_start sasl_step is " << _saslStep;
            result = sasl_server_start(_saslConnection,
                                       mechName.c_str(),
                                       input,
                                       inputLen,
                                       &output,
                                       &outputLen);
        }
        else {
            log() << "calling sasl_server_step sasl_step is " << _saslStep;
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

    std::string CyrusSaslAuthenticationSession::getPrincipalId() const {
        const void* principalId;
        if (SASL_OK == sasl_getprop(_saslConnection, SASL_USERNAME, &principalId))
            return static_cast<const char*>(principalId);
        return std::string();
    }

    const char* CyrusSaslAuthenticationSession::getMechanism() const {
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

    SaslAuthenticationSession* createCyrusSaslAuthenticationSession(
        AuthorizationSession* authzSession) {
        return new CyrusSaslAuthenticationSession(authzSession);
    }

    // This group is used to ensure that all the plugins are registered before we attempt
    // the smoke test in SaslCommands.
    MONGO_INITIALIZER_GROUP(CyrusSaslAllPluginsRegistered, MONGO_NO_PREREQUISITES, 
                            MONGO_NO_DEPENDENTS);

    MONGO_INITIALIZER_WITH_PREREQUISITES(CyrusSaslServerCore,
                                         ("CyrusSaslAllocatorsAndMutexes",
                                          "SaslClientContext",
                                          "NativeSaslServerCore"))
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

        SaslAuthenticationSession::create = createCyrusSaslAuthenticationSession;
        
        return Status::OK();
    }

}  // namespace
}  // namespace mongo
