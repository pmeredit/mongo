/*
 * Copyright (C) 2014 10gen, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include <sasl/sasl.h>

#include "cyrus_sasl_authentication_session.h"

#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/stringutils.h"
#include "mongo_gssapi.h"

#include "../ldap/ldap_manager.h"
#include "../ldap/ldap_options.h"

namespace mongo {

namespace {

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
                         unsigned* outLen) throw() {
    static constexpr StringData mongodbCanonMechanism = "MongoDBInternalCanon"_sd;

    try {
        unsigned ignored;
        if (!outLen)
            outLen = &ignored;

        ServerMechanismBase* session = static_cast<ServerMechanismBase*>(context);
        if (!session || !optionRaw || !outResult)
            return SASL_BADPARAM;

        const StringData option = optionRaw;

        if (option == "canon_user_plugin"_sd) {
            // Returns the name of the plugin to use to canonicalize user names.  We use a custome
            // plugin that only strips leading and trailing whitespace.  The default plugin also
            // appends realm information, which MongoDB does not expect.
            *outResult = mongodbCanonMechanism.rawData();
            *outLen = static_cast<unsigned>(mongodbCanonMechanism.size());
            return SASL_OK;
        }

        if ((option == "pwcheck_method"_sd) &&
            (session->getAuthenticationDatabase() == "$external")) {
            static constexpr StringData pwcheckAuthd = "saslauthd"_sd;
            *outResult = pwcheckAuthd.rawData();
            *outLen = pwcheckAuthd.size();
            return SASL_OK;
        }

        if (option == "saslauthd_path"_sd) {
            if (saslGlobalParams.authdPath.empty())
                return SASL_FAIL;
            *outResult = saslGlobalParams.authdPath.c_str();
            *outLen = static_cast<unsigned>(saslGlobalParams.authdPath.size());
            return SASL_OK;
        }

        return SASL_FAIL;
    } catch (...) {
        error() << "Unexpected exception in saslServerConnGetOpt: " << exceptionToStatus().reason();
        return SASL_FAIL;
    }
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
                            struct propctx* properties) throw() {
    if (!conn || !context || !requestedUserRaw || !authenticatedIdentityRaw)
        return SASL_BADPARAM;

    try {
        ServerMechanismBase* session = static_cast<ServerMechanismBase*>(context);

        StringData requestedUser(requestedUserRaw, requestedUserLen);
        StringData authenticatedIdentity(authenticatedIdentityRaw, authenticatedIdentityLen);
        if (!session->isAuthorizedToActAs(requestedUser, authenticatedIdentity)) {
            std::stringstream errorMsg;
            errorMsg << "saslServerConnAuthorize: Requested identity "
                     << escape(std::string(requestedUserRaw))
                     << " does not match authenticated identity "
                     << escape(std::string(authenticatedIdentityRaw));
            sasl_seterror(conn, 0, errorMsg.str().c_str());
            return SASL_BADAUTH;
        }
        return SASL_OK;
    } catch (...) {
        StringBuilder sb;
        sb << "Unexpected exception in saslServerConnAuthorize: " << exceptionToStatus().reason();
        sasl_seterror(conn, 0, sb.str().c_str());
        return SASL_FAIL;
    }
}

int saslAlwaysFailCallback() throw() {
    return SASL_FAIL;
}

/**
 * Type of pointer used to store SASL callback functions.
 */
typedef int (*SaslCallbackFn)();

/// This value chosen because it is unused, and unlikely to be used by the SASL library.
const int mongoSessionCallbackId = 0xF00F;

}  // namespace


template <typename Policy>
CyrusSaslMechShim<Policy>::CyrusSaslMechShim(std::string authenticationDatabase)
    : MakeServerMechanism<Policy>(std::move(authenticationDatabase)) {
    const sasl_callback_t callbackTemplate[maxCallbacks] = {
        {SASL_CB_GETOPT,
         SaslCallbackFn(saslServerConnGetOpt),
         static_cast<ServerMechanismBase*>(this)},
        {SASL_CB_PROXY_POLICY,
         SaslCallbackFn(saslServerConnAuthorize),
         static_cast<ServerMechanismBase*>(this)},
        {mongoSessionCallbackId, saslAlwaysFailCallback, static_cast<ServerMechanismBase*>(this)},
        {SASL_CB_LIST_END}};
    static_assert(std::extent<decltype(callbackTemplate)>::value == maxCallbacks,
                  "Insufficient space for copy");
    std::copy(callbackTemplate, callbackTemplate + maxCallbacks, _callbacks);

    int result = sasl_server_new(saslGlobalParams.serviceName.c_str(),  // service
                                 saslGlobalParams.hostName.c_str(),     // serviceFQDN
                                 NULL,                                  // user_realm
                                 NULL,                                  // iplocalport
                                 NULL,                                  // ipremoteport
                                 _callbacks,                            // callbacks
                                 0,                                     // flags
                                 &_saslConnection);                     // pconn
    if (SASL_OK != result) {
        uassertStatusOK(Status(ErrorCodes::UnknownError,
                               mongoutils::str::stream() << sasl_errstring(result, NULL, NULL)));
    }
}

template <typename Policy>
StringData CyrusSaslMechShim<Policy>::getPrincipalName() const {
    const void* principalId;

    int result = sasl_getprop(_saslConnection, SASL_USERNAME, &principalId);
    if (SASL_NOTDONE == result) {
        LOG(1) << "Was not able to acquire authorization username from Cyrus SASL. "
               << "Falling back to authentication name.";
        result = sasl_getprop(_saslConnection, SASL_AUTHUSER, &principalId);
    }

    // If either case was successful, we can return the Id that was found
    if (SASL_OK != result) {
        error() << "Was not able to acquire principal id from Cyrus SASL: " << result;
        return "";
    }

    return static_cast<const char*>(principalId);
}

template <typename Policy>
StatusWith<std::tuple<bool, std::string>> CyrusSaslMechShim<Policy>::stepImpl(
    OperationContext* opCtx, StringData input) {
    const char* output;
    unsigned outputLen;
    int result;

    if (0 == _saslStep) {
        // Cyrus SASL uses "SCRAM" as the internal mechanism name
        std::string mechName = Policy::getName() == "SCRAM-SHA-1" ? std::string("SCRAM")
                                                                  : Policy::getName().toString();
        result = sasl_server_start(
            _saslConnection, mechName.c_str(), input.rawData(), input.size(), &output, &outputLen);
    } else {
        result =
            sasl_server_step(_saslConnection, input.rawData(), input.size(), &output, &outputLen);
    }

    switch (result) {
        case SASL_OK: {
            ++_saslStep;
            return std::make_tuple(true, std::string());
        }
        case SASL_CONTINUE:
            ++_saslStep;
            return std::make_tuple(false, std::string(output, outputLen));
        case SASL_NOMECH:
            return Status(ErrorCodes::BadValue, sasl_errdetail(_saslConnection));
        case SASL_BADAUTH:
            return Status(ErrorCodes::AuthenticationFailed, sasl_errdetail(_saslConnection));
        default:
            return Status(ErrorCodes::ProtocolError, sasl_errdetail(_saslConnection));
    }
}

bool CyrusGSSAPIServerMechanism::isAuthorizedToActAs(StringData requestedUser,
                                                     StringData authenticatedUser) {
    std::string canonicalAuthenticatedUser;
    if (!gssapi::canonicalizeUserName(authenticatedUser, &canonicalAuthenticatedUser).isOK()) {
        return false;
    }

    return requestedUser == canonicalAuthenticatedUser;
}


/**
 * Implementation of sasl_log_t for handling log messages generated by the SASL library.
 */
int saslServerGlobalLog(void* context, int level, const char* message) throw() {
    switch (level) {
        case SASL_LOG_NONE:
            break;
        case SASL_LOG_ERR:
        case SASL_LOG_FAIL:
            error() << message;
            break;
        case SASL_LOG_WARN:
            warning() << message;
            break;
        case SASL_LOG_NOTE:
            log() << message;
            break;
        case SASL_LOG_DEBUG:
            LOG(1) << message;
            break;
        case SASL_LOG_TRACE:
            LOG(3) << message;
            break;
        case SASL_LOG_PASS:
            // Don't log trace data that includes passwords.
            break;
        default:
            error() << "Unexpected sasl log level " << level;
            break;
    }
    return SASL_OK;
}

// This group is used to ensure that all the plugins are registered before we attempt
// the smoke test in SaslCommands.
MONGO_INITIALIZER_GROUP(CyrusSaslAllPluginsRegistered, MONGO_NO_PREREQUISITES, MONGO_NO_DEPENDENTS);

template <typename Factory>
void smokeAndRegister(SASLServerMechanismRegistry* registry) {
    bool registered = registry->registerFactory<Factory>();
    if (registered) {
        Factory factory;
        if (factory.mechanismName() == "GSSAPI") {
            fassert(50743,
                    gssapi::tryAcquireServerCredential(static_cast<std::string>(
                        mongoutils::str::stream() << saslGlobalParams.serviceName << "@"
                                                  << saslGlobalParams.hostName)));
        }
        auto mech = factory.create("test");
    }
}

MONGO_INITIALIZER_WITH_PREREQUISITES(CyrusSaslServerCore,
                                     ("CyrusSaslAllocatorsAndMutexes", "CyrusSaslClientContext"))
(InitializerContext* context) {
    static const sasl_callback_t saslServerGlobalCallbacks[] = {
        {SASL_CB_LOG, SaslCallbackFn(saslServerGlobalLog), NULL}, {SASL_CB_LIST_END}};

    int result = sasl_server_init(saslServerGlobalCallbacks, "mongodb");
    if (result != SASL_OK) {
        return Status(ErrorCodes::UnknownError,
                      mongoutils::str::stream() << "Could not initialize sasl server components ("
                                                << sasl_errstring(result, NULL, NULL)
                                                << ")");
    }

    return Status::OK();
};

MONGO_INITIALIZER_WITH_PREREQUISITES(CyrusSaslRegisterMechanisms,
                                     ("CyrusSaslServerCore",
                                      "CyrusSaslAllPluginsRegistered",
                                      "CreateSASLServerMechanismRegistry"))
(InitializerContext* context) {
    auto& registry = SASLServerMechanismRegistry::get(getGlobalServiceContext());

    if (!saslGlobalParams.authdPath.empty()) {
        smokeAndRegister<CyrusPlainServerFactory>(&registry);
    }

    smokeAndRegister<CyrusGSSAPIServerFactory>(&registry);

    return Status::OK();
}

}  // namespace mongo
