/*
 * Copyright (C) 2014-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "cyrus_sasl_authentication_session.h"

#include <sasl/sasl.h>

#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/signal_handlers_synchronous.h"
#include "mongo/util/str.h"
#include "mongo_gssapi.h"

#include "../ldap/ldap_manager.h"
#include "../ldap/ldap_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace {

void setSaslError(sasl_conn_t* conn, const std::string& msg) {
    sasl_seterror(conn, 0, "%s", msg.c_str());
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
                         unsigned* outLen) throw() {
#ifdef _WIN32
    static constexpr StringData mongodbAuxpropMechanism = "MongoDBInternalAuxprop"_sd;
#endif
    static constexpr StringData mongodbCanonMechanism = "MongoDBInternalCanon"_sd;

    try {
        unsigned ignored;
        if (!outLen)
            outLen = &ignored;

        ServerMechanismBase* session = static_cast<ServerMechanismBase*>(context);
        if (!session || !optionRaw || !outResult)
            return SASL_BADPARAM;

        const StringData option = optionRaw;


#ifdef _WIN32
        if (option == "auxprop_plugin"_sd) {
            // The plugin exists so that sasl cannonicalization does not fail during GSSAPI
            *outResult = mongodbAuxpropMechanism.rawData();
            *outLen = static_cast<unsigned>(mongodbAuxpropMechanism.size());
            return SASL_OK;
        }
#endif

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
        LOGV2_ERROR(24029,
                    "Unexpected exception in saslServerConnGetOpt",
                    "status"_attr = exceptionToStatus());
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
                     << str::escape(std::string(requestedUserRaw))
                     << " does not match authenticated identity "
                     << str::escape(std::string(authenticatedIdentityRaw));
            setSaslError(conn, errorMsg.str());
            return SASL_BADAUTH;
        }
        return SASL_OK;
    } catch (...) {
        StringBuilder sb;
        sb << "Unexpected exception in saslServerConnAuthorize: " << exceptionToStatus().reason();
        setSaslError(conn, sb.str());
        return SASL_FAIL;
    }
}

int saslAlwaysFailCallback() throw() {
    return SASL_FAIL;
}

/**
 * Implements the Cyrus SASL default_verifyfile_cb interface registered in the
 * Cyrus SASL library to verify, and then accept or reject, the loading of
 * plugin libraries from the target directory.
 *
 * On Windows environments, disable loading of plugin files.
 */
int saslServerVerifyPluginFile(void*, const char*, sasl_verify_type_t type) {

    if (type != SASL_VRFY_PLUGIN) {
        return SASL_OK;
    }

#ifdef _WIN32
    return SASL_CONTINUE;  // A non-SASL_OK response indicates to Cyrus SASL that it
                           // should not load a file. This effectively disables
                           // loading plugins from path on Windows.
#else
    return SASL_OK;
#endif
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
                                 nullptr,                               // user_realm
                                 nullptr,                               // iplocalport
                                 nullptr,                               // ipremoteport
                                 _callbacks,                            // callbacks
                                 0,                                     // flags
                                 &_saslConnection);                     // pconn
    if (SASL_OK != result) {
        uassertStatusOK(Status(ErrorCodes::UnknownError,
                               str::stream() << sasl_errstring(result, nullptr, nullptr)));
    }
}

template <typename Policy>
StringData CyrusSaslMechShim<Policy>::getPrincipalName() const {
    const void* principalId;

    int result = sasl_getprop(_saslConnection, SASL_USERNAME, &principalId);
    if (SASL_NOTDONE == result) {
        LOGV2_DEBUG(24024,
                    1,
                    "Was not able to acquire authorization username from Cyrus SASL. Falling back "
                    "to authentication name.");
        result = sasl_getprop(_saslConnection, SASL_AUTHUSER, &principalId);
    }

    // If either case was successful, we can return the Id that was found
    if (SASL_OK != result) {
        LOGV2_ERROR(
            24030, "Was not able to acquire principal id from Cyrus SASL", "result"_attr = result);
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

// Explicitly instantiate these types to match up with the header.
template struct CyrusSaslMechShim<PLAINPolicy>;
template struct CyrusSaslMechShim<GSSAPIPolicy>;

bool CyrusGSSAPIServerMechanism::isAuthorizedToActAs(StringData requestedUser,
                                                     StringData authenticatedUser) {
    auto statusOrValue = gssapi::canonicalizeUserName(authenticatedUser);
    if (!statusOrValue.isOK()) {
        return false;
    }

    return requestedUser == statusOrValue.getValue();
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
            LOGV2_ERROR(24031, "{message}", "message"_attr = message);
            break;
        case SASL_LOG_WARN:
            LOGV2_WARNING(24028, "{message}", "message"_attr = message);
            break;
        case SASL_LOG_NOTE:
            LOGV2(24025, "{message}", "message"_attr = message);
            break;
        case SASL_LOG_DEBUG:
            LOGV2_DEBUG(24026, 1, "{message}", "message"_attr = message);
            break;
        case SASL_LOG_TRACE:
            LOGV2_DEBUG(24027, 3, "{message}", "message"_attr = message);
            break;
        case SASL_LOG_PASS:
            // Don't log trace data that includes passwords.
            break;
        default:
            LOGV2_ERROR(24032, "Unexpected sasl log level", "level"_attr = level);
            break;
    }
    return SASL_OK;
}

namespace {

// This group is used to ensure that all the plugins are registered before we attempt
// the smoke test in SaslCommands.
MONGO_INITIALIZER_GROUP(CyrusSaslAllPluginsRegistered, (), ());

/*
 * Allocator functions to be used by the SASL library, if the client
 * doesn't initialize the library for us.
 */

// Version 2.1.26 is the first version to use size_t in the allocator signatures
#if (SASL_VERSION_FULL >= ((2 << 16) | (1 << 8) | 26))
typedef size_t SaslAllocSize;
#else
typedef unsigned long SaslAllocSize;
#endif

typedef int (*SaslCallbackFn)();

void* saslOurMalloc(SaslAllocSize sz) {
    return mongoMalloc(sz);
}

void* saslOurCalloc(SaslAllocSize count, SaslAllocSize size) {
    void* ptr = calloc(count, size);
    if (!ptr) {
        reportOutOfMemoryErrorAndExit();
    }
    return ptr;
}

void* saslOurRealloc(void* ptr, SaslAllocSize sz) {
    return mongoRealloc(ptr, sz);
}

/*
 * Mutex functions to be used by the SASL library, if the client doesn't initialize the library
 * for us.
 */

void* saslMutexAlloc(void) {
    return new stdx::mutex;
}

int saslMutexLock(void* mutex) {
    static_cast<stdx::mutex*>(mutex)->lock();
    return SASL_OK;
}

int saslMutexUnlock(void* mutex) {
    static_cast<stdx::mutex*>(mutex)->unlock();
    return SASL_OK;
}

void saslMutexFree(void* mutex) {
    delete static_cast<stdx::mutex*>(mutex);
}

/**
 * Configures the SASL library to use allocator and mutex functions we specify,
 * unless the client application has previously initialized the SASL library.
 */
MONGO_INITIALIZER(CyrusSaslAllocatorsAndMutexesServer)(InitializerContext*) {
    sasl_set_alloc(saslOurMalloc, saslOurCalloc, saslOurRealloc, free);

    sasl_set_mutex(saslMutexAlloc, saslMutexLock, saslMutexUnlock, saslMutexFree);
}

MONGO_INITIALIZER_WITH_PREREQUISITES(CyrusSaslServerCore, ("CyrusSaslAllocatorsAndMutexesServer"))
(InitializerContext* context) {
    static const sasl_callback_t saslServerGlobalCallbacks[] = {
        {SASL_CB_LOG, SaslCallbackFn(saslServerGlobalLog), nullptr},
        {SASL_CB_VERIFYFILE, SaslCallbackFn(saslServerVerifyPluginFile), nullptr /*context*/},
        {SASL_CB_LIST_END}};

    int result = sasl_server_init(saslServerGlobalCallbacks, "mongodb");
    if (result != SASL_OK) {
        uasserted(ErrorCodes::UnknownError,
                  str::stream() << "Could not initialize sasl server components ("
                                << sasl_errstring(result, nullptr, nullptr) << ")");
    }
};

Service::ConstructorActionRegisterer cyrusSaslServerMechanismRegisterMechanisms{
    "CyrusSaslRegisterMechanisms,",
    {"CyrusSaslServerCore", "CyrusSaslAllPluginsRegistered", "CreateSASLServerMechanismRegistry"},
    {"ValidateSASLServerMechanismRegistry"},
    [](Service* service) {
        auto& registry = SASLServerMechanismRegistry::get(service);
        if (registry.registerFactory<CyrusGSSAPIServerFactory>()) {
            CyrusGSSAPIServerFactory factory;
            fassertNoTrace(50743,
                           gssapi::tryAcquireServerCredential(static_cast<std::string>(
                               str::stream() << saslGlobalParams.serviceName << "@"
                                             << saslGlobalParams.hostName)));
            auto mech = factory.create("test");
        }

        // The PLAIN variant of Cyrus is registered in ldap_sasl_authentication_session.cpp
        // if the LDAP backed PLAIN mechanism is disabled.
        CyrusPlainServerFactory cyrusPlainServerFactory;
        cyrusPlainServerFactory.create("test");
    }};
}  // namespace
}  // namespace mongo
