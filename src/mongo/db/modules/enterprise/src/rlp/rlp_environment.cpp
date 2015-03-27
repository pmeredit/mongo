/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "rlp_environment.h"

#include <string>

#include "mongo/logger/log_severity.h"
#include "mongo/platform/shared_library.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "rlp_context.h"

namespace mongo {
namespace fts {
namespace {

    /**
    * The application registers this function to receive diagnostic log
    * entries. RLPEnvironment::SetLogLevel determines which message
    * channels (error, warning, info) are posted to the callback.
    */
    void log_callback(void* info_p, int channel, const char* message) {
        try {
            logger::LogSeverity loglevel(logger::LogSeverity::Info());

            switch (channel) {
                case 0:
                    loglevel = logger::LogSeverity::Warning();
                    break;
                case 1:
                    loglevel = logger::LogSeverity::Error();
                    break;
                case 2:
                    loglevel = logger::LogSeverity::Info();
                    break;
            }

            LogstreamBuilder(logger::globalLogDomain(),
                             getThreadName(),
                             loglevel,
                             ::MongoLogDefaultComponent_component)
                << "RLP: " << message;
        } catch (...) {
            error() << exceptionToStatus();
            std::terminate();
        }
    }
}  // namespace

// Cache function calls
// These names are stable for RLP Mac gcc40 and Linux gcc41 builds
//
#define RLP_C_FUNC_DLSYM(ret, name, args)                                                          \
    {                                                                                              \
        auto s = library->getFunctionAs<ret(*) args>(#name);                                       \
        if (!s.isOK()) {                                                                           \
            return {s.getStatus()};                                                                \
        }                                                                                          \
                                                                                                   \
        if (s.getValue() == nullptr) {                                                             \
            return {ErrorCodes::InternalError, str::stream() << "Symbol " << #name << " is null"}; \
        }                                                                                          \
        env->_##name = s.getValue();                                                               \
    }

    StatusWith<std::unique_ptr<RlpEnvironment>> RlpEnvironment::create(std::string btRoot,
                                                                       bool verbose,
                                                                       SharedLibrary* library) {
        auto env = stdx::make_unique<RlpEnvironment>();

        RLP_C_FUNC(RLP_C_FUNC_DLSYM)

        env->_factory = stdx::make_unique<ContextFactory>(env.get());

        if (!env->BT_RLP_Library_VersionIsCompatible(BT_RLP_LIBRARY_INTERFACE_VERSION)) {
            return StatusWith<std::unique_ptr<RlpEnvironment>>(
                ErrorCodes::InternalError,
                str::stream() << "RLP library mismatch: have version '"
                              << env->BT_RLP_Library_VersionString() << "', expected version '"
                              << BT_RLP_LIBRARY_VERSION_STRING << "'");
        }

        LOG(0) << "Loaded Rosette Linguistics Platform Library: "
               << env->BT_RLP_Library_VersionString();

        env->BT_RLP_Environment_SetBTRootDirectory(btRoot.c_str());

        // Set up logging if not already set with environment variable.
        // Log level is some combination of "warning,error,info"
        // or "all" or "none".
        //
        env->BT_RLP_Environment_SetLogCallbackFunction(nullptr, log_callback);

        const char* logLevel = verbose ? "error,warning,info" : "error";
        env->BT_RLP_Environment_SetLogLevel(logLevel);

        env->_rlpenv = env->BT_RLP_Environment_Create();
        if (!env->_rlpenv) {
            return {ErrorCodes::InternalError,
                    str::stream() << "Failed to intialize RLP_Environment"};
        }

        // Initialize the empty environment with the global environment
        // configuration file.
        //
        boost::filesystem::path environmentPath(btRoot);
        environmentPath /= "/rlp/etc/rlp-environment.xml";

        BT_Result rc = env->BT_RLP_Environment_InitializeFromFile(
            env->getEnvironment(), environmentPath.generic_string().c_str());

        if (rc != BT_OK) {
            return {ErrorCodes::InternalError,
                    str::stream() << "Unable to initialize the RLP environment using: "
                                  << environmentPath.generic_string()};
        }

        return {std::move(env)};
    }

}  // namespace fts
}  // namespace mongo
