/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "cryptd_options.h"

#include <iostream>

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_options_server_helpers.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/version.h"

namespace mongo {

MongoCryptDGlobalParams mongoCryptDGlobalParams;

Status addMongoCryptDOptions(moe::OptionSection* options) {
    moe::OptionSection general_options("General options");

    Status ret = addGeneralServerOptions(&general_options);
    if (!ret.isOK()) {
        return ret;
    }

    ret = options->addSection(general_options);
    if (!ret.isOK()) {
        return ret;
    }

    return Status::OK();
}

void printMongoCryptDHelp(std::ostream* out) {
    *out << moe::startupOptions.helpString();
    *out << std::flush;
}

bool handlePreValidationMongoCryptDOptions(const moe::Environment& params) {
    if (params.count("help") && params["help"].as<bool>() == true) {
        printMongoCryptDHelp(&std::cout);
        return false;
    }

    if (params.count("version") && params["version"].as<bool>() == true) {
        setPlainConsoleLogger();
        auto&& vii = VersionInfoInterface::instance();
        log() << mongodVersion(vii);
        vii.logBuildInfo();
        return false;
    }

    return true;
}


Status validateMongoCryptDOptions(const moe::Environment& params) {
    return validateServerOptions(params);
}

Status canonicalizeMongoCryptDOptions(moe::Environment* params) {
    return canonicalizeServerOptions(params);
}

Status storeMongoCryptDOptions(const moe::Environment& params,
                               const std::vector<std::string>& args) {
    Status ret = storeServerOptions(params);
    if (!ret.isOK()) {
        return ret;
    }

#ifdef _WIN32
    if (!params.count("net.port")) {
        return {ErrorCodes::BadValue, "Missing required option: --port"};
    }

    mongoCryptDGlobalParams.port = params["net.port"].as<int>();
#endif

    mongoCryptDGlobalParams.idleShutdownTimeout =
        Seconds(params["processManagement.idleShutdownTimeoutSecs"].as<int>());

    // When the user passes processManagement.pidFilePath, it is handled in other
    // option parsing code. We just set a default here if it is not set.
    if (!params.count("processManagement.pidFilePath")) {
        serverGlobalParams.pidFile = "mongocryptd.pid";
    }

    return Status::OK();
}

}  // namespace mongo
