/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "decrypt_tool_options.h"

#include <iostream>

#include "mongo/base/status.h"
#include "mongo/logger/logger.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

namespace moe = mongo::optionenvironment;

namespace mongo {

DecryptToolOptions globalDecryptToolOptions;

namespace {

MONGO_STARTUP_OPTIONS_VALIDATE(MongoDecryptToolOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("help")) {
        std::cout << "Usage: mongodecrypt [options] --inputPath <path> --outputPath <path> "
                  << std::endl
                  << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
                  << std::endl
                  << moe::startupOptions.helpString() << std::flush;
        quickExit(EXIT_SUCCESS);
    }

    return Status::OK();
}

Status storeDecryptToolOptions(const moe::Environment& params) {
    if (!params.count("inputPath")) {
        return Status(ErrorCodes::BadValue, "Missing required option: \"--inputPath\"");
    }
    globalDecryptToolOptions.inputPath = params["inputPath"].as<std::string>();

    if (!params.count("outputPath")) {
        return Status(ErrorCodes::BadValue, "Missing required option: \"--outputPath\"");
    }
    globalDecryptToolOptions.outputPath = params["outputPath"].as<std::string>();

    if (params.count("cipherMode")) {
        globalDecryptToolOptions.mode =
            crypto::getCipherModeFromString(params["cipherMode"].as<std::string>());
    }

    if (params.count("keyFile")) {
        globalDecryptToolOptions.keyFile = params["keyFile"].as<std::string>();
    }

    auto swKmipParams = parseKMIPOptions(params);
    if (!swKmipParams.isOK()) {
        return swKmipParams.getStatus();
    }
    globalDecryptToolOptions.kmipParams = std::move(swKmipParams.getValue());

    if (params.count("verbose")) {
        logger::globalLogDomain()->setMinimumLoggedSeverity(logger::LogSeverity::Debug(1));
    }

    if (params.count("noConfirm")) {
        globalDecryptToolOptions.confirmDecryption = false;
    }

    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(MongoDecryptToolOptions)(InitializerContext* context) {
    Status ret = storeDecryptToolOptions(moe::startupOptionsParsed);
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

}  // namespace
}  // namespace mongo
