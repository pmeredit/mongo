/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "decrypt_tool_options.h"

#include <iostream>

#include "mongo/base/status.h"
#include "mongo/logger/logger.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

namespace mongo {

DecryptToolOptions globalDecryptToolOptions;

Status addDecryptToolOptions(moe::OptionSection* options) {
    options->addOptionChaining("help", "help", moe::Switch, "produce help message");

    options->addOptionChaining(
        "inputPath", "inputPath", moe::String, "path to encrypted administrative file");

    options->addOptionChaining(
        "outputPath", "outputPath", moe::String, "path to where decrypted file will be placed");

    options
        ->addOptionChaining(
            "cipherMode", "cipherMode", moe::String, "name of the cipher used to encrypt the data")
        .format("(:?AES256-CBC)|(:?AES256-GCM)", "'AES256-CBC' or 'AES256-GCM'")
        .setDefault(moe::Value(std::string("AES256-CBC")));

    options->addOptionChaining(
        "keyFile", "keyFile", moe::String, "path to base64 encoded AES key on filesystem");

    addKMIPOptions(options);

    options->addOptionChaining("verbose", "verbose", moe::Switch, "increase verbosity");

    options->addOptionChaining(
        "noConfirm", "noConfirm", moe::Switch, "do not ask for confirmation before decrypting");

    return Status::OK();
}

void printDecryptToolHelp(std::ostream& out) {
    out << "Usage: mongodecrypt [options] --inputPath <path> --outputPath <path> " << std::endl
        << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
        << std::endl
        << moe::startupOptions.helpString() << std::flush;
}

bool handlePreValidationDecryptToolOptions(const moe::Environment& params) {
    if (params.count("help")) {
        printDecryptToolHelp(std::cout);
        return false;
    }
    return true;
}

Status storeDecryptToolOptions(const moe::Environment& params,
                               const std::vector<std::string>& args) {
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

MONGO_GENERAL_STARTUP_OPTIONS_REGISTER(MongoDecryptToolOptions)(InitializerContext* context) {
    return addDecryptToolOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongoDecryptToolOptions)(InitializerContext* context) {
    if (!handlePreValidationDecryptToolOptions(moe::startupOptionsParsed)) {
        quickExit(EXIT_SUCCESS);
    }
    Status ret = moe::startupOptionsParsed.validate();
    if (!ret.isOK()) {
        return ret;
    }
    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(MongoDecryptToolOptions)(InitializerContext* context) {
    Status ret = storeDecryptToolOptions(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

}  // namespace mongo
