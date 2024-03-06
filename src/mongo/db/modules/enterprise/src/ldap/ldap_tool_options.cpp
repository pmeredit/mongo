/*
 *    Copyright (C) 2016 MongoDB Inc.
 */

#include "ldap_tool_options.h"

#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

namespace mongo {

namespace {
void printLDAPToolHelp(std::ostream& out) {
    out << "Usage: mongoldap [options] " << std::endl
        << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
        << std::endl
        << moe::startupOptions.helpString() << std::flush;
}

bool handlePreValidationLDAPToolOptions(const moe::Environment& params) {
    if (params.count("help")) {
        printLDAPToolHelp(std::cout);
        return false;
    }
    return true;
}
}  // namespace

LDAPToolOptions globalLDAPToolOptions;

Status addLDAPToolOptions(moe::OptionSection* options) {
    options->addOptionChaining("help", "help", moe::Switch, "produce help message");
    options
        ->addOptionChaining(
            "config", "config,f", moe::String, "configuration file specifying additional options")
        .setSources(moe::SourceAllLegacy);
    options->addOptionChaining("user", "user", moe::String, "user to acquire roles for")
        .setSources(moe::SourceAllLegacy);
    options->addOptionChaining("color", "color", moe::Bool, "Enable colored output")
        .setSources(moe::SourceAllLegacy);

    return Status::OK();
}

Status storeLDAPToolOptions(const moe::Environment& params, const std::vector<std::string>& args) {
    if (!params.count("user")) {
        return Status(ErrorCodes::BadValue, "Missing required option: \"--user\"");
    }
    globalLDAPToolOptions.user = params["user"].as<std::string>();

    if (params.count("color")) {
        globalLDAPToolOptions.color = params["color"].as<bool>();
    } else {
#ifdef _WIN32
        globalLDAPToolOptions.color = false;
#else
        globalLDAPToolOptions.color = true;
#endif
    }
    return Status::OK();
}

MONGO_GENERAL_STARTUP_OPTIONS_REGISTER(MongoLDAPToolOptions)(InitializerContext* context) {
    return addLDAPToolOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongoLDAPToolOptions)(InitializerContext* context) {
    if (!handlePreValidationLDAPToolOptions(moe::startupOptionsParsed)) {
        quickExit(EXIT_SUCCESS);
    }
    return moe::startupOptionsParsed.validate();
}

MONGO_STARTUP_OPTIONS_STORE(MongoLDAPToolOptions)(InitializerContext* context) {
    Status ret = storeLDAPToolOptions(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

}  // namespace mongo
