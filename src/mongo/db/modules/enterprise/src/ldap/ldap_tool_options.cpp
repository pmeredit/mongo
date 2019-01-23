/*
 *    Copyright (C) 2016 MongoDB Inc.
 */

#include "ldap_tool_options.h"

#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

namespace mongo {
LDAPToolOptions* globalLDAPToolOptions;

namespace moe = mongo::optionenvironment;
namespace {

bool shouldUseStrict() {
    // Indicates that unknown config options are allowed. This is necessary to parse generic mongod
    // config files successfully without implementing support for all options.
    return false;
}

MONGO_INITIALIZER_GENERAL(LDAPToolUseStrict,
                          ("OptionsParseUseStrict"),
                          ("BeginStartupOptionParsing"))
(InitializerContext* context) {
    moe::OptionsParser::useStrict = shouldUseStrict;
    return Status::OK();
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongoLDAPToolOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (!params.count("user")) {
        return {ErrorCodes::BadValue, "Missing required option: \"--user\""};
    }

    if (params.count("help")) {
        std::cout << "Usage: mongoldap [options] " << std::endl
                  << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
                  << std::endl
                  << moe::startupOptions.helpString() << std::flush;
        quickExit(EXIT_SUCCESS);
    }

    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(MongoLDAPToolOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("password")) {
        globalLDAPToolOptions->password =
            SecureString(params["password"].as<std::string>().c_str());
    }

    return Status::OK();
}

MONGO_INITIALIZER_GENERAL(MongoLDAPToolOptions, ("SecureAllocator"), ("MongoLDAPToolOptions_Store"))
(InitializerContext* context) {
    globalLDAPToolOptions = new LDAPToolOptions();
    return Status::OK();
}

}  // namespace
}  // namespace mongo
