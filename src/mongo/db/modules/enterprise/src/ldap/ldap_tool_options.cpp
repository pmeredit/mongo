/*
 *    Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "ldap_tool_options.h"

#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

#ifdef _WIN32
#include <cstdio>
#include <io.h>
#endif

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
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongoLDAPToolOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("help")) {
        std::cout << "Usage: mongoldap [options] " << std::endl
                  << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
                  << std::endl
                  << moe::startupOptions.helpString() << std::flush;
        quickExit(ExitCode::clean);
    }

    if (!params.count("user")) {
        uasserted(ErrorCodes::BadValue, "Missing required option: \"--user\"");
    }
}

MONGO_STARTUP_OPTIONS_STORE(MongoLDAPToolOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("password")) {
        globalLDAPToolOptions->password =
            SecureString(params["password"].as<std::string>().c_str());
    }

#ifdef _WIN32
    int isTty = _isatty(_fileno(stdout));
#else
    int isTty = isatty(STDOUT_FILENO);
#endif
    if (!isTty && !params.count("color")) {
        globalLDAPToolOptions->color = false;
    }
}

MONGO_INITIALIZER_GENERAL(MongoLDAPToolOptions, ("SecureAllocator"), ("BeginStartupOptionStorage"))
(InitializerContext* context) {
    globalLDAPToolOptions = new LDAPToolOptions();
}

}  // namespace
}  // namespace mongo
