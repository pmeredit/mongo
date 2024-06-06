/**
 *  Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "kerberos_tool_options.h"

#include "mongo/base/init.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

namespace mongo {
KerberosToolOptions* globalKerberosToolOptions;

namespace moe = mongo::optionenvironment;
namespace {

bool shouldUseStrict() {
    // Indicates that unknown config options are allowed. This is necessary to parse generic mongod
    // config files successfully without implementing support for all options.
    return false;
}

MONGO_INITIALIZER_GENERAL(KerberosToolUseStrict,
                          ("OptionsParseUseStrict"),
                          ("BeginStartupOptionParsing"))
(InitializerContext*) {
    moe::OptionsParser::useStrict = shouldUseStrict;
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongoKerberosToolOptions)(InitializerContext*) {
    auto& params = moe::startupOptionsParsed;

    if (params.count("help")) {
        std::cout << "Usage: mongokerberos [options] <--client|--server> <additional_options>"
                  << std::endl
                  << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
                  << std::endl
                  << moe::startupOptions.helpString() << std::flush;
        quickExit(ExitCode::clean);
    }
    uassertStatusOK(params.validate());
    // check for required --client|--server parameter
    if (!params.count("client") && !params.count("server")) {
        uasserted(ErrorCodes::BadValue, "Missing required option: \"--client|--server\"");
    }
}

MONGO_STARTUP_OPTIONS_STORE(MongoKerberosToolOptions)(InitializerContext*) {
    const auto& params = moe::startupOptionsParsed;

    globalKerberosToolOptions->connectionType = params.count("server")
        ? KerberosToolOptions::ConnectionType::kServer
        : KerberosToolOptions::ConnectionType::kClient;

    if (globalKerberosToolOptions->host.empty()) {
        globalKerberosToolOptions->host = getHostNameCached();
    }
}

MONGO_INITIALIZER_GENERAL(MongoKerberosToolOptions,
                          ("SecureAllocator"),
                          ("BeginStartupOptionStorage"))
(InitializerContext*) {
    globalKerberosToolOptions = new KerberosToolOptions();
}

}  // namespace

std::string KerberosToolOptions::getGSSAPIHost() const {
    if (!gssapiHostName.empty()) {
        return gssapiHostName;
    } else {
        return host;
    }
}

std::string KerberosToolOptions::getHostbasedService() const {
    return str::stream() << gssapiServiceName << "@" << getGSSAPIHost();
}

}  // namespace mongo
