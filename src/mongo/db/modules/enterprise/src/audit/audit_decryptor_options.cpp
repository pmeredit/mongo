/*
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit_decryptor_options.h"

#include <iostream>

#include "mongo/base/status.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_component_settings.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/version.h"

namespace moe = mongo::optionenvironment;

namespace mongo {
namespace audit {

AuditDecryptorOptions globalAuditDecryptorOptions;

namespace {

MONGO_STARTUP_OPTIONS_VALIDATE(MongoAuditDecryptorOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("help")) {
        std::cout << "Usage: mongoauditdecrypt [options] --inputPath <path> --outputPath <path> "
                  << std::endl
                  << "Version " << mongo::VersionInfoInterface::instance().version() << std::endl
                  << std::endl
                  << moe::startupOptions.helpString() << std::flush;
        quickExit(ExitCode::clean);
    }
}

Status storeAuditDecryptorOptions(const moe::Environment& params) {
    if (!params.count("inputPath")) {
        return Status(ErrorCodes::BadValue, "Missing required option: \"--inputPath\"");
    }
    globalAuditDecryptorOptions.inputPath = params["inputPath"].as<std::string>();

    if (!params.count("outputPath")) {
        return Status(ErrorCodes::BadValue, "Missing required option: \"--outputPath\"");
    }
    globalAuditDecryptorOptions.outputPath = params["outputPath"].as<std::string>();

    if (params.count("noConfirm")) {
        globalAuditDecryptorOptions.confirmDecryption = false;
    }

    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(MongoAuditDecryptorOptions)(InitializerContext* context) {
    Status ret = storeAuditDecryptorOptions(moe::startupOptionsParsed);
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(ExitCode::badOptions);
    }
}

}  // namespace
}  // namespace audit
}  // namespace mongo
