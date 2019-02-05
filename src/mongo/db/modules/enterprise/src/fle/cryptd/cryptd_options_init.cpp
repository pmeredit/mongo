/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#include "cryptd_options.h"

#include <iostream>

#include "mongo/base/status.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"

namespace mongo {
namespace {
MONGO_GENERAL_STARTUP_OPTIONS_REGISTER(MongoCryptDOptions)(InitializerContext* context) {
    return addMongoCryptDOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongoCryptDOptions)(InitializerContext* context) {
    if (!handlePreValidationMongoCryptDOptions(moe::startupOptionsParsed)) {
        quickExit(EXIT_SUCCESS);
    }

    Status ret = validateMongoCryptDOptions(moe::startupOptionsParsed);
    if (!ret.isOK()) {
        return ret;
    }

    ret = canonicalizeMongoCryptDOptions(&moe::startupOptionsParsed);
    if (!ret.isOK()) {
        return ret;
    }

    ret = moe::startupOptionsParsed.validate();
    if (!ret.isOK()) {
        return ret;
    }

    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(MongoCryptDOptions)(InitializerContext* context) {
    Status ret = storeMongoCryptDOptions(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(EXIT_BADOPTIONS);
    }

    return Status::OK();
}
}  // namespace
}  // namespace mongo
