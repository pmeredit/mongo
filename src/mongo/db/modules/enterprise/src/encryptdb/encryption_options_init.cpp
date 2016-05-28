/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "encryption_options.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/server_options.h"

#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(EncryptionOptions)(InitializerContext* context) {
    return addEncryptionOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_STORE(EncryptionOptions)(InitializerContext* context) {
    return storeEncryptionOptions(moe::startupOptionsParsed);
}

}  // namespace mongo
