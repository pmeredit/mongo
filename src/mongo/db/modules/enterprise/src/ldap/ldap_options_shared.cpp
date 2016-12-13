/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "ldap_options.h"

#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace {
namespace moe = mongo::optionenvironment;

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(LDAPOptions)(InitializerContext* context) {
    return addSharedLDAPOptions(&moe::startupOptions);
}

}  // namespace
}  // mongo
