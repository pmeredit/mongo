/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/cluster_auth_mode_option_gen.h"
#include "mongo/db/keyfile_option_gen.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace {

MONGO_GENERAL_STARTUP_OPTIONS_REGISTER(MongotMockOptions)(InitializerContext* context) {
    uassertStatusOK(addKeyfileServerOption(&optionenvironment::startupOptions));
    uassertStatusOK(addClusterAuthModeServerOption(&optionenvironment::startupOptions));
}

}  // namespace
}  // namespace mongo
