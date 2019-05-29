/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/initialize_server_security_state.h"
#include "mongo/db/service_context.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

ServiceContext::ConstructorActionRegisterer registerInitializeGlobalSecurityState{
    "InitializeGlobalSecurityState",
    {"CreateAuthorizationManager"},
    [](ServiceContext* serviceContext) {
        fassert(31009, initializeServerSecurityGlobalState(serviceContext));
    }};

}  // namespace
}  // namespace mongo
