/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"

#include "ldap_options.h"
#include "ldap_runner_impl.h"

namespace mongo {

/* Make a LDAPRunnerImpl pointer a decoration on the global ServiceContext */
MONGO_INITIALIZER_WITH_PREREQUISITES(SetLDAPRunnerImpl, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverURIs);
    auto runner =
        stdx::make_unique<LDAPRunnerImpl>(std::move(bindOptions), std::move(connectionOptions));
    LDAPRunner::set(getGlobalServiceContext(), std::move(runner));

    return Status::OK();
}
}  // mongo
