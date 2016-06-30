/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"

#include "ldap_manager_impl.h"
#include "ldap_options.h"

namespace mongo {

/* Make a LDAPRunnerImpl pointer a decoration on the global ServiceContext */
MONGO_INITIALIZER_WITH_PREREQUISITES(SetLDAPManagerImpl, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverURIs);

    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        globalLDAPParams->userAcquisitionQueryTemplate);
    if (!swQueryParameters.isOK()) {
        return swQueryParameters.getStatus();
    }

    auto swMapper =
        InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping);
    massertStatusOK(swMapper.getStatus());

    auto manager = stdx::make_unique<LDAPManagerImpl>(
        stdx::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions),
        std::move(swQueryParameters.getValue()),
        std::move(swMapper.getValue()));
    LDAPManager::set(getGlobalServiceContext(), std::move(manager));

    return Status::OK();
}
}  // mongo
