/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"

#include "ldap_manager_impl.h"
#include "ldap_options.h"
#include "ldap_query.h"

namespace mongo {

/* Make a LDAPRunnerImpl pointer a decoration on the global ServiceContext */
MONGO_INITIALIZER_WITH_PREREQUISITES(SetLDAPManagerImpl, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    if (globalLDAPParams->serverHosts.empty()) {
        return Status::OK();
    }

    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverHosts,
                                            globalLDAPParams->transportSecurity);

    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        globalLDAPParams->userAcquisitionQueryTemplate);
    if (!swQueryParameters.isOK()) {
        return swQueryParameters.getStatus();
    }

    auto swMapper =
        InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping);
    massertStatusOK(swMapper.getStatus());

    // Perform smoke test of the connection parameters.
    // TODO: Possibly store the root DSE for future reference.
    auto runner = stdx::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);
    StatusWith<LDAPEntityCollection> swRes =
        runner->runQuery(LDAPQuery::instantiateQuery(LDAPQueryConfig()).getValue());

    if (!swRes.isOK()) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "Can't connect to the specified LDAP servers, error: "
                                    << swRes.getStatus().reason());
    }

    auto manager = stdx::make_unique<LDAPManagerImpl>(
        std::move(runner), std::move(swQueryParameters.getValue()), std::move(swMapper.getValue()));
    LDAPManager::set(getGlobalServiceContext(), std::move(manager));

    return Status::OK();
}
}  // mongo
