/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"

#include "ldap_manager_impl.h"
#include "ldap_options.h"
#include "ldap_query.h"

namespace mongo {
namespace {
/* Make a LDAPRunnerImpl pointer a decoration on service contexts */
ServiceContext::ConstructorActionRegisterer setLDAPManagerImpl{
    "SetLDAPManagerImpl", [](ServiceContext* service) {
        LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                    globalLDAPParams->bindPassword,
                                    globalLDAPParams->bindMethod,
                                    globalLDAPParams->bindSASLMechanisms,
                                    globalLDAPParams->useOSDefaults);
        LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                                globalLDAPParams->serverHosts,
                                                globalLDAPParams->transportSecurity);

        auto queryParameters = uassertStatusOK(LDAPQueryConfig::createLDAPQueryConfigWithUserName(
            globalLDAPParams->userAcquisitionQueryTemplate));
        auto mapper = uassertStatusOK(
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping));
        auto runner = std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);

        // Perform smoke test of the connection parameters.
        if (!globalLDAPParams->serverHosts.empty() && globalLDAPParams->smokeTestOnStartup) {
            StatusWith<LDAPEntityCollection> swRes =
                runner->runQuery(LDAPQuery::instantiateQuery(LDAPQueryConfig()).getValue());

            if (!swRes.isOK()) {
                uasserted(ErrorCodes::FailedToParse,
                          str::stream() << "Can't connect to the specified LDAP servers, error: "
                                        << swRes.getStatus().reason());
            }
        }

        auto manager = std::make_unique<LDAPManagerImpl>(
            std::move(runner), std::move(queryParameters), std::move(mapper));
        LDAPManager::set(service, std::move(manager));
    }};
}  // namespace
}  // namespace mongo
