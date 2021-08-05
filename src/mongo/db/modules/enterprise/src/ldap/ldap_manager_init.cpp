/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/duration.h"

#include "ldap/ldap_parameters_gen.h"
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
                                                globalLDAPParams->serverHosts);

        auto queryParameters =
            uassertStatusOK(LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(
                globalLDAPParams->userAcquisitionQueryTemplate));
        auto mapper = uassertStatusOK(
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping));
        auto runner = std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);

        // Perform smoke test of the connection parameters.
        if (!globalLDAPParams->serverHosts.empty() && globalLDAPParams->smokeTestOnStartup) {
            std::unique_ptr<UserAcquisitionStats> userAcquisitionStats =
                std::make_unique<UserAcquisitionStats>();
            auto status =
                runner->checkLiveness(service->getTickSource(), userAcquisitionStats.get());
            if (!status.isOK()) {
                uasserted(status.code(),
                          str::stream() << "Can't connect to the specified LDAP servers, error: "
                                        << status.reason());
            }
        }

        runner->setUseConnectionPool(ldapUseConnectionPool);

        auto manager = std::make_unique<LDAPManagerImpl>(
            std::move(runner), std::move(queryParameters), std::move(mapper));
        LDAPManager::set(service, std::move(manager));
    }};
}  // namespace
}  // namespace mongo
