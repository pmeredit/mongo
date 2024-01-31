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
Service::ConstructorActionRegisterer setLDAPManagerImpl{
    "SetLDAPManagerImpl", [](Service* service) {
        auto svcCtx = service->getServiceContext();

        // TODO - SERVER-86083 move back to ServiceContext CAR if ordering is changed.
        //
        // The ordering of initialization between Service and ServiceContext is that
        // 1/ ServiceContext is constructed
        // 2/ Service is constructed
        // 3/ Service CARs are run
        // 4/ ServiceContext CARs are run
        // This needed to move to a Service CAR because it is required by the PLAIN
        // mechanism factory, and therefore needs to run before then. The IF statement
        // is in case we're running in embedded router mode, this CAR runs twice and
        // we don't need to initialize the LDAPManager twice since it hangs off of the
        // ServiceContext (global).
        if (LDAPManager::get(svcCtx) && LDAPManager::get(svcCtx)->isInitialized()) {
            return;
        }

        LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                    globalLDAPParams->bindPassword,
                                    globalLDAPParams->bindMethod,
                                    globalLDAPParams->bindSASLMechanisms,
                                    globalLDAPParams->useOSDefaults);
        LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                                globalLDAPParams->serverHosts,
                                                globalLDAPParams->retryCount);

        auto queryParameters =
            uassertStatusOK(LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(
                globalLDAPParams->userAcquisitionQueryTemplate));
        auto mapper = uassertStatusOK(
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping));
        auto runner = std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);

        // Perform smoke test of the connection parameters.
        if (!globalLDAPParams->serverHosts.empty() && globalLDAPParams->smokeTestOnStartup) {
            auto userAcquisitionStats = std::make_shared<UserAcquisitionStats>();
            auto status = runner->checkLiveness(svcCtx->getTickSource(), userAcquisitionStats);
            if (!status.isOK()) {
                uasserted(status.code(),
                          str::stream() << "Can't connect to the specified LDAP servers, error: "
                                        << status.reason());
            }
        }

        runner->setUseConnectionPool(ldapUseConnectionPool);

        auto manager = std::make_unique<LDAPManagerImpl>(
            std::move(runner), std::move(queryParameters), std::move(mapper));
        LDAPManager::set(svcCtx, std::move(manager));
    }};
}  // namespace
}  // namespace mongo
