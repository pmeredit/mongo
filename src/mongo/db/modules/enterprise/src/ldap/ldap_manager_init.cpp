/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/duration.h"

#include "ldap/connections/ldap_connection_factory.h"
#include "ldap/ldap_parameters_gen.h"
#include "ldap_manager_impl.h"
#include "ldap_options.h"
#include "ldap_query.h"

namespace mongo {
namespace {

// Define LDAP-related ServerStatusSections.
class LDAPConnectionFactoryServerStatus : public ServerStatusSection {
public:
    using ServerStatusSection::ServerStatusSection;

    bool includeByDefault() const override {
        // Include this section by default if there are any LDAP servers defined.
        return LDAPManager::get(getGlobalServiceContext())->hasHosts();
    }

    BSONObj generateSection(OperationContext*, const BSONElement&) const override {
        invariant(_factory);
        return _factory->generateServerStatusData();
    }

    void setFactory(const LDAPConnectionFactory* factory) {
        _factory = factory;
    }

private:
    const LDAPConnectionFactory* _factory{nullptr};
};

auto& ldapConnectionFactoryServerStatus =
    *ServerStatusSectionBuilder<LDAPConnectionFactoryServerStatus>("ldapConnPool")
         .forShard()
         .forRouter();

class LDAPOperationsServerStatusSection : public ServerStatusSection {
public:
    using ServerStatusSection::ServerStatusSection;

    bool includeByDefault() const override {
        const auto ls = LDAPCumulativeOperationStats::get();
        return nullptr != ls && ls->hasData();
    }

    BSONObj generateSection(OperationContext* opCtx,
                            const BSONElement& configElement) const override {
        const auto ls = LDAPCumulativeOperationStats::get();
        if (nullptr == ls) {
            return BSONObj();
        }
        BSONObjBuilder builder;
        ls->report(&builder);
        return builder.obj();
    }
};
auto& ldapOperationsServerStatus =
    *ServerStatusSectionBuilder<LDAPOperationsServerStatusSection>("ldapOperations")
         .forShard()
         .forRouter();

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

        auto queryParameters =
            uassertStatusOK(LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(
                globalLDAPParams->userAcquisitionQueryTemplate));
        auto mapper = uassertStatusOK(
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping));

        LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                                globalLDAPParams->serverHosts,
                                                globalLDAPParams->retryCount);

        auto factory = std::make_unique<LDAPConnectionFactory>(connectionOptions.timeout);

        ldapConnectionFactoryServerStatus.setFactory(factory.get());

        auto runner =
            std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions, std::move(factory));

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
