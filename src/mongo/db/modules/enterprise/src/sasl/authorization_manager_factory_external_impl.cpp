/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "sasl/authorization_manager_factory_external_impl.h"

#include "idp_manager.h"
#include "ldap/authorization_backend_ldap.h"
#include "ldap/authz_manager_external_state_ldap.h"
#include "ldap/ldap_options.h"

#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_backend_local.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_impl.h"
#include "mongo/db/auth/authorization_router_impl.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/authz_manager_external_state_d.h"
#include "mongo/db/auth/authz_manager_external_state_s.h"

namespace mongo {

std::unique_ptr<AuthorizationManager> AuthorizationManagerFactoryExternalImpl::createRouter(
    Service* service) {
    std::unique_ptr<AuthzManagerExternalState> externalState =
        std::make_unique<AuthzManagerExternalStateMongos>();
    std::unique_ptr<AuthorizationRouter> authzRouter = std::make_unique<AuthorizationRouterImpl>(
        service, std::make_unique<AuthorizationClientHandleRouter>());

    if (createOIDCAuthzManagerExternalState) {
        externalState = createOIDCAuthzManagerExternalState(std::move(externalState));
    }

    return std::make_unique<AuthorizationManagerImpl>(
        service, std::move(externalState), std::move(authzRouter));
}

std::unique_ptr<AuthorizationManager> AuthorizationManagerFactoryExternalImpl::createShard(
    Service* service) {
    std::unique_ptr<AuthzManagerExternalState> externalState =
        std::make_unique<AuthzManagerExternalStateMongod>();
    std::unique_ptr<AuthorizationRouter> authzRouter = std::make_unique<AuthorizationRouterImpl>(
        service, std::make_unique<AuthorizationClientHandleShard>());

    if (globalLDAPParams->isLDAPAuthzEnabled()) {
        externalState = std::make_unique<AuthzManagerExternalStateLDAP>(
            std::unique_ptr<AuthzManagerExternalStateMongod>(
                reinterpret_cast<AuthzManagerExternalStateMongod*>(externalState.release())));
    }

    if (createOIDCAuthzManagerExternalState) {
        externalState = createOIDCAuthzManagerExternalState(std::move(externalState));
    }

    return std::make_unique<AuthorizationManagerImpl>(
        service, std::move(externalState), std::move(authzRouter));
}

std::unique_ptr<auth::AuthorizationBackendInterface>
AuthorizationManagerFactoryExternalImpl::createBackendInterface(Service* service) {
    invariant(service->role().has(ClusterRole::ShardServer) ||
              service->role().has(ClusterRole::ConfigServer));
    if (globalLDAPParams->isLDAPAuthzEnabled()) {
        return std::make_unique<auth::AuthorizationBackendLDAP>();
    }
    return std::make_unique<auth::AuthorizationBackendLocal>();
}

Status AuthorizationManagerFactoryExternalImpl::initialize(OperationContext* opCtx) {
#if MONGO_CONFIG_SSL_PROVIDER == MONGO_CONFIG_SSL_PROVIDER_OPENSSL
    if (auth::IDPManager::isOIDCEnabled() && auth::IDPManager::get()) {
        auth::IDPManager::get()->initialize();
    }
#endif

    if (auth::AuthorizationBackendInterface::get(opCtx->getService())) {
        return auth::AuthorizationBackendInterface::get(opCtx->getService())->initialize(opCtx);
    }

    return Status::OK();
}

namespace {

MONGO_INITIALIZER_WITH_PREREQUISITES(RegisterExternalAuthzManagerFactory,
                                     ("RegisterGlobalAuthzManagerFactory"))
(InitializerContext* initializer) {
    globalAuthzManagerFactory = std::make_unique<AuthorizationManagerFactoryExternalImpl>();
}

}  // namespace

}  // namespace mongo
