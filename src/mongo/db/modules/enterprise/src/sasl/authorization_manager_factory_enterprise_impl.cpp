/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "sasl/authorization_manager_factory_enterprise_impl.h"

#include "idp_manager.h"
#include "ldap/authorization_backend_ldap.h"
#include "ldap/ldap_options.h"
#include "sasl/idp_manager.h"

#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_backend_interface.h"
#include "mongo/db/auth/authorization_backend_local.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_impl.h"
#include "mongo/db/auth/authorization_router_impl.h"
#include "mongo/db/cluster_role.h"
#include "mongo/db/operation_context.h"

namespace mongo {
std::unique_ptr<auth::AuthorizationBackendInterface>
AuthorizationManagerFactoryEnterpriseImpl::createBackendInterface(Service* service) {
    invariant(service->role().has(ClusterRole::ShardServer) ||
              service->role().has(ClusterRole::ConfigServer));
    if (globalLDAPParams->isLDAPAuthzEnabled()) {
        return std::make_unique<auth::AuthorizationBackendLDAP>();
    }
    return std::make_unique<auth::AuthorizationBackendLocal>();
}

Status AuthorizationManagerFactoryEnterpriseImpl::initialize(OperationContext* opCtx) {
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
    globalAuthzManagerFactory = std::make_unique<AuthorizationManagerFactoryEnterpriseImpl>();
}

}  // namespace

}  // namespace mongo
