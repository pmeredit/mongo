/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "sasl/authorization_manager_factory_external_impl.h"

#include "ldap/authz_manager_external_state_ldap.h"
#include "ldap/ldap_options.h"
#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_impl.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/authz_manager_external_state_d.h"
#include "mongo/db/auth/authz_manager_external_state_s.h"

namespace mongo {

std::unique_ptr<AuthorizationManager> AuthorizationManagerFactoryExternalImpl::createRouter(
    Service* service) {
    std::unique_ptr<AuthzManagerExternalState> externalState =
        std::make_unique<AuthzManagerExternalStateMongos>();

    if (createOIDCAuthzManagerExternalState) {
        externalState = createOIDCAuthzManagerExternalState(std::move(externalState));
    }

    return std::make_unique<AuthorizationManagerImpl>(service, std::move(externalState));
}

std::unique_ptr<AuthorizationManager> AuthorizationManagerFactoryExternalImpl::createShard(
    Service* service) {
    std::unique_ptr<AuthzManagerExternalState> externalState =
        std::make_unique<AuthzManagerExternalStateMongod>();

    if (globalLDAPParams->isLDAPAuthzEnabled()) {
        externalState = std::make_unique<AuthzManagerExternalStateLDAP>(
            std::unique_ptr<AuthzManagerExternalStateMongod>(
                reinterpret_cast<AuthzManagerExternalStateMongod*>(externalState.release())));
    }

    if (createOIDCAuthzManagerExternalState) {
        externalState = createOIDCAuthzManagerExternalState(std::move(externalState));
    }

    return std::make_unique<AuthorizationManagerImpl>(service, std::move(externalState));
}

namespace {

MONGO_INITIALIZER_WITH_PREREQUISITES(RegisterExternalAuthzManagerFactory,
                                     ("RegisterGlobalAuthzManagerFactory"))
(InitializerContext* initializer) {
    globalAuthzManagerFactory = std::make_unique<AuthorizationManagerFactoryExternalImpl>();
}

}  // namespace

}  // namespace mongo
