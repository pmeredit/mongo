/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_factory.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/service_context.h"

namespace mongo {

extern std::unique_ptr<AuthzManagerExternalState> (*createOIDCAuthzManagerExternalState)(
    std::unique_ptr<AuthzManagerExternalState>);

class AuthorizationManagerFactoryExternalImpl : public AuthorizationManagerFactory {

    std::unique_ptr<AuthorizationManager> createRouter(Service* service) final;

    std::unique_ptr<AuthorizationManager> createShard(Service* service) final;
};

}  // namespace mongo
