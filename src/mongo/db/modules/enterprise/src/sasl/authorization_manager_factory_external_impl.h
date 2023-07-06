/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
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

    std::unique_ptr<AuthorizationManager> createRouter(ServiceContext* service) final;

    std::unique_ptr<AuthorizationManager> createShard(ServiceContext* service) final;
};

}  // namespace mongo
