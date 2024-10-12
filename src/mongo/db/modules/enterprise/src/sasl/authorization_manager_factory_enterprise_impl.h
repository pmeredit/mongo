/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "ldap/authorization_backend_ldap.h"

#include "mongo/db/auth/authorization_client_handle.h"
#include "mongo/db/auth/authorization_client_handle_router.h"
#include "mongo/db/auth/authorization_client_handle_shard.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_factory.h"
#include "mongo/db/auth/authorization_manager_factory_impl.h"
#include "mongo/db/service_context.h"

namespace mongo {

class AuthorizationManagerFactoryEnterpriseImpl : public AuthorizationManagerFactoryImpl {
    std::unique_ptr<auth::AuthorizationBackendInterface> createBackendInterface(
        Service* service) final;

    Status initialize(OperationContext* opCtx) final;
};

}  // namespace mongo
