/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/auth/authz_manager_external_state.h"
#include "sasl/authorization_manager_factory_external_impl.h"

namespace mongo {

// We have to link in AuthzManagerOIDC this way because it only exists on platforms
// that support OpenSSL.
std::unique_ptr<AuthzManagerExternalState> (*createOIDCAuthzManagerExternalState)(
    std::unique_ptr<AuthzManagerExternalState>);

}  // namespace mongo
