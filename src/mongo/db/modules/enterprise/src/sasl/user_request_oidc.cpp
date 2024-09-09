/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "user_request_oidc.h"

#include "mongo/db/auth/user.h"

namespace mongo {

UserRequest::UserRequestCacheKey UserRequestOIDC::generateUserRequestCacheKey() const {
    auto hashElements = getUserNameAndRolesVector(getUserName(), getRoles());
    hashElements.push_back(getJWTString().toString());
    return UserRequestCacheKey(getUserName(), hashElements);
}
}  // namespace mongo
