/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#pragma once

#include "mongo/platform/basic.h"

#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/db/auth/user.h"

namespace mongo {

/**
 * This is the version of UserRequest that is used by X509. It provides
 * a way to store the OIDC specific metadata for retrieving roles.
 */
class UserRequestOIDC : public UserRequestGeneral {
public:
    UserRequestOIDC(UserName name, boost::optional<std::set<RoleName>> roles, std::string token)
        : UserRequestGeneral(std::move(name), std::move(roles)), _jwtString(std::move(token)) {}
    UserRequestType getType() const final {
        return UserRequestType::OIDC;
    }
    StringData getJWTString() const {
        return _jwtString;
    }
    std::unique_ptr<UserRequest> clone() const final {
        return std::make_unique<UserRequestOIDC>(
            getUserName(), getRoles(), getJWTString().toString());
    }
    UserRequestCacheKey generateUserRequestCacheKey() const final;

private:
    std::string _jwtString;
};
}  // namespace mongo
