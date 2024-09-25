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
 * This is the version of UserRequest that is used by OIDC. It provides
 * a way to store the OIDC specific metadata for retrieving roles.
 *
 * When constructing the UserRequestOIDC, you must use the static function
 * makeUserRequestOIDC. It will automatically populate the roles from the
 * JWT if they exist.
 */
class UserRequestOIDC : public UserRequestGeneral {
public:
    friend std::unique_ptr<UserRequestOIDC> std::make_unique<UserRequestOIDC>(
        mongo::UserName&& name,
        boost::optional<std::set<mongo::RoleName>>&& roles,
        std::string&& token);

    /**
     * Makes a new UserRequestOIDC. Toggling for re-acquire to true enables
     * a re-fetch of the roles from the OIDC server.
     */
    static StatusWith<std::unique_ptr<UserRequest>> makeUserRequestOIDC(
        UserName name,
        boost::optional<std::set<RoleName>> roles,
        std::string token,
        bool forReacquire = true);

    UserRequestType getType() const final {
        return UserRequestType::OIDC;
    }
    StringData getJWTString() const {
        return _jwtString;
    }

    std::unique_ptr<UserRequest> clone() const final {
        // Since we are invoking the non-reacquire version of makeUserRequestOIDC,
        // we can be certain that the uassert below will not throw.
        return uassertStatusOK(
            makeUserRequestOIDC(getUserName(), getRoles(), getJWTString().toString(), false));
    }

    /**
     * Since we refresh the roles with a call to clone, it's ok to just call clone.
     */
    StatusWith<std::unique_ptr<UserRequest>> cloneForReacquire() const final {
        return makeUserRequestOIDC(getUserName(), getRoles(), getJWTString().toString());
    }

    UserRequestCacheKey generateUserRequestCacheKey() const final;

protected:
    UserRequestOIDC(UserName name, boost::optional<std::set<RoleName>> roles, std::string token)
        : UserRequestGeneral(std::move(name), std::move(roles)), _jwtString(std::move(token)) {}

private:
    Status _tryAcquireRoles();

    std::string _jwtString;
};
}  // namespace mongo
