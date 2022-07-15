/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <string>

#include "mongo/crypto/jwk_manager.h"

namespace mongo {

struct OIDCGlobalParams {
    std::string authURL;
    std::string clientId;
    std::string clientSecret;
    std::string keySetFile;
};

extern OIDCGlobalParams oidcGlobalParams;

/**
 * Retrieve the global JWKManager instance for OIDC keys.
 */
crypto::JWKManager* getOIDCKeyManager();

}  // namespace mongo
