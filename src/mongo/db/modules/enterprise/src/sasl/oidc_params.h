/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <string>

namespace mongo {

struct OIDCGlobalParams {
    std::string authURL;
    std::string clientId;
    std::string clientSecret;
};

extern OIDCGlobalParams oidcGlobalParams;

}  // namespace mongo
