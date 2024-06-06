/*
 *    Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/status.h"

#include <string>

#include "mongo/base/secure_allocator.h"

namespace mongo {

struct LDAPToolOptions {
#ifdef _WIN32
    bool color = false;
#else
    bool color = true;
#endif

    bool debug = false;
    std::string user;
    SecureString password;
};

extern LDAPToolOptions* globalLDAPToolOptions;

}  // namespace mongo
