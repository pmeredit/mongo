/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

/**
 * Utilities for performing simple GSSAPI operations.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"

namespace mongo {
namespace gssapi {

    /**
     * Canonicalize the user principal for "name".
     */
    Status canonicalizeUserName(const StringData& name, std::string* canonicalName);

    /**
     * Canonicalize the server principal for "name".
     */
    Status canonicalizeServerName(const StringData& name, std::string* canonicalName);

    /**
     * Returns Status::OK() if the process can acquire a GSSAPI credential for the given server
     * principal.
     *
     * Use this function to see if the process can accept connections destined for "principalName",
     * say for validating the keytab supplied in the server configuration.
     */
    Status tryAcquireServerCredential(const StringData& principalName);

}  // namespace gssapi
}  // namespace mongo
