/*
 * Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

/**
 * Utilities for performing simple GSSAPI operations.
 */

#pragma once

#include <string>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"

namespace mongo {
namespace gssapi {

/**
 * Canonicalize the user principal for "name".
 */
StatusWith<std::string> canonicalizeUserName(StringData name);

/**
 * Canonicalize the server principal for "name".
 */
StatusWith<std::string> canonicalizeServerName(StringData name);

/**
 * Returns Status::OK() if the process can acquire a GSSAPI credential for the given server
 * principal.
 *
 * Use this function to see if the process can accept connections destined for "principalName",
 * say for validating the keytab supplied in the server configuration.
 */
Status tryAcquireServerCredential(const std::string& principalName);

}  // namespace gssapi
}  // namespace mongo
