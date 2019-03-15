/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

#include "symmetric_crypto.h"

#include "mongo/base/status.h"

namespace mongo {
namespace crypto {

/**
 * Tests AES operations, returns a non-OK Status if encryption/decryption has unexpected outputs.
 */
Status smokeTestAESCipherMode(aesMode, PageSchema);

}  // namespace crypto
}  // namespace mongo
