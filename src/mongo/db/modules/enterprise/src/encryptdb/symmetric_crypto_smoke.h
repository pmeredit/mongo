/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

namespace mongo {

class Status;

namespace crypto {

/**
 * Tests AES operations, returns a non-OK Status if encryption/decryption has unexpected outputs.
 */
Status smokeTestAESCipherMode(const std::string& mode);

}  // namespace crypto
}  // namespace mongo
