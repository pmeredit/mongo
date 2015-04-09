/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <cstddef>

#include "mongo/base/status.h"
#include "mongo/platform/cstdint.h"

namespace mongo {
namespace crypto {

    // Encryption algorithm identifiers and block sizes
    const size_t aesAlgorithm = 0x1;
    const size_t aesBlockSize = 16;

    // Key sizes
    const size_t sym128KeySize = 16;
    const size_t sym256KeySize = 32;

    // Encryption mode identifiers
    const size_t cbcMode = 0x100;
    const size_t gcmMode = 0x200;

    /*
     * Encrypts the plaintext 'in' using AES with 'key' and block size 'keySize'
     * using encryption mode 'mode'. Supported modes are CBC and GCM.
     *
     * The size of the encrypted buffer 'out' is returned in 'outLen'.
     */
    Status aesEncrypt(const uint8_t* in,
                      size_t inLen,
                      const uint8_t* key,
                      size_t keySize,
                      int mode,
                      const uint8_t* iv,
                      uint8_t* out,
                      size_t* outLen);

    /*
     * Decrypts the plaintext 'in' using AES with 'key' and block size 'keySize'
     * using encryption mode 'mode'. Supported modes are CBC and GCM.
     *
     * The size of the decrypted buffer 'out' is provided in 'outLen'.
     */
    Status aesDecrypt(const uint8_t* in,
                      size_t inLen,
                      const uint8_t* key,
                      size_t keySize,
                      int mode,
                      const uint8_t* iv,
                      uint8_t* out,
                      size_t* outLen);

} // namespace crypto
} // namespace mongo
