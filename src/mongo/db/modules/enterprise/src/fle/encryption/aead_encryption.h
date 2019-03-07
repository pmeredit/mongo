/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include "mongo/base/status.h"

#include "encryptdb/symmetric_key.h"

namespace mongo {
namespace crypto {

/**
 * Constants used in the AEAD function
 */
const size_t kAeadAesHmacKeySize = 64;
const size_t kHmacOutSize = 32;

/**
 * Returns the length of the ciphertext output given the plaintext length. Only for AEAD.
 */
size_t aeadCipherOutputLength(size_t plainTextLen);

/**
 * Encrypts the plaintext using following the AEAD_AES_256_CBC_HMAC_SHA_512 encryption
 * algorithm. Writes output to out.
 */
Status aeadEncrypt(const SymmetricKey& key,
                   const uint8_t* in,
                   size_t inLen,
                   const uint8_t* iv,
                   const size_t ivLen,
                   uint8_t* associatedData,
                   const size_t associatedDataLen,
                   uint8_t* out,
                   const size_t outLen);

/**
 * Decrypts the cipherText using AEAD_AES_256_CBC_HMAC_SHA_512 decryption. Writes output
 * to out.
 */
Status aeadDecrypt(const SymmetricKey& key,
                   const uint8_t* cipherText,
                   const size_t cipherLen,
                   const uint8_t* associatedData,
                   const size_t associatedDataLen,
                   uint8_t* out,
                   size_t* outLen);

}  // namespace crypto
}  // namespace mongo
