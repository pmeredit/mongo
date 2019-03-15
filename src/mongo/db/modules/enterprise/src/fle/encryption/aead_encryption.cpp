/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "aead_encryption.h"

#include "encryptdb/symmetric_crypto.h"

#include "mongo/base/data_view.h"
#include "mongo/crypto/sha512_block.h"
#include "mongo/util/secure_compare_memory.h"

namespace mongo {
namespace crypto {

size_t aeadCipherOutputLength(size_t plainTextLen) {
    // To calculate the size of the byte, we divide by the byte size and add 2 for padding
    // (1 for the attached IV, and 1 for the extra padding). The algorithm will add padding even
    // if the len is a multiple of the byte size, so if the len divides cleanly it will be
    // 32 bytes longer than the original, which is 16 bytes as padding and 16 bytes for the
    // IV. For things that don't divide cleanly, the cast takes care of floor dividing so it will
    // be 0 < x < 16 bytes added for padding and 16 bytes added for the IV.
    size_t aesOutLen = aesBlockSize * (plainTextLen / aesBlockSize + 2);
    return aesOutLen + kHmacOutSize;
}

Status aeadEncrypt(const SymmetricKey& key,
                   const uint8_t* in,
                   const size_t inLen,
                   const uint8_t* iv,
                   const size_t ivLen,
                   const uint8_t* associatedData,
                   const uint64_t associatedDataLen,
                   uint8_t* out,
                   const size_t outLen) {
    if (key.getKeySize() != kAeadAesHmacKeySize) {
        return Status(ErrorCodes::BadValue, "Invalid key size.");
    }

    if (!(in && out)) {
        return Status(ErrorCodes::BadValue, "Invalid AEAD parameters.");
    }

    if (outLen != aeadCipherOutputLength(inLen)) {
        return Status(ErrorCodes::BadValue, "Invalid output buffer size.");
    }

    if (key.getAlgorithm() != aesAlgorithm) {
        return Status(ErrorCodes::BadValue, "Invalid key.");
    }

    if (associatedDataLen >= 1 << 16) {
        return Status(ErrorCodes::BadValue, "Data for encryption is too large.");
    }

    const uint8_t* macKey = key.getKey();
    const uint8_t* encKey = key.getKey() + sym256KeySize;

    size_t aesOutLen = outLen - kHmacOutSize;

    invariant(ivLen == 16);
    std::copy(iv, iv + ivLen, out);

    size_t cipherTextLen = 0;

    SymmetricKey symEncKey(encKey, sym256KeySize, aesAlgorithm, key.getKeyId(), 1);
    auto sEncrypt = aesEncrypt(
        symEncKey, aesMode::cbc, PageSchema::k0, in, inLen, out, aesOutLen, &cipherTextLen, true);

    if (!sEncrypt.isOK()) {
        return sEncrypt;
    }

    uint64_t dataLenBits = associatedDataLen * 8;
    std::array<char, sizeof(uint64_t)> bigEndian;
    DataView(bigEndian.data()).write<BigEndian<uint64_t>>(dataLenBits);

    SHA512Block hmacOutput = SHA512Block::computeHmac(
        macKey,
        sym256KeySize,
        {ConstDataRange(reinterpret_cast<const char*>(associatedData), associatedDataLen),
         ConstDataRange(reinterpret_cast<const char*>(out), cipherTextLen),
         ConstDataRange(reinterpret_cast<const char*>(bigEndian.data()), bigEndian.size())});

    std::copy(hmacOutput.data(), hmacOutput.data() + kHmacOutSize, out + cipherTextLen);
    return Status::OK();
}

Status aeadDecrypt(const SymmetricKey& key,
                   const uint8_t* cipherText,
                   const size_t cipherLen,
                   const uint8_t* associatedData,
                   const uint64_t associatedDataLen,
                   uint8_t* out,
                   size_t* outLen) {

    if (key.getKeySize() != kAeadAesHmacKeySize) {
        return Status(ErrorCodes::BadValue, "Invalid key size.");
    }

    if (!(cipherText && out)) {
        return Status(ErrorCodes::BadValue, "Invalid AEAD parameters.");
    }

    if ((*outLen) != cipherLen) {
        return Status(ErrorCodes::BadValue, "Output buffer must be as long as the cipherText.");
    }

    if (associatedDataLen >= 1 << 16) {
        return Status(ErrorCodes::BadValue, "Data for encryption is too large.");
    }

    const uint8_t* macKey = key.getKey();
    const uint8_t* encKey = key.getKey() + sym256KeySize;

    if (cipherLen < kHmacOutSize) {
        return Status(ErrorCodes::BadValue, "Ciphertext is not long enough.");
    }
    size_t aesLen = cipherLen - kHmacOutSize;

    uint64_t dataLenBits = associatedDataLen * 8;
    std::array<char, sizeof(uint64_t)> bigEndian;
    DataView(bigEndian.data()).write<BigEndian<uint64_t>>(dataLenBits);

    SHA512Block hmacOutput = SHA512Block::computeHmac(
        macKey,
        sym256KeySize,
        {ConstDataRange(reinterpret_cast<const char*>(associatedData), associatedDataLen),
         ConstDataRange(reinterpret_cast<const char*>(cipherText), aesLen),
         ConstDataRange(reinterpret_cast<const char*>(bigEndian.data()), bigEndian.size())});

    if (consttimeMemEqual(reinterpret_cast<const unsigned char*>(hmacOutput.data()),
                          reinterpret_cast<const unsigned char*>(cipherText + aesLen),
                          kHmacOutSize) == false) {
        return Status(ErrorCodes::BadValue, "HMAC data authentication failed.");
    }

    SymmetricKey symEncKey(encKey, sym256KeySize, aesAlgorithm, key.getKeyId(), 1);

    auto sDecrypt = aesDecrypt(
        symEncKey, aesMode::cbc, PageSchema::k0, cipherText, aesLen, out, aesLen, outLen);
    if (!sDecrypt.isOK()) {
        return sDecrypt;
    }

    return Status::OK();
}

}  // namespace crypto
}  // namespace mongo
