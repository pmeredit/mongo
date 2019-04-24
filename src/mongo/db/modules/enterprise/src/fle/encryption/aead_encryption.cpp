/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "aead_encryption.h"

#include "encryptdb/symmetric_crypto.h"

#include "mongo/base/data_view.h"
#include "mongo/crypto/sha512_block.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/util/secure_compare_memory.h"
namespace mongo {
namespace crypto {

namespace {
constexpr size_t kHmacOutSize = 32;
constexpr size_t kIVSize = 16;

// AssociatedData can be 2^24 bytes but since there needs to be room for the ciphertext in the
// object, a value of 1<<16 was decided to cap the maximum size of AssociatedData.
constexpr int kMaxAssociatedDataLength = 1 << 16;
}  // namespace

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
                   const uint8_t* associatedData,
                   const uint64_t associatedDataLen,
                   uint8_t* out,
                   size_t outLen) {

    if (associatedDataLen >= kMaxAssociatedDataLength) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "AssociatedData for encryption is too large. Cannot be larger than "
                          << kMaxAssociatedDataLength
                          << " bytes.");
    }

    // According to the rfc on AES encryption, the associatedDataLength is defined as the
    // number of bits in associatedData in BigEndian format. This is what the code segment
    // below describes.
    // RFC: (https://tools.ietf.org/html/draft-mcgrew-aead-aes-cbc-hmac-sha2-01#section-2.1)
    std::array<uint8_t, sizeof(uint64_t)> dataLenBitsEncodedStorage;
    DataRange dataLenBitsEncoded(dataLenBitsEncodedStorage);
    dataLenBitsEncoded.write<BigEndian<uint64_t>>(associatedDataLen * 8);

    auto keySize = key.getKeySize();
    if (keySize < kAeadAesHmacKeySize) {
        return Status(ErrorCodes::BadValue,
                      "AEAD encryption key too short. "
                      "Must be either 64 or 96 bytes.");
    }

    ConstDataRange aeadKey(key.getKey(), kAeadAesHmacKeySize);

    if (key.getKeySize() == kAeadAesHmacKeySize) {
        // local key store key encryption
        return aeadEncryptWithIV(aeadKey,
                                 in,
                                 inLen,
                                 nullptr,
                                 0,
                                 associatedData,
                                 associatedDataLen,
                                 dataLenBitsEncoded,
                                 out,
                                 outLen);
    }

    if (key.getKeySize() != kFieldLevelEncryptionKeySize) {
        return Status(ErrorCodes::BadValue, "Invalid key size.");
    }

    if (in == nullptr || !in) {
        return Status(ErrorCodes::BadValue, "Invalid AEAD plaintext input.");
    }

    if (key.getAlgorithm() != aesAlgorithm) {
        return Status(ErrorCodes::BadValue, "Invalid algorithm for key.");
    }

    ConstDataRange hmacCDR(nullptr, 0);
    SHA512Block hmacOutput;
    if (static_cast<int>(associatedData[0]) ==
        FleAlgorithmInt_serializer(FleAlgorithmInt::kDeterministic)) {
        const uint8_t* ivKey = key.getKey() + kAeadAesHmacKeySize;
        hmacOutput = SHA512Block::computeHmac(ivKey,
                                              sym256KeySize,
                                              {ConstDataRange(associatedData, associatedDataLen),
                                               dataLenBitsEncoded,
                                               ConstDataRange(in, inLen)});

        static_assert(SHA512Block::kHashLength >= kIVSize,
                      "Invalid AEAD parameters. Generated IV too short.");

        hmacCDR = ConstDataRange(hmacOutput.data(), kIVSize);
    }
    return aeadEncryptWithIV(aeadKey,
                             in,
                             inLen,
                             reinterpret_cast<const uint8_t*>(hmacCDR.data()),
                             hmacCDR.length(),
                             associatedData,
                             associatedDataLen,
                             dataLenBitsEncoded,
                             out,
                             outLen);
}

Status aeadEncryptWithIV(ConstDataRange key,
                         const uint8_t* in,
                         const size_t inLen,
                         const uint8_t* iv,
                         const size_t ivLen,
                         const uint8_t* associatedData,
                         const uint64_t associatedDataLen,
                         ConstDataRange dataLenBitsEncoded,
                         uint8_t* out,
                         size_t outLen) {
    if (key.length() != kAeadAesHmacKeySize) {
        return Status(ErrorCodes::BadValue, "Invalid key size.");
    }

    if (!(in && out)) {
        return Status(ErrorCodes::BadValue, "Invalid AEAD parameters.");
    }

    if (outLen != aeadCipherOutputLength(inLen)) {
        return Status(ErrorCodes::BadValue, "Invalid output buffer size.");
    }

    if (associatedDataLen >= kMaxAssociatedDataLength) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "AssociatedData for encryption is too large. Cannot be larger than "
                          << kMaxAssociatedDataLength
                          << " bytes.");
    }

    const uint8_t* macKey = reinterpret_cast<const uint8_t*>(key.data());
    const uint8_t* encKey = reinterpret_cast<const uint8_t*>(key.data() + sym256KeySize);

    size_t aesOutLen = outLen - kHmacOutSize;

    size_t cipherTextLen = 0;

    SymmetricKey symEncKey(encKey, sym256KeySize, aesAlgorithm, "aesKey", 1);

    bool ivProvided = false;
    if (ivLen != 0) {
        invariant(ivLen == 16);
        std::copy(iv, iv + ivLen, out);
        ivProvided = true;
    }

    auto sEncrypt = aesEncrypt(symEncKey,
                               aesMode::cbc,
                               PageSchema::k0,
                               in,
                               inLen,
                               out,
                               aesOutLen,
                               &cipherTextLen,
                               ivProvided);

    if (!sEncrypt.isOK()) {
        return sEncrypt;
    }

    SHA512Block hmacOutput =
        SHA512Block::computeHmac(macKey,
                                 sym256KeySize,
                                 {ConstDataRange(associatedData, associatedDataLen),
                                  ConstDataRange(out, cipherTextLen),
                                  dataLenBitsEncoded});

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
    if (key.getKeySize() < kAeadAesHmacKeySize) {
        return Status(ErrorCodes::BadValue, "Invalid key size.");
    }

    if (!(cipherText && out)) {
        return Status(ErrorCodes::BadValue, "Invalid AEAD parameters.");
    }

    if ((*outLen) != cipherLen) {
        return Status(ErrorCodes::BadValue, "Output buffer must be as long as the cipherText.");
    }

    if (associatedDataLen >= kMaxAssociatedDataLength) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "AssociatedData for encryption is too large. Cannot be larger than "
                          << kMaxAssociatedDataLength
                          << " bytes.");
    }

    const uint8_t* macKey = key.getKey();
    const uint8_t* encKey = key.getKey() + sym256KeySize;

    if (cipherLen < kHmacOutSize) {
        return Status(ErrorCodes::BadValue, "Ciphertext is not long enough.");
    }
    size_t aesLen = cipherLen - kHmacOutSize;

    // According to the rfc on AES encryption, the associatedDataLength is defined as the
    // number of bits in associatedData in BigEndian format. This is what the code segment
    // below describes.
    std::array<uint8_t, sizeof(uint64_t)> dataLenBitsEncodedStorage;
    DataRange dataLenBitsEncoded(dataLenBitsEncodedStorage);
    dataLenBitsEncoded.write<BigEndian<uint64_t>>(associatedDataLen * 8);

    SHA512Block hmacOutput =
        SHA512Block::computeHmac(macKey,
                                 sym256KeySize,
                                 {ConstDataRange(associatedData, associatedDataLen),
                                  ConstDataRange(cipherText, aesLen),
                                  dataLenBitsEncoded});

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
