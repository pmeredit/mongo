/**
 *    Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "symmetric_crypto_smoke.h"

#include <vector>

#include "mongo/base/status.h"
#include "mongo/crypto/symmetric_key.h"
#include "mongo/util/str.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace crypto {

namespace {
// make space for a padding block
constexpr std::size_t kMaxPTSize = (4 + 1) * crypto::aesBlockSize;

struct aesTest {
    std::size_t keySize;
    crypto::aesMode mode;
    std::size_t ptLen;
    std::uint8_t key[kMaxPTSize];
    std::uint8_t iv[kMaxPTSize];
    std::uint8_t pt[kMaxPTSize];
    std::uint8_t ct[kMaxPTSize];
    std::uint8_t tagV0[kMaxPTSize];
    std::uint8_t tagV1[kMaxPTSize];
};

// AES CBC mode test vectors from NIST sp800-38a
// clang-format off
const aesTest kAESTestCBCData = {
    crypto::sym256KeySize,
    crypto::aesMode::cbc,
    4 * crypto::aesBlockSize,
    {0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
     0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
     0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
     0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4},
    {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
     0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
    {0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96,
     0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17, 0x2a,
     0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03, 0xac, 0x9c,
     0x9e, 0xb7, 0x6f, 0xac, 0x45, 0xaf, 0x8e, 0x51,
     0x30, 0xc8, 0x1c, 0x46, 0xa3, 0x5c, 0xe4, 0x11,
     0xe5, 0xfb, 0xc1, 0x19, 0x1a, 0x0a, 0x52, 0xef,
     0xf6, 0x9f, 0x24, 0x45, 0xdf, 0x4f, 0x9b, 0x17,
     0xad, 0x2b, 0x41, 0x7b, 0xe6, 0x6c, 0x37, 0x10},
    {0xf5, 0x8c, 0x4c, 0x04, 0xd6, 0xe5, 0xf1, 0xba,
     0x77, 0x9e, 0xab, 0xfb, 0x5f, 0x7b, 0xfb, 0xd6,
     0x9c, 0xfc, 0x4e, 0x96, 0x7e, 0xdb, 0x80, 0x8d,
     0x67, 0x9f, 0x77, 0x7b, 0xc6, 0x70, 0x2c, 0x7d,
     0x39, 0xf2, 0x33, 0x69, 0xa9, 0xd9, 0xba, 0xcf,
     0xa5, 0x30, 0xe2, 0x63, 0x04, 0x23, 0x14, 0x61,
     0xb2, 0xeb, 0x05, 0xe2, 0xc3, 0x9b, 0xe9, 0xfc,
     0xda, 0x6c, 0x19, 0x07, 0x8c, 0x6a, 0x9d, 0x1b},
    {},
    {},
};
// clang-format on

// AES GCM mode test vectors from
// http://csrc.nist.gov/groups/ST/toolkit/BCM/documents/proposedmodes/gcm/gcm-revised-spec.pdf
// clang-format off
const aesTest kAESTestGCMData = {
    crypto::sym256KeySize,
    crypto::aesMode::gcm,
    64,
    {0xfe, 0xff, 0xe9, 0x92, 0x86, 0x65, 0x73, 0x1c,
     0x6d, 0x6a, 0x8f, 0x94, 0x67, 0x30, 0x83, 0x08,
     0xfe, 0xff, 0xe9, 0x92, 0x86, 0x65, 0x73, 0x1c,
     0x6d, 0x6a, 0x8f, 0x94, 0x67, 0x30, 0x83, 0x08},
    {0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce, 0xdb, 0xad,
     0xde, 0xca, 0xf8, 0x88},
    {0xd9, 0x31, 0x32, 0x25, 0xf8, 0x84, 0x06, 0xe5,
     0xa5, 0x59, 0x09, 0xc5, 0xaf, 0xf5, 0x26, 0x9a,
     0x86, 0xa7, 0xa9, 0x53, 0x15, 0x34, 0xf7, 0xda,
     0x2e, 0x4c, 0x30, 0x3d, 0x8a, 0x31, 0x8a, 0x72,
     0x1c, 0x3c, 0x0c, 0x95, 0x95, 0x68, 0x09, 0x53,
     0x2f, 0xcf, 0x0e, 0x24, 0x49, 0xa6, 0xb5, 0x25,
     0xb1, 0x6a, 0xed, 0xf5, 0xaa, 0x0d, 0xe6, 0x57,
     0xba, 0x63, 0x7b, 0x39, 0x1a, 0xaf, 0xd2, 0x55},
    {0x52, 0x2d, 0xc1, 0xf0, 0x99, 0x56, 0x7d, 0x07,
     0xf4, 0x7f, 0x37, 0xa3, 0x2a, 0x84, 0x42, 0x7d,
     0x64, 0x3a, 0x8c, 0xdc, 0xbf, 0xe5, 0xc0, 0xc9,
     0x75, 0x98, 0xa2, 0xbd, 0x25, 0x55, 0xd1, 0xaa,
     0x8c, 0xb0, 0x8e, 0x48, 0x59, 0x0d, 0xbb, 0x3d,
     0xa7, 0xb0, 0x8b, 0x10, 0x56, 0x82, 0x88, 0x38,
     0xc5, 0xf6, 0x1e, 0x63, 0x93, 0xba, 0x7a, 0x0a,
     0xbc, 0xc9, 0xf6, 0x62, 0x89, 0x80, 0x15, 0xad},
    {0xb0, 0x94, 0xda, 0xc5, 0xd9, 0x34, 0x71, 0xbd,
     0xec, 0x1a, 0x50, 0x22},
    {0xc2, 0xa1, 0x87, 0xa2, 0x60, 0x66, 0xda, 0x6a,
     0x02, 0x9e, 0x0e, 0xcb},
};
// clang-format on

// Changing this value will alter the expected tag for GCMv1
constexpr SymmetricKeyId::id_type kKeyIdForTest = 0x01234567890ABCDEF;

template <typename T>
Status _doSmokeTestAESCipherMode(const aesTest& test) {
    invariant(T::kMode == test.mode);

    const size_t outputBufferSize = 4 * crypto::aesBlockSize + kMaxPTSize;

    std::uint8_t pt[kMaxPTSize];
    std::uint8_t outputBuffer[outputBufferSize];
    std::size_t resultLen;

    // Initialize buffers.
    memset(pt, 0, kMaxPTSize);
    memset(outputBuffer, 0, outputBufferSize);

    SymmetricKeyId keyId("test", kKeyIdForTest);
    SymmetricKey key(test.key, test.keySize, crypto::aesAlgorithm, keyId, 0);

    crypto::MutableEncryptedMemoryLayout<T> layout(outputBuffer, outputBufferSize);
    invariant(layout.getIVSize() <= sizeof(test.iv));
    memcpy(layout.getIV(), test.iv, layout.getIVSize());

    Status ret = crypto::aesEncrypt(key,
                                    T::kMode,
                                    T::kSchema,
                                    test.pt,
                                    test.ptLen,
                                    outputBuffer,
                                    outputBufferSize,
                                    &resultLen,
                                    true);

    if (getSupportedSymmetricAlgorithms().count(getStringFromCipherMode(T::kMode)) == 0) {
        // The platform does not support this cipher mode, so expect failure
        if (ret.isOK()) {
            return {ErrorCodes::OperationFailed,
                    "aesEncrypt succeeded for an unsupported ciphermode"};
        }
        return Status::OK();
    }

    if (!ret.isOK()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "aesEncrypt failed: " << ret.reason()};
    }

    // Check tag for GCM.
    if (T::kMode == crypto::aesMode::gcm) {
        const auto* testTag = test.tagV0;
        if (T::kSchema == PageSchema::k1) {
            testTag = test.tagV1;
        }
        if (memcmp(layout.getTag(), testTag, layout.getTagSize()) != 0) {
            return {ErrorCodes::OperationFailed, "GCM tag mismatch"};
        }
    }

    // Check Page Schema v1 marker, version, and ID.
    if ((T::kMode == crypto::aesMode::gcm) && (T::kSchema == PageSchema::k1)) {
        ConstDataRangeCursor cursor(layout.getExtra(), layout.getExtraSize());

        const auto marker = cursor.readAndAdvance<std::uint32_t>();
        if (marker != std::numeric_limits<std::uint32_t>::max()) {
            return {ErrorCodes::OperationFailed, "PageSchema marker invalid"};
        }

        const auto version = cursor.readAndAdvance<std::uint8_t>();
        if (version != 1) {
            return {ErrorCodes::OperationFailed, "Invalid page schema version"};
        }

        const auto id = cursor.readAndAdvance<LittleEndian<SymmetricKeyId::id_type>>();
        if (id != kKeyIdForTest) {
            return {ErrorCodes::OperationFailed, "Incorrect key id found"};
        }
    }

    // Check that the ciphertext matches the stored result in both length and content
    std::size_t expectedSize;
    if (T::kMode == crypto::aesMode::cbc) {
        expectedSize = test.ptLen + crypto::aesBlockSize;
    } else {
        expectedSize = test.ptLen;
    }

    if (expectedSize != resultLen - layout.getHeaderSize()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "aesEncrypt produced " << layout.getDataSize() << " but "
                              << expectedSize << " were expected"};
    }

    if (0 != memcmp(test.ct, layout.getData(), test.ptLen)) {
        return {ErrorCodes::OperationFailed, "aesEncrypt did not produce the correct ciphertext"};
    }

    ret = crypto::aesDecrypt(
        key, T::kMode, T::kSchema, outputBuffer, resultLen, pt, kMaxPTSize, &resultLen);

    if (!ret.isOK()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "aesDecrypt failed: " << ret.reason()};
    }

    if (resultLen != test.ptLen || 0 != memcmp(test.pt, pt, test.ptLen)) {
        return {ErrorCodes::OperationFailed, "aesDecrypt did not produce expected plaintext"};
    }

    // Check that GCM ciphers are validating their tags correctly
    if (T::kMode == crypto::aesMode::gcm) {
        // Corrupt the tag
        layout.getTag()[0] ^= 0xFF;

        if (crypto::aesDecrypt(
                key, T::kMode, T::kSchema, outputBuffer, resultLen, pt, kMaxPTSize, &resultLen)
                .isOK()) {
            return {ErrorCodes::OperationFailed, "Corrupt GCM tag successfully decrypted"};
        }
    }

    return Status::OK();
}
}  // namespace
Status smokeTestAESCipherMode(aesMode mode, PageSchema schema) {
    switch (mode) {
        case aesMode::cbc:
            invariant(schema == PageSchema::k0);
            return _doSmokeTestAESCipherMode<crypto::HeaderCBCV0>(kAESTestCBCData);
        case aesMode::gcm:
            switch (schema) {
                case PageSchema::k0:
                    return _doSmokeTestAESCipherMode<crypto::HeaderGCMV0>(kAESTestGCMData);
                case PageSchema::k1:
                    return _doSmokeTestAESCipherMode<crypto::HeaderGCMV1>(kAESTestGCMData);
                default:
                    MONGO_UNREACHABLE;
            }
        default:
            MONGO_UNREACHABLE;
    }
}

}  // namespace crypto
}  // namespace mongo
