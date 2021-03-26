/**
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */


#include <algorithm>

#include "encryption_options.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/scopeguard.h"
#include "symmetric_crypto.h"
#include "symmetric_crypto_smoke.h"

namespace mongo {
namespace {

const size_t maxPTSize = (4 + 1) * crypto::aesBlockSize;
const size_t outputBufferSize = 3 * crypto::aesBlockSize + maxPTSize;

TEST(AES, CBCTestVectors) {
    ASSERT_OK(crypto::smokeTestAESCipherMode(crypto::aesMode::cbc, crypto::PageSchema::k0));
}

class AESRoundTrip : public mongo::unittest::Test {
public:
    AESRoundTrip() {}

    bool modeSupported(crypto::aesMode mode) {
        return crypto::getSupportedSymmetricAlgorithms().count(getStringFromCipherMode(mode)) != 0;
    }

    Status encrypt(crypto::aesMode mode) {
        return crypto::aesEncrypt(key,
                                  mode,
                                  crypto::PageSchema::k0,
                                  plaintext.data(),
                                  plaintext.size(),
                                  cryptoBuffer.data(),
                                  cryptoBuffer.size(),
                                  &cryptoLen);
    }

    Status decrypt(crypto::aesMode mode) {
        return crypto::aesDecrypt(key,
                                  mode,
                                  crypto::PageSchema::k0,
                                  cryptoBuffer.data(),
                                  cryptoLen,
                                  plainBuffer.data(),
                                  plainBuffer.size(),
                                  &plainLen);
    }

protected:
    bool plainTextMatch() const {
        return std::equal(plaintext.begin(),
                          plaintext.end(),
                          plainBuffer.begin(),
                          plainBuffer.begin() + plaintext.size());
    }

    SymmetricKey key = crypto::aesGenerate(crypto::sym256KeySize, "testID");
    static constexpr std::array<std::uint8_t, 10> plaintext{"plaintext"};
    std::array<std::uint8_t, 1024> cryptoBuffer;
    size_t cryptoLen;
    std::array<std::uint8_t, 1024> plainBuffer;
    size_t plainLen;
};
constexpr std::array<std::uint8_t, 10> AESRoundTrip::plaintext;

TEST_F(AESRoundTrip, CBC) {
    ASSERT_OK(encrypt(crypto::aesMode::cbc));
    ASSERT_OK(decrypt(crypto::aesMode::cbc));
    ASSERT_TRUE(plainTextMatch());

    // Changing the key should result in decryption failure or bad plain text. Make sure to
    // reset plainBuffer before this check.
    key = crypto::aesGenerate(crypto::sym256KeySize, "testID");
    plainBuffer.fill(0);
    if (decrypt(crypto::aesMode::cbc).isOK()) {
        ASSERT_FALSE(plainTextMatch());
    }
}

#ifndef DISABLE_GCM_TESTVECTORS
TEST(AES, GCMTestVectors) {
    ASSERT_OK(crypto::smokeTestAESCipherMode(crypto::aesMode::gcm, crypto::PageSchema::k0));
    ASSERT_OK(crypto::smokeTestAESCipherMode(crypto::aesMode::gcm, crypto::PageSchema::k1));
}

TEST_F(AESRoundTrip, GCM) {
    if (!modeSupported(crypto::aesMode::gcm)) {
        return;
    }
    ASSERT_OK(encrypt(crypto::aesMode::gcm));
    ASSERT_OK(decrypt(crypto::aesMode::gcm));
    ASSERT_TRUE(plainTextMatch());
}
#endif


TEST(EncryptedMemoryLayout, CanCreateMemoryLayoutWithCBC) {
    uint8_t outputBuffer[outputBufferSize];
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderCBCV0> layout(outputBuffer, outputBufferSize);
}

TEST(EncryptedMemoryLayout, CanCreateMemoryLayoutWithGCM) {
    uint8_t outputBuffer[outputBufferSize];
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0> layout(outputBuffer, outputBufferSize);
}

DEATH_TEST(EncryptedMemoryLayout, CannotCreateMemoryLayoutOnNullptr, "invariant") {
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0> layout(nullptr, outputBufferSize);
}

TEST(EncryptedMemoryLayout, CiphertexLen) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    size_t expected = 16;
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderCBCV0> layoutCBC(outputBuffer,
                                                                      outputBufferSize);
    ASSERT_EQ(expected, layoutCBC.expectedCiphertextLen(10));

    // Test GCM
    expected = 10;
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0> layoutGCM(outputBuffer,
                                                                      outputBufferSize);
    ASSERT_EQ(expected, layoutGCM.expectedCiphertextLen(10));
}

TEST(EncryptedMemoryLayout, CanFitPlaintText) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderCBCV0> layoutCBC(outputBuffer,
                                                                      outputBufferSize);
    ASSERT_TRUE(layoutCBC.canFitPlaintext(outputBufferSize - 32));
    ASSERT_FALSE(layoutCBC.canFitPlaintext(outputBufferSize - 15));

    // Test GCM
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0> layoutGCM(outputBuffer,
                                                                      outputBufferSize);
    ASSERT_TRUE(layoutGCM.canFitPlaintext(outputBufferSize - 24));
    ASSERT_FALSE(layoutGCM.canFitPlaintext(outputBufferSize - 23));
}

TEST(EncryptedMemoryLayout, GetDataSize) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderCBCV0> layoutCBC(outputBuffer,
                                                                      outputBufferSize);
    ASSERT_EQ(outputBufferSize - layoutCBC.getHeaderSize(), layoutCBC.getDataSize());

    // Test GCM
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0> layoutGCM(outputBuffer,
                                                                      outputBufferSize);
    ASSERT_EQ(outputBufferSize - layoutGCM.getHeaderSize(), layoutGCM.getDataSize());
}

TEST(EncryptedMemoryLayout, PlaintextLen) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderCBCV0> layoutCBC(outputBuffer,
                                                                      outputBufferSize);
    std::pair<size_t, size_t> expected{outputBufferSize - layoutCBC.getHeaderSize() -
                                           crypto::aesBlockSize,
                                       outputBufferSize - layoutCBC.getHeaderSize()};
    ASSERT_TRUE(expected == layoutCBC.expectedPlaintextLen());

    // Test GCM
    crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0> layoutGCM(outputBuffer,
                                                                      outputBufferSize);
    expected = {outputBufferSize - layoutGCM.getHeaderSize(),
                outputBufferSize - layoutGCM.getHeaderSize()};
    ASSERT_TRUE(expected == layoutGCM.expectedPlaintextLen());
}

// The following tests are meant to test behavior of SymmetricEncryptorWindows, who grapples with
// some peculiar (but by design) behavior with Windows BCrypt padding incomplete blocks before
// finalize is called. These tests should pass just fine on Windows and non-Windows encryptors.
TEST(SymmetricEncryptor, PaddingLogic) {
    SymmetricKey key = crypto::aesGenerate(crypto::sym256KeySize, "SymmetricEncryptorWindowsTest");
    const std::array<uint8_t, 16> iv = {};
    std::array<std::uint8_t, 1024> cryptoBuffer;
    DataRange cryptoRange(cryptoBuffer.data(), cryptoBuffer.size());
    // This array defines a series of different test cases. Each sub-array defines a series of calls
    // to encryptor->update(). Each number represents the number of bytes to pass to update in a
    // single call
    const std::vector<std::vector<uint8_t>> testData = {
        // Pre-condition: blockBuffer is empty
        {
            // Update is invoked with 1 byte. We should get 0 bytes back
            // Post-condition: 1 byte is in the blockBuffer. 15 more bytes are required to fill it.
            0x01,
            // Update is invoked with 16 bytes
            // One block is written out, 1 byte remains in blockBuffer
            0x10
            // Finalize is then called, filling out the buffer with 15 bytes of padding.
        },
        {
            // Update is invoked with 16 bytes. We should get 16 bytes back
            // Post-condition: 0 bytes in the block buffer, 16 bytes returned
            0x10
            // Finalize is called, providing no padding and encrypting nothing
        },
        {
            // Update is invoked with 32 bytes. We should get 32 bytes back
            // Post-condition: 0 bytes in the block buffer, 16 bytes returned
            0x20
            // Finalize is called, providing no padding and encrypting nothing
        },
        {
            // Update is invoked with 5 bytes. We should get 0 bytes back.
            // Post-condition: 5 bytes in the blockBuffer. 11 more bytes are required to fill it.
            0x05,
            // Update is invoked with 48 bytes.
            // 3 blocks are written out. 5 bytes remain in blockBuffer.
            0x30
            // Finalize is called, filling out the buffer with 11 bytes of padding
        },
        {
            // Update is invoked with 2 bytes. We should get 0 bytes back
            // Post-condition: 2 bytes in the blockBuffer. 14 more bytes are required to fill it.
            0x02,
            // Update is invoked with 7 bytes. We should get 0 bytes back
            // Post-condition: 9 bytes in the blockBuffer. 7 more bytes are required to fill it.
            0x07,
            // Finalize is called, filling out the buffer with 7 bytes of padding.
        },
        {
            // Update is invoked with 11 bytes. We should get 0 bytes back.
            // Post-condition: 11 bytes in the blockBuffer. 5 more bytes are required to fill it.
            0x0B,
            // Finalize is called, filling out the buffer with 5 bytes of padding.
        },
        {
            // Update is invoked with 21 bytes. We should get 16 bytes back.
            // Post-condition: 5 bytes in the blockBuffer. 11 more bytes are required to fill it.
            0x15,
            // Update is invoked with 38 bytes. We should get 32 bytes vback
            // Post-condition: 11 bytes in the blockBuffer. 5 more bytes are required to fill it.
            0x26,
            // Finalize is called, filling out the buffer with 5 bytes of padding.
        },
    };

    // We will loop through all of the test cases, ensuring no fatal errors,
    // and ensuring correct encryption and decryption.
    for (auto& testCase : testData) {
        auto swEnc =
            crypto::SymmetricEncryptor::create(key, crypto::aesMode::cbc, iv.data(), iv.size());
        ASSERT_OK(swEnc.getStatus());
        auto encryptor = std::move(swEnc.getValue());
        uint8_t blockBufferSize = 0;
        DataRangeCursor cryptoCursor(cryptoRange);
        // Make subsequent calls to encryptor->update() as defined by the test data
        for (auto& updateBytes : testCase) {
            std::vector<uint8_t> plainText(updateBytes, updateBytes);
            auto swSize = encryptor->update(plainText.data(),
                                            plainText.size(),
                                            const_cast<uint8_t*>(cryptoCursor.data<uint8_t>()),
                                            cryptoCursor.length());
            ASSERT_OK(swSize);
            cryptoCursor.advance(swSize.getValue());
            size_t totalBytes = updateBytes + blockBufferSize;
            // Assert that number of bytes written is correct
            if (totalBytes < 16) {
                // If we didn't fill a block with the last call to update,
                // ensure nothing was encrypted.
                ASSERT_EQ(swSize.getValue(), 0);
            } else {
                // If we did fill at least one block, ensure the highest available multiple of 16
                // bytes was encrypted
                ASSERT_EQ(swSize.getValue(), (totalBytes - (totalBytes % 16)));
            }
            blockBufferSize = totalBytes % 16;
        }
        auto swSize = encryptor->finalize(const_cast<uint8_t*>(cryptoCursor.data<uint8_t>()),
                                          cryptoCursor.length());
        ASSERT_OK(swSize);
        // finalize is guaranteed to encrypt 16 bytes, as there can never be more than 15 bytes left
        // in the blockBuffer from the last update, and it will always pad to an exact value of 16
        ASSERT_EQ(swSize.getValue(), 16);
    }
}

SymmetricKey aesGeneratePredictableKey256(StringData stringKey, StringData keyId) {
    const size_t keySize = crypto::sym256KeySize;
    ASSERT_EQ(keySize, stringKey.size());

    SecureVector<uint8_t> key(keySize);
    std::copy(stringKey.begin(), stringKey.end(), key->begin());

    return SymmetricKey(std::move(key), crypto::aesAlgorithm, keyId.toString());
}

// Convenience wrappers to avoid line-wraps later.
const std::uint8_t* asUint8(const char* str) {
    return reinterpret_cast<const std::uint8_t*>(str);
};

const char* asChar(const std::uint8_t* data) {
    return reinterpret_cast<const char*>(data);
};

// Positive/Negative test for additional authenticated data GCM encryption.
// Setup encryptor/decryptor with fixed key/iv/aad in order to produce predictable results.
// Check roundtrip and that tag violation triggers failure.
void GCMAdditionalAuthenticatedDataHelper(bool succeed) {
    const auto mode = crypto::aesMode::gcm;
    if (!crypto::getSupportedSymmetricAlgorithms().count(getStringFromCipherMode(mode))) {
        return;
    }

    constexpr auto kKey = "abcdefghijklmnopABCDEFGHIJKLMNOP"_sd;
    SymmetricKey key = aesGeneratePredictableKey256(kKey, "testID");

    constexpr auto kIV = "FOOBARbazqux"_sd;
    std::array<std::uint8_t, 12> iv;
    std::copy(kIV.begin(), kIV.end(), iv.begin());

    auto encryptor =
        uassertStatusOK(crypto::SymmetricEncryptor::create(key, mode, iv.data(), iv.size()));

    constexpr auto kAAD = "Hello World"_sd;
    ASSERT_OK(encryptor->addAuthenticatedData(asUint8(kAAD.rawData()), kAAD.size()));

    constexpr auto kPlaintextMessage = "01234567012345670123456701234567"_sd;
    constexpr auto kBufferSize = kPlaintextMessage.size() + (2 * crypto::aesBlockSize);
    std::array<std::uint8_t, kBufferSize> cipherText;
    auto cipherLen = uassertStatusOK(encryptor->update(asUint8(kPlaintextMessage.rawData()),
                                                       kPlaintextMessage.size(),
                                                       cipherText.data(),
                                                       cipherText.size()));
    cipherLen += uassertStatusOK(
        encryptor->finalize(cipherText.data() + cipherLen, cipherText.size() - cipherLen));

    constexpr auto kExpectedCipherText =
        "\xF1\x87\x38\x92\xA3\x0E\x77\x27\x92\xB1\x3B\xA6\x27\xB5\xF5\x2B"
        "\xA0\x16\xCC\xB8\x88\x54\xC0\x06\x6E\x36\xCF\x3B\xB0\x8B\xF5\x11";
    ASSERT_EQ(StringData(asChar(cipherText.data()), cipherLen), kExpectedCipherText);

    std::array<std::uint8_t, 12> tag;
    const auto taglen = uassertStatusOK(encryptor->finalizeTag(tag.data(), tag.size()));

    constexpr auto kExpectedTag = "\xF9\xD6\xF9\x63\x21\x93\xE8\x5C\x42\xAA\x5E\x02"_sd;
    ASSERT_EQ(StringData(asChar(tag.data()), taglen), kExpectedTag);

    auto decryptor =
        uassertStatusOK(crypto::SymmetricDecryptor::create(key, mode, iv.data(), iv.size()));
    ASSERT_OK(decryptor->addAuthenticatedData(asUint8(kAAD.rawData()), kAAD.size()));

    std::array<std::uint8_t, kBufferSize> plainText;
    auto plainLen = uassertStatusOK(
        decryptor->update(cipherText.data(), cipherLen, plainText.data(), plainText.size()));

    if (!succeed) {
        // Corrupt the authenticated tag, which should cause a failure below.
        ++tag[0];
    }

    ASSERT_OK(decryptor->updateTag(tag.data(), tag.size()));
    auto swFinalize = decryptor->finalize(plainText.data() + plainLen, plainText.size() - plainLen);

    if (!succeed) {
        ASSERT_NOT_OK(swFinalize.getStatus());
        return;
    }

    ASSERT_OK(swFinalize.getStatus());
    plainLen += swFinalize.getValue();

    ASSERT_EQ(StringData(asChar(plainText.data()), plainLen), kPlaintextMessage);
}

TEST(AES, GCMAdditionalAuthenticatedData) {
    GCMAdditionalAuthenticatedDataHelper(true);
    GCMAdditionalAuthenticatedDataHelper(false);
}

}  // namespace
}  // namespace mongo
