/**
 * Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
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

}  // namespace
}  // namespace mongo
