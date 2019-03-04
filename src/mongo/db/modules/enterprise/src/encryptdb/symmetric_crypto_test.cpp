/**
 *    Copyright (C) 2015 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
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
    ASSERT_OK(crypto::smokeTestAESCipherMode("AES256-CBC"));
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
                                  plaintext.data(),
                                  plaintext.size(),
                                  cryptoBuffer.data(),
                                  cryptoBuffer.size(),
                                  &cryptoLen);
    }

    Status decrypt(crypto::aesMode mode) {
        return crypto::aesDecrypt(key,
                                  mode,
                                  cryptoBuffer.data(),
                                  cryptoLen,
                                  plainBuffer.data(),
                                  plainBuffer.size(),
                                  &plainLen);
    }

protected:
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
    ASSERT_TRUE(std::equal(plaintext.begin(),
                           plaintext.end(),
                           plainBuffer.begin(),
                           plainBuffer.begin() + plaintext.size()));
}

DEATH_TEST_F(AESRoundTrip,
             CBCReadOnlyCanDecrypt,
             "Invariant failure !encryptionGlobalParams.readOnlyMode") {
    ASSERT_OK(encrypt(crypto::aesMode::cbc));

    encryptionGlobalParams.readOnlyMode = true;
    auto guard = makeGuard([]() { encryptionGlobalParams.readOnlyMode = false; });

    ASSERT_OK(decrypt(crypto::aesMode::cbc));

    ASSERT_TRUE(std::equal(plaintext.begin(),
                           plaintext.end(),
                           plainBuffer.begin(),
                           plainBuffer.begin() + plaintext.size()));

    encrypt(crypto::aesMode::cbc).ignore();
}

#ifndef DISABLE_GCM_TESTVECTORS
TEST(AES, GCMTestVectors) {
    ASSERT_OK(crypto::smokeTestAESCipherMode("AES256-GCM"));
}

TEST_F(AESRoundTrip, GCM) {
    if (!modeSupported(crypto::aesMode::gcm)) {
        return;
    }
    ASSERT_OK(encrypt(crypto::aesMode::gcm));
    ASSERT_OK(decrypt(crypto::aesMode::gcm));
    ASSERT_TRUE(std::equal(plaintext.begin(),
                           plaintext.end(),
                           plainBuffer.begin(),
                           plainBuffer.begin() + plaintext.size()));
}

DEATH_TEST_F(AESRoundTrip,
             GCMReadOnlyCanDecrypt,
             "Invariant failure !encryptionGlobalParams.readOnlyMode") {
    ASSERT_OK(encrypt(crypto::aesMode::gcm));

    encryptionGlobalParams.readOnlyMode = true;
    auto guard = makeGuard([]() { encryptionGlobalParams.readOnlyMode = false; });

    ASSERT_OK(decrypt(crypto::aesMode::gcm));

    ASSERT_TRUE(std::equal(plaintext.begin(),
                           plaintext.end(),
                           plainBuffer.begin(),
                           plainBuffer.begin() + plaintext.size()));

    encrypt(crypto::aesMode::gcm).ignore();
}
#endif


TEST(EncryptedMemoryLayout, CanCreateMemoryLayoutWithCBC) {
    uint8_t outputBuffer[outputBufferSize];
    crypto::ConstEncryptedMemoryLayout layout(crypto::aesMode::cbc, outputBuffer, outputBufferSize);
}

TEST(EncryptedMemoryLayout, CanCreateMemoryLayoutWithGCM) {
    uint8_t outputBuffer[outputBufferSize];
    crypto::ConstEncryptedMemoryLayout layout(crypto::aesMode::gcm, outputBuffer, outputBufferSize);
}

DEATH_TEST(EncryptedMemoryLayout, CannotCreateMemoryLayoutWithInvalid, "Fatal Assertion 4052") {
    uint8_t outputBuffer[outputBufferSize];
    // Note that this type of cast should never be performed normally
    crypto::ConstEncryptedMemoryLayout layout(
        (crypto::aesMode)(255), outputBuffer, outputBufferSize);
}

DEATH_TEST(EncryptedMemoryLayout, CannotCreateMemoryLayoutOnNullptr, "invariant") {
    crypto::ConstEncryptedMemoryLayout layout(crypto::aesMode::gcm, nullptr, outputBufferSize);
}

TEST(EncryptedMemoryLayout, CiphertexLen) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    size_t expected = 16;
    crypto::ConstEncryptedMemoryLayout layoutCBC(
        crypto::aesMode::cbc, outputBuffer, outputBufferSize);
    ASSERT_EQ(expected, layoutCBC.expectedCiphertextLen(10));

    // Test GCM
    expected = 10;
    crypto::ConstEncryptedMemoryLayout layoutGCM(
        crypto::aesMode::gcm, outputBuffer, outputBufferSize);
    ASSERT_EQ(expected, layoutGCM.expectedCiphertextLen(10));
}

TEST(EncryptedMemoryLayout, CanFitPlaintText) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    crypto::ConstEncryptedMemoryLayout layoutCBC(
        crypto::aesMode::cbc, outputBuffer, outputBufferSize);
    ASSERT_TRUE(layoutCBC.canFitPlaintext(outputBufferSize - 32));
    ASSERT_FALSE(layoutCBC.canFitPlaintext(outputBufferSize - 15));

    // Test GCM
    crypto::ConstEncryptedMemoryLayout layoutGCM(
        crypto::aesMode::gcm, outputBuffer, outputBufferSize);
    ASSERT_TRUE(layoutGCM.canFitPlaintext(outputBufferSize - 24));
    ASSERT_FALSE(layoutGCM.canFitPlaintext(outputBufferSize - 23));
}

TEST(EncryptedMemoryLayout, GetDataSize) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    crypto::ConstEncryptedMemoryLayout layoutCBC(
        crypto::aesMode::cbc, outputBuffer, outputBufferSize);
    ASSERT_EQ(outputBufferSize - layoutCBC.getHeaderSize(), layoutCBC.getDataSize());

    // Test GCM
    crypto::ConstEncryptedMemoryLayout layoutGCM(
        crypto::aesMode::gcm, outputBuffer, outputBufferSize);
    ASSERT_EQ(outputBufferSize - layoutGCM.getHeaderSize(), layoutGCM.getDataSize());
}

TEST(EncryptedMemoryLayout, PlaintextLen) {
    uint8_t outputBuffer[outputBufferSize];

    // Test CBC
    crypto::ConstEncryptedMemoryLayout layoutCBC(
        crypto::aesMode::cbc, outputBuffer, outputBufferSize);
    std::pair<size_t, size_t> expected{outputBufferSize - layoutCBC.getHeaderSize() -
                                           crypto::aesBlockSize,
                                       outputBufferSize - layoutCBC.getHeaderSize()};
    ASSERT_TRUE(expected == layoutCBC.expectedPlaintextLen());

    // Test GCM
    crypto::ConstEncryptedMemoryLayout layoutGCM(
        crypto::aesMode::gcm, outputBuffer, outputBufferSize);
    expected = {outputBufferSize - layoutGCM.getHeaderSize(),
                outputBufferSize - layoutGCM.getHeaderSize()};
    ASSERT_TRUE(expected == layoutGCM.expectedPlaintextLen());
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
