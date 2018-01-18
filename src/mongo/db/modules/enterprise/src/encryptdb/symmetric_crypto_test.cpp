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

#include "symmetric_crypto.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "symmetric_crypto_smoke.h"

namespace mongo {
namespace {

const size_t maxPTSize = (4 + 1) * crypto::aesBlockSize;
const size_t outputBufferSize = 3 * crypto::aesBlockSize + maxPTSize;

TEST(AES, CBCTestVectors) {
    ASSERT_OK(crypto::smokeTestAESCipherMode("AES256-CBC"));
}

#ifndef DISABLE_GCM_TESTVECTORS
TEST(AES, GCMTestVectors) {
    ASSERT_OK(crypto::smokeTestAESCipherMode("AES256-GCM"));
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

}  // namespace
}  // namespace mongo
