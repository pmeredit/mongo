/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <arpa/inet.h>

#include "mongo/crypto/symmetric_key.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/kafka_connect_auth_callback.h"
#include "streams/exec/tests/test_utils.h"

using namespace mongo;

namespace streams {

class KafkaConnectAuthCallbackTest : public unittest::Test {
protected:
    mongo::SymmetricKey checkBuildKey(std::string key);
    auto makeContext() {
        auto context = std::make_unique<Context>();
        context->streamProcessorId = UUID::gen().toString();
        context->tenantId = UUID::gen().toString();

        return context;
    }
};

mongo::SymmetricKey KafkaConnectAuthCallbackTest::checkBuildKey(std::string key) {
    auto context = makeContext();
    const std::string initKey{"thiskeyisunusedforthistest123456"};
    KafkaConnectAuthCallback kcac{context.get(), "testOperator", initKey, 60};

    return kcac.buildKey(key);
}

TEST_F(KafkaConnectAuthCallbackTest, EnsureBuildKeySuccessful) {
    // Wrapper to test private buildKey method.
    auto bk = checkBuildKey("aabbccddeeffgghhiijjkkllmmnnoopp");
    ASSERT_EQUALS(bk.getKeySize(), 32);     // 32 byte key
    ASSERT_EQUALS(bk.getAlgorithm(), 0x1);  // AES
}

TEST_F(KafkaConnectAuthCallbackTest, EnsureBuildHeyKeySuccessful) {
    // Wrapper to test private buildKey method.
    auto bk = checkBuildKey("6162636465666768696a6b6c6d6e6f704142434445464748494a4b4c4d4e4f50");
    ASSERT_EQUALS(bk.getKeySize(), 32);     // 32 byte key
    ASSERT_EQUALS(bk.getAlgorithm(), 0x1);  // AES
}

DEATH_TEST_F(KafkaConnectAuthCallbackTest,
             EnsureInvalidKeyThrows,
             "Encryption key size is invalid") {
    // Wrapper to test private buildKey method.
    // Invalid key length for any key strategy.
    ASSERT_THROWS(checkBuildKey("aabbccddeeffgghhiijjkkllmmnnooppNO"), mongo::DBException);
}

TEST_F(KafkaConnectAuthCallbackTest, EnsureInvalidHexKeyThrows) {
    // Wrapper to test private buildKey method.
    // This is a key that's the correct number of characters, but not actually hex, so should throw
    // an exception.
    ASSERT_THROWS(checkBuildKey("X162636465666768696a6b6c6d6e6f704142434445464748494a4b4c4d4e4f50"),
                  mongo::DBException);
}

}  // namespace streams
