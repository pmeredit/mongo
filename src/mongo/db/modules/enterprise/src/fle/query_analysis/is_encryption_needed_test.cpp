/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

TEST(CommandsTest, IsEncryptedNotPresent) {
    auto input = BSON("properties" << BSON("foo" << BSONObj()));
}

TEST(CommandsTest, isEncryptionNeededEmptyEncrypt) {
    uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
    auto encryptObj = BSON("keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID)) << "algorithm"
                                   << "AEAD_AES_256_CBC_HMAC_SHA_512-Random");
    auto input = BSON("properties" << BSON("a" << BSON("encrypt" << encryptObj)) << "type"
                                   << "object");
    ASSERT_TRUE(isEncryptionNeeded(input));
}

TEST(CommandsTest, isEncryptionNeededDeepEncrypt) {
    uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
    auto encryptObj = BSON("keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID)) << "algorithm"
                                   << "AEAD_AES_256_CBC_HMAC_SHA_512-Random");
    auto input =
        BSON("properties" << BSON("a" << BSON("type"
                                              << "object"
                                              << "properties"
                                              << BSON("b" << BSON("encrypt" << encryptObj)))
                                      << "c"
                                      << BSONObj())
                          << "type"
                          << "object");

    ASSERT_TRUE(isEncryptionNeeded(input));
}

}  // namespace
}  // namespace mongo
