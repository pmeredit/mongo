/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "fle_test_fixture.h"

#include "mongo/bson/json.h"

namespace mongo {

void FLETestFixture::setUp() {
    kDefaultSsnSchema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [{'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "string"
                    }
                }
            }
        })");

    kDefaultNestedSchema = fromjson(R"({
            type: "object",
            properties: {
                user: {
                    type: "object",
                    properties: {
                        ssn: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                                bsonType: "string"
                            }
                        }
                    }
                }
            }
        })");

    kAllEncryptedSchema = fromjson(R"({
            type: "object",
            properties: {},
            additionalProperties: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                    bsonType: "string"
                }
            }
        })");
}

}  // namespace mongo
