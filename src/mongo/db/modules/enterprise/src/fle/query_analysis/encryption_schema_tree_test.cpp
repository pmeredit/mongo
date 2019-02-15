/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "encryption_schema_tree.h"

#include "mongo/bson/json.h"
#include "mongo/db/bson/bson_helper.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {

const EncryptionMetadata kDefaultMetadata = EncryptionMetadata{};

/**
 * Parses 'schema' into an encryption schema tree and returns the EncryptionMetadata for
 * 'path'.
 */
EncryptionMetadata extractMetadata(BSONObj schema, std::string path) {
    auto result = EncryptionSchemaTreeNode::parse(schema);
    auto metadata = result->getEncryptionMetadataForPath(FieldRef(path));
    ASSERT(metadata);
    return metadata.get();
}

/**
 * Parses 'schema' into an encryption schema tree and verifies that 'path' is not encrypted.
 */
void assertNotEncrypted(BSONObj schema, std::string path) {
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(result->getEncryptionMetadataForPath(FieldRef(path)));
}

TEST(EncryptionSchemaTreeTest, MarksTopLevelFieldAsEncrypted) {
    BSONObj schema =
        fromjson(R"({type: "object", properties: {ssn: {encrypt: {}}, name: {type: "string"}}})");
    ASSERT(extractMetadata(schema, "ssn") == kDefaultMetadata);
    assertNotEncrypted(schema, "ssn.nonexistent");
    assertNotEncrypted(schema, "name");
}

TEST(EncryptionSchemaTreeTest, MarksNestedFieldsAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: "object", 
                properties: {
                    ssn: {encrypt: {}}
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(extractMetadata(schema, "user.ssn") == kDefaultMetadata);
    assertNotEncrypted(schema, "user");
    assertNotEncrypted(schema, "user.name");
}

TEST(EncryptionSchemaTreeTest, MarksNumericFieldNameAsEncrypted) {
    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("0" << BSON("encrypt" << BSONObj())));
    ASSERT(extractMetadata(schema, "0") == kDefaultMetadata);

    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("nested" << BSON("type"
                                           << "object"
                                           << "properties"
                                           << BSON("0" << BSON("encrypt" << BSONObj())))));
    ASSERT(extractMetadata(schema, "nested.0") == kDefaultMetadata);
}

TEST(EncryptionSchemaTreeTest, MarksMultipleFieldsAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {            
            ssn: {encrypt: {}},
            accountNumber: {encrypt: {}},
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "ssn") == kDefaultMetadata);
    ASSERT(extractMetadata(schema, "accountNumber") == kDefaultMetadata);
    ASSERT(extractMetadata(schema, "super.secret") == kDefaultMetadata);
    assertNotEncrypted(schema, "super");
}

TEST(EncryptionSchemaTreeTest, TopLevelEncryptMarksEmptyPathAsEncrypted) {
    ASSERT(extractMetadata(fromjson("{encrypt: {}}"), "") == kDefaultMetadata);
}

TEST(EncryptionSchemaTreeTest, ExtractsCorrectMetadataOptions) {
    const IDLParserErrorContext encryptCtxt("encrypt");
    auto metadataObj = BSON("algorithm"
                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                            << "initializationVector"
                            << BSONBinData(NULL, 0, BinDataType::BinDataGeneral)
                            << "keyId"
                            << BSON_ARRAY(UUID::gen()));

    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("ssn" << BSON("encrypt" << metadataObj)));
    ASSERT(extractMetadata(schema, "ssn") != kDefaultMetadata);
    ASSERT(extractMetadata(schema, "ssn") == EncryptionMetadata::parse(encryptCtxt, metadataObj));
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptAlongsideAnotherTypeKeyword) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                type: "object"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                bsonType: "BinData"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptWithSiblingKeywords) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                properties: {invalid: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                minimum: 5,
                items: {}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfConflictingEncryptKeywords) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                properties: {
                    invalid: {encrypt: {}}
                },
                items: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinItems) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                type: "array", 
                items: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: "object",
                properties: {
                    ssn: {
                        type: "array", 
                        items: {encrypt: {}}
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinAdditionalItems) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                type: "array", 
                items: {},
                additionalItems: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                additionalItems: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsNotTypeRestricted) {
    BSONObj schema = fromjson(R"({properties: {ssn: {encrypt: {}}}})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                properties: {
                    ssn: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsTypeRestrictedWithMultipleTypes) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: ["object", "array"],
                properties: {
                    ssn: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidSchema) {
    BSONObj schema = fromjson(R"({properties: "invalid"})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: "invalid"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseUnknownFieldInEncrypt) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {unknownField: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 40415);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidAlgorithm) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-SomeInvalidAlgo"}
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::BadValue);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidIV) {
    BSONObj schema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("ssn" << BSON("encrypt" << BSON("initializationVector" << BSONBinData(
                                                         nullptr, 0, BinDataType::MD5Type)))));
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidKeyIdUUID) {
    BSONObj schema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("ssn" << BSON("encrypt" << BSON("keyId" << BSON_ARRAY(BSONBinData(
                                                         nullptr, 0, BinDataType::MD5Type))))));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51084);
}

}  // namespace
}  // namespace mongo
