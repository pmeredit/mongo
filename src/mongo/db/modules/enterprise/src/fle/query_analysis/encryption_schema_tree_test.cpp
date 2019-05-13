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
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {

/**
 * Parses 'schema' into an encryption schema tree and returns the EncryptionMetadata for
 * 'path'.
 */
ResolvedEncryptionInfo extractMetadata(BSONObj schema, std::string path) {
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
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    BSONObj schema =
        fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                    }
                },
                name: {
                    type: "string"
                }
            }
        })");
    ASSERT(extractMetadata(schema, "ssn") == metadata);
    assertNotEncrypted(schema, "name");
}

TEST(EncryptionSchemaTreeTest, MarksNestedFieldsAsEncrypted) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    BSONObj schema =
        fromjson(R"({
        type: "object",
        properties: {
            user: {
                type: "object",
                properties: {
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                        }
                    }
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(extractMetadata(schema, "user.ssn") == metadata);
    assertNotEncrypted(schema, "user");
    assertNotEncrypted(schema, "user.name");
}

TEST(EncryptionSchemaTreeTest, MarksNumericFieldNameAsEncrypted) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};
    BSONObj encryptObj = fromjson(R"({
        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
        keyId: [)" + uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) +
                                  R"(]})");

    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("0" << BSON("encrypt" << encryptObj)));
    ASSERT(extractMetadata(schema, "0") == metadata);

    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("nested" << BSON("type"
                                           << "object"
                                           << "properties"
                                           << BSON("0" << BSON("encrypt" << encryptObj)))));
    ASSERT(extractMetadata(schema, "nested.0") == metadata);
}

TEST(EncryptionSchemaTreeTest, MarksMultipleFieldsAsEncrypted) {
    const auto uuid = UUID::gen();
    const auto uuidStr = uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [)" +
                              uuidStr + R"(]
                }
            },
            accountNumber: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [)" +
                              uuidStr + R"(]
                }
            },
            super: {
                type: "object",
                properties: {
                    secret: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [)" +
                              uuidStr + R"(]
                        }
                    }
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "ssn") == metadata);
    ASSERT(extractMetadata(schema, "accountNumber") == metadata);
    ASSERT(extractMetadata(schema, "super.secret") == metadata);
    assertNotEncrypted(schema, "super");
}

TEST(EncryptionSchemaTreeTest, TopLevelEncryptMarksEmptyPathAsEncrypted) {
    const auto uuid = UUID::gen();
    const auto uuidStr = uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    BSONObj schema = fromjson(R"({
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [)" +
                              uuidStr + R"(]
                }})");

    ASSERT(extractMetadata(schema, "") == metadata);
}

TEST(EncryptionSchemaTreeTest, ExtractsCorrectMetadataOptions) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo ssnMetadata{EncryptSchemaKeyId{std::vector<UUID>{uuid}},
                                       FleAlgorithmEnum::kDeterministic,
                                       MatcherTypeSet{BSONType::String}};

    const IDLParserErrorContext encryptCtxt("encrypt");
    auto encryptObj = BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "bsonType"
                           << "string"
                           << "keyId"
                           << BSON_ARRAY(uuid));
    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("ssn" << BSON("encrypt" << encryptObj)));
    ASSERT(extractMetadata(schema, "ssn") == ssnMetadata);
}

TEST(EncryptionSchemaTreeTest, ThrowsAnErrorIfPathContainsEncryptedPrefix) {
    BSONObj schema = fromjson(R"({type: "object", properties: {ssn: {encrypt: {}}}})");
    ASSERT_THROWS_CODE(extractMetadata(schema, "ssn.nonexistent"), AssertionException, 51099);
}

TEST(EncryptionSchemaTreeTest, ReturnsNotEncryptedForPathWithNonEncryptedPrefix) {
    BSONObj schema = fromjson(R"({type: "object", properties: {ssn: {}}})");
    assertNotEncrypted(schema, "ssn.nonexistent");

    schema = fromjson(R"({type: "object", properties: {blah: {}}, additionalProperties: {}})");
    assertNotEncrypted(schema, "additional.prop.path");
}

TEST(EncryptionSchemaTreeTest, ThrowsAnErrorIfPathContainsPrefixToEncryptedAdditionalProperties) {
    BSONObj schema = fromjson(
        R"({type: "object", properties: {blah: {}}, additionalProperties: {encrypt: {}}})");
    ASSERT_THROWS_CODE(extractMetadata(schema, "path.extends.encrypt"), AssertionException, 51099);
}

TEST(EncryptionSchemaTreeTest,
     ThrowsAnErrorIfPathContainsPrefixToNestedEncryptedAdditionalProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            blah: {
                type: "object",
                properties: {
                    foo: {}
                },
                additionalProperties: {encrypt: {}}
            }
        }
    })");
    ASSERT_THROWS_CODE(extractMetadata(schema, "blah.not.foo"), AssertionException, 51099);
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptAlongsideAnotherTypeKeyword) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
                type: "object"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
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
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
                properties: {invalid: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
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
                    invalid: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                },
                items: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
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
                items: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
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
                        items: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                            }
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptMetadataWithinItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                type: "array",
                items: {
                    encryptMetadata: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinAdditionalItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                type: "array",
                items: {},
                additionalItems: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                additionalItems: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptMetadataWithinAdditionalItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                type: "array",
                items: {},
                additionalItems: {
                    encryptMetadata: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsNotTypeRestricted) {
    BSONObj schema = fromjson(R"({
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object",
        properties: {
            user: {
                properties: {
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
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
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPropertyHasDot) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            "dotted.field" : {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfParentPropertyHasDot) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            "dotted.parent.field" : {
                type: "object",
                properties: {
                    secret: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfDottedPropertyNestedInPatternProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            "abc" : {
                type: "object",
                properties: {
                    "d.e" : {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfDottedPropertyNestedInAdditionalProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            type: "object",
            properties: {
                "a.c" : {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, AllowDottedNonEncryptProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            "dotted.non.encrypt" : {
                type: "object"
            },
            secret: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    assertNotEncrypted(schema, "dotted.non.encrypt");
    ASSERT(result->getEncryptionMetadataForPath(FieldRef{"secret"}));
}

TEST(EncryptionSchemaTreeTest, DottedPropertiesNotEncryptedWithMatchingPatternProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            "a.c" : {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    assertNotEncrypted(schema, "a.c");
    ASSERT(result->getEncryptionMetadataForPath(FieldRef{"abc"}));
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
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
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
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-SomeInvalidAlgo",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::BadValue);
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

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptBelowAdditionalPropertiesWithoutTypeObject) {
    BSONObj schema = fromjson(R"({
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfIllegalSubschemaUnderAdditionalProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            },
            illegal: 1
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfAdditionalPropertiesIsWrongType) {
    BSONObj schema = fromjson("{additionalProperties: [{type: 'string'}]}");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfAdditionalPropertiesWithEncryptInsideItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            arr: {
                type: "array",
                items: {
                    type: "object",
                    additionalProperties: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesAllPropertiesCorrectlyReportedAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }
    })");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz"}));
}

TEST(EncryptionSchemaTreeTest,
     NestedAdditionalPropertiesAllPropertiesCorrectlyReportedAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            obj: {
                type: "object",
                additionalProperties: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.baz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other.foo"}));
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesOnlyAppliesToFieldsNotNamedByProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            obj: {
                type: "object",
                properties: {
                    a: {type: "string"},
                    b: {type: "object"},
                    c: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                },
                additionalProperties: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.baz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.b"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.b.c"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.c"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other.foo"}));
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesWorksWithNestedPropertiesSubschema) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            type: "object",
            properties: {
                a: {type: "string"},
                b: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.b"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar.b"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar.a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar.c"}));
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesWorksWithNestedAdditionalPropertiesSubschema) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            type: "object",
            additionalProperties: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.baz"}));
}

TEST(EncryptionSchemaTreeTest, CanSuccessfullyParseAdditionalItemsWhenBoolean) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            arr: {
                type: "array",
                additionalItems: true
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"arr"}));
}

TEST(EncryptionSchemaTreeTest, CanSuccessfullyParseAdditionalPropertiesWhenBoolean) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            obj: {
                type: "object",
                additionalProperties: false
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.bar"}));
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataWithOverriding) {
    const auto uuid = UUID::gen();

    ResolvedEncryptionInfo secretMetadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    ResolvedEncryptionInfo ssnMetadata{EncryptSchemaKeyId{{uuid}},
                                       FleAlgorithmEnum::kDeterministic,
                                       MatcherTypeSet{BSONType::String}};

    BSONObj schema =
        fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
            keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
        },
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    bsonType: "string"
                }
            },
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "ssn") == ssnMetadata);
    ASSERT(extractMetadata(schema, "super.secret") == secretMetadata);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMultipleLevels) {
    const auto uuid1 = UUID::gen();
    const auto uuid2 = UUID::gen();

    ResolvedEncryptionInfo secretMetadata{EncryptSchemaKeyId{{uuid1}},
                                          FleAlgorithmEnum::kDeterministic,
                                          MatcherTypeSet{BSONType::String}};

    ResolvedEncryptionInfo mysteryMetadata{
        EncryptSchemaKeyId{{uuid2}}, FleAlgorithmEnum::kRandom, boost::none};

    BSONObj schema =
        fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
        },
        type: "object",
        properties: {
            super: {
                encryptMetadata: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [)" +
                 uuid1.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                },
                type: "object",
                properties: {
                    secret: {encrypt: {bsonType: "string"}}
                }
            },
            duper: {
                type: "object",
                properties: {
                    mystery: {
                        encrypt: {
                            keyId: [)" +
                 uuid2.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                        }
                    }
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "super.secret") == secretMetadata);
    ASSERT(extractMetadata(schema, "duper.mystery") == mysteryMetadata);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMissingAlgorithm) {
    const auto uuid = UUID::gen();

    BSONObj schema =
        fromjson(R"({
        encryptMetadata: {
            keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
        },
        type: "object",
        properties: {
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51099);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMissingKeyId) {
    BSONObj schema = fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
        },
        type: "object",
        properties: {
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51097);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPatternPropertiesIsNotAnObject) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: true
    })");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPatternPropertiesHasNonObjectProperty) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: true
        }
    })");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPatternPropertiesHasAnIllFormedRegex) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            "(": {}
        }
    })");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::BadValue);
}

TEST(EncryptionSchemaTreeTest, LegalPatternPropertiesParsesSuccessfully) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {},
            bar: {}
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.quux"}));
}

TEST(EncryptionSchemaTreeTest, FieldNamesMatchingPatternPropertiesReportEncryptedMetadata) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.quux"}),
                       AssertionException,
                       51102);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"a_foo_b"}));
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"a_foo_b.quux"}),
                       AssertionException,
                       51102);
}

TEST(EncryptionSchemaTreeTest,
     PatternPropertiesWorksTogetherWithPropertiesAndAdditionalProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            },
            b: {type: "number"}
        },
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            },
            bar: {type: "number"}
        },
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"b"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo2"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"3foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar2"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"3bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"x"}));
    ASSERT_THROWS_CODE(
        encryptionTree->getEncryptionMetadataForPath(FieldRef{"y.z.w"}), AssertionException, 51102);
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesErrorIfMultiplePatternsMatchButOnlyOneEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            },
            bar: {type: "number"}
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}),
                       AssertionException,
                       51142);
}

TEST(EncryptionSchemaTreeTest,
     PatternPropertiesErrorIfMultiplePatternsMatchWithDifferentEncryptionOptions) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            },
            bar: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{"$binary": "0PCfrZlWRWeIe4ZbJ1IODQ==", "$type": "04"}]
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}),
                       AssertionException,
                       51142);
}

TEST(EncryptionSchemaTreeTest,
     PatternPropertiesEncryptIfMultiplePatternsMatchWithSameEncryptionOptions) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            },
            bar: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}));
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesErrorIfInconsistentWithPropertiesOnlyOneEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foobar: {type: "string"}
        },
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}),
                       AssertionException,
                       51142);
}

TEST(EncryptionSchemaTreeTest,
     PatternPropertiesErrorIfInconsistentWithPropertiesDifferentEncryptionOptions) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foobar: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{"$binary": "0PCfrZlWRWeIe4ZbJ1IODQ==", "$type": "04"}]
                }
            }
        },
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}),
                       AssertionException,
                       51142);
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesSuccessIfAlsoInPropertiesButSameEncryptionOptions) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foobar: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        },
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}));
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesFailsIfTypeObjectNotSpecified) {
    BSONObj schema = fromjson(R"({
        patternProperties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, NestedPatternPropertiesFailsIfTypeObjectNotSpecified) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                patternProperties: {
                    bar: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");

    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesNestedBelowProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                type: "object",
                patternProperties: {
                    bar: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz.x.y"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.foobar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.bar2"}));
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesNestedBelowPatternProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {
                type: "object",
                patternProperties: {
                    bar: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz.x.y"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.foobar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.bar2"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foobar.foobar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo2.bar3"}));
}

TEST(EncryptionSchemaTreeTest, FourMatchingEncryptSpecifiersSucceedsIfAllEncryptOptionsSame) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            a: {
                type: "object",
                patternProperties: {
                    x: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    },
                    y: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            },
            b: {
                type: "object",
                patternProperties: {
                    w: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    },
                    z: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"aa.xx"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"aa.yy"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"aa.xy"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bb.ww"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bb.zz"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"ab.xyzw"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"i.j"}));
}

TEST(EncryptionSchemaTreeTest, FourMatchingEncryptSpecifiersFailsIfOneEncryptOptionsDiffers) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            a: {
                type: "object",
                patternProperties: {
                    x: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    },
                    y: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            },
            b: {
                type: "object",
                patternProperties: {
                    w: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    },
                    z: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{"$binary": "0PCfrZlWRWeIe4ZbJ1IODQ==", "$type": "04"}]
                        }
                    }
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"aa.xx"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"aa.yy"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"aa.xy"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bb.ww"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bb.zz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"i.j"}));
    ASSERT_THROWS_CODE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"ab.xyzw"}),
                       AssertionException,
                       51142);
}

TEST(EncryptionSchemaTreeTest,
     AdditionalPropertiesWithInconsistentEncryptOptionsNotConsideredIfPatternMatches) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        },
        patternProperties: {
            b: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        },
        additionalProperties: {type: "string"}
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"ab"}));
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesInheritsParentEncryptMetadata) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo expectedMetadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    auto uuidJsonStr = uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false);
    BSONObj schema = fromjson(R"({
        type: "object",
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
            keyId: [)" + uuidJsonStr +
                              R"(]
        },
        properties: {
            a: {
                type: "object",
                patternProperties: {
                    b: {encrypt: {}}
                }
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    auto foundMetadata = encryptionTree->getEncryptionMetadataForPath(FieldRef{"a.bb"});
    ASSERT(foundMetadata == expectedMetadata);
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsTrueIfPropertyContainsEncryptedNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    ASSERT(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsTrueIfNestedPropertyContainsEncryptedNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                type: "object",
                properties: {
                    b: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");

    ASSERT(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsFalseIfPropertyDoesNotContainEncryptedNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        }
    })");

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}


TEST(EncryptionSchemaTreeTest,
     ContainsEncryptReturnsFalseIfPropertyWithNestedPropertyDoesNotContainEncryptedNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                type: "object", 
                properties: {
                    b: {}
                }
            }
        }
    })");

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsTrueIfAdditionalPropertiesHasEncryptNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        },
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
            }
        }
    })");

    ASSERT_TRUE(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest,
     ContainsEncryptReturnsFalseIfAdditionalPropertiesDoesNotHaveEncryptNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        },
        additionalProperties: {
            type: "number"
        }
    })");

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsTrueIfPatternPropertiesHasEncryptNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        },
        patternProperties: {
            "^b": {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    ASSERT_TRUE(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, EncryptedBelowPrefixReturnsTrueOnShortPrefix) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                type: "object",
                properties: {
                    b: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }}
                    }
            }
        }
    })");
    ASSERT_TRUE(
        EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNodeBelowPrefix(FieldRef("a")));
}

TEST(EncryptionSchemaTreeTest, EncryptedBelowPrefixReturnsFalseOnMissingPath) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                type: "object",
                properties: {
                    b: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }}
                    }
            }
        }
    })");
    ASSERT_FALSE(
        EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNodeBelowPrefix(FieldRef("c")));
}


DEATH_TEST(EncryptionSchemaTreeTest, EncryptedBelowPrefixInvariantOnEncryptedPath, "invariant") {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");

    EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNodeBelowPrefix(FieldRef("foo"));
}

TEST(EncryptionSchemaTreeTest,
     ContainsEncryptReturnsFalseIfPatternPropertiesDoesNotHaveEncryptNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        },
        patternProperties: {
            "^b": {
                type: "number"
            }
        }
    })");

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema)->containsEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, EncryptedBelowPrefixReturnsTrueOnLongPrefix) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                type: "object",
                properties: {
                    b: {
                        type: "object",
                        properties: {
                            c: {
                                type: "object",
                                properties: {
                                    d: {
                                        encrypt: {
                                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                        keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(schemaTree->containsEncryptedNodeBelowPrefix(FieldRef("a")));
    ASSERT_TRUE(schemaTree->containsEncryptedNodeBelowPrefix(FieldRef("a.b")));
    ASSERT_TRUE(schemaTree->containsEncryptedNodeBelowPrefix(FieldRef("a.b.c")));
}

TEST(EncryptionSchemaTreeTest, ParseFailsIfDeterministicAndNoBSONType) {
    const auto uuid = UUID::gen();
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "keyId"
                                             << BSON_ARRAY(uuid)));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << encryptObj));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, ParseFailsIfDeterministicAndMultipleBSONType) {
    const auto uuid = UUID::gen();
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "bsonType"
                                             << BSON_ARRAY("string"
                                                           << "int")
                                             << "keyId"
                                             << BSON_ARRAY(uuid)));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << encryptObj));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, ParseSucceedsIfLengthOneTypeArray) {
    const auto uuid = UUID::gen();
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "bsonType"
                                             << BSON_ARRAY("string")
                                             << "keyId"
                                             << BSON_ARRAY(uuid)));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << encryptObj));
    // Just making sure the parse succeeds, don't care about the result.
    EncryptionSchemaTreeNode::parse(schema);
}

TEST(EncryptionSchemaTreeTest, AllOfKeywordSubschemaWithoutEncryptSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                allOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            baz: {bsonType: "int"}
                        }
                    }
                ]
            }
        }
    })");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.bar")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.baz")));
}

TEST(EncryptionSchemaTreeTest, AllOfKeywordSubschemaWithEncryptFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                allOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            bar: {
                                encrypt: {
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                }
                            }
                        }
                    }
                ]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, AllOfKeywordSubschemaWithEncryptMetadataFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                allOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            bar: {
                                encryptMetadata: {
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                }
                            }
                        }
                    }
                ]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

TEST(EncryptionSchemaTreeTest, AnyOfKeywordSubschemaWithoutEncryptSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                anyOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            baz: {bsonType: "int"}
                        }
                    }
                ]
            }
        }
    })");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.bar")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.baz")));
}

TEST(EncryptionSchemaTreeTest, AnyOfKeywordSubschemaWithEncryptFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                anyOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            bar: {
                                encrypt: {
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                }
                            }
                        }
                    }
                ]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, AnyOfKeywordSubschemaWithEncryptMetadataFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                anyOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            bar: {
                                encryptMetadata: {
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                }
                            }
                        }
                    }
                ]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

TEST(EncryptionSchemaTreeTest, OneOfKeywordSubschemaWithoutEncryptSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                oneOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            baz: {bsonType: "int"}
                        }
                    }
                ]
            }
        }
    })");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.bar")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.baz")));
}

TEST(EncryptionSchemaTreeTest, OneOfKeywordSubschemaWithEncryptFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                oneOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            bar: {
                                encrypt: {
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                }
                            }
                        }
                    }
                ]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, OneOfKeywordSubschemaWithEncryptMetadataFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                oneOf: [
                    {
                        type: "object",
                        properties: {
                            bar: {type: "string"}
                        }
                    },
                    {
                        type: "object",
                        properties: {
                            bar: {
                                encryptMetadata: {
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                }
                            }
                        }
                    }
                ]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

TEST(EncryptionSchemaTreeTest, NotKeywordSubschemaWithoutEncryptSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                not: {
                    type: "object",
                    properties: {
                        bar: {type: "string"}
                    }
                }
            }
        }
    })");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.bar")));
    ASSERT_FALSE(schemaTree->getEncryptionMetadataForPath(FieldRef("foo.baz")));
}

TEST(EncryptionSchemaTreeTest, NotKeywordSubschemaWithEncryptFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                not: {
                    type: "object",
                    properties: {
                        bar: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                            }
                        }
                    }
                }
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, NotKeywordSubschemaWithEncryptMetadataFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                not: {
                    type: "object",
                    properties: {
                        bar: {
                            encryptMetadata: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                            }
                        }
                    }
                }
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

TEST(EncryptionSchemaTreeTest, EncryptMetadataWithoutExplicitTypeObjectSpecificationFails) {
    BSONObj schema = fromjson(R"({
        properties: {
            foo: {
                encryptMetadata: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                }
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31077);
}

// TODO SERVER-40837 Relax the restrictions on these JSON Schema keywords.
TEST(EncryptionSchemaTreeNode, UnsupportedJSONSchemaKeywords) {
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(fromjson(
                           "{dependencies: {foo: {properties: {bar: {type: 'string'}}}}}")),
                       AssertionException,
                       31068);
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(fromjson("{description: 'test'}")),
                       AssertionException,
                       31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {enum: [1, 2, 3]}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(
                           fromjson("{properties: {foo: {exclusiveMaximum: true, maximum: 1}}}")),
                       AssertionException,
                       31068);
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(
                           fromjson("{properties: {foo: {exclusiveMinimum: true, minimum: 1}}}")),
                       AssertionException,
                       31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {maxItems: 1}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {maxLength: 1}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{maxProperties: 1}")), AssertionException, 31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {maximum: 1}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {minItems: 1}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {minLength: 1}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{minProperties: 1}")), AssertionException, 31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{minProperties: 1}")), AssertionException, 31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {minimum: 1}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {multipleOf: 2}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {pattern: 'bar'}}}")),
        AssertionException,
        31068);
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(fromjson("{required: ['a', 'b']}")),
                       AssertionException,
                       31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{title: 'title'}")), AssertionException, 31068);
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson("{properties: {foo: {uniqueItems: true}}}")),
        AssertionException,
        31068);
}

TEST(EncryptionSchemaTreeTest, SchemaTreeHoldsBsonTypeSetWithMultipleTypes) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                    bsonType: ["string", "int", "long"]
                }
            }
        }
    })");

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto resolvedMetadata = schemaTree->getEncryptionMetadataForPath(FieldRef{"foo"});
    ASSERT(resolvedMetadata);
    MatcherTypeSet typeSet;
    typeSet.bsonTypes = {BSONType::String, BSONType::NumberInt, BSONType::NumberLong};
    ASSERT(resolvedMetadata->bsonTypeSet == typeSet);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithNoBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId"
                                            << BSON_ARRAY(uuid)));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithBSONTypeObjectInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId"
                                            << BSON_ARRAY(uuid)
                                            << "bsonType"
                                            << "object"));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithEmptyArrayBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId"
                                            << BSON_ARRAY(uuid)
                                            << "bsonType"
                                            << BSONArray()));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithMultipleElementArrayBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId"
                                            << BSON_ARRAY(uuid)
                                            << "bsonType"
                                            << BSON_ARRAY("int"
                                                          << "string")));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithObjectInArrayBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId"
                                            << BSON_ARRAY(uuid)
                                            << "bsonType"
                                            << BSON_ARRAY("object")));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithSingleValueBSONTypeInEncryptObject) {
    auto uuid = UUID::gen();
    // Test MinKey
    BSONObj encrypt = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "bsonType"
                                             << "minKey"
                                             << "keyId"
                                             << BSON_ARRAY(uuid)));
    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31041);
    // Test MaxKey
    encrypt = BSON("encrypt" << BSON("algorithm"
                                     << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                     << "bsonType"
                                     << "maxKey"
                                     << "keyId"
                                     << BSON_ARRAY(uuid)));
    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31041);
    // Test Undefined
    encrypt = BSON("encrypt" << BSON("algorithm"
                                     << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                     << "bsonType"
                                     << "undefined"
                                     << "keyId"
                                     << BSON_ARRAY(uuid)));
    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31041);
    // Test Null
    encrypt = BSON("encrypt" << BSON("algorithm"
                                     << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                     << "bsonType"
                                     << "null"
                                     << "keyId"
                                     << BSON_ARRAY(uuid)));
    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 31041);
}

}  // namespace
}  // namespace mongo
