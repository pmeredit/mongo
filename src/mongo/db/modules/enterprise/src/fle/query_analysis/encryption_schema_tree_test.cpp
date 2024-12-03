/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "encryption_schema_tree.h"
#include "query_analysis.h"

#include "fle_test_fixture.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/query/bson/bson_helper.h"
#include "mongo/idl/server_parameter_test_util.h"
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
    auto result = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto metadata = result->getEncryptionMetadataForPath(FieldRef(path));
    ASSERT(metadata);
    return metadata.value();
}

static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
const BSONObj encryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))
                           << "bsonType"
                           << "string"));
const BSONObj encryptObjNumber =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))
                           << "bsonType"
                           << "int"));

struct EncryptionSchemasTestData {
    const BSONObj cmdObj;
    std::map<NamespaceString, std::vector<std::string>> namespaceToEncryptedFieldsMap;
    FleVersion fleVersion;
};

const EncryptionSchemasTestData kCsfleEncryptionSchemasTestData{
    .cmdObj = fromjson(R"({
               "csfleEncryptionSchemas": { 
                    "testdb.coll_a": {
                        "jsonSchema": { 
                                type: "object",
                                properties: {
                                    a: {
                                        encrypt: {
                                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                        }
                                    }
                                }
                        },
                        "isRemoteSchema": false },
                   "testdb.coll_b": {
                        "jsonSchema": { 
                                type: "object",
                                properties: {
                                    b: {
                                        encrypt: {
                                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                        }
                                    }
                                }
                        },
                        "isRemoteSchema": false },
                    "testdb.coll_c": {
                        "jsonSchema": { 
                                type: "object",
                                properties: {
                                    c: {
                                        encrypt: {
                                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                        }
                                    }
                                }
                        },
                        "isRemoteSchema": false }
                }
           }
    )"),
    .namespaceToEncryptedFieldsMap =
        {{NamespaceString::createNamespaceString_forTest("testdb.coll_a"), {"a"}},
         {NamespaceString::createNamespaceString_forTest("testdb.coll_b"), {"b"}},
         {NamespaceString::createNamespaceString_forTest("testdb.coll_c"), {"c"}}},
    .fleVersion = FleVersion::kFle1};

const EncryptionSchemasTestData kEncryptionInformationSchemasTestData{
    .cmdObj = fromjson(R"({
               "encryptionInformation": {
                    "type": 1,
                    "schema":{
                        "testdb.coll_a": {
                            "escCollection": "enxcol_.coll_a.esc",
                            "ecocCollection": "enxcol_.coll_a.ecoc",
                            "fields":
                                [
                                {
                                    "keyId": {
                                        "$uuid": "c7d050cb-e8c1-4108-8dd1-10f33f2c6dc3"
                                    },
                                    "path": "ssn_a",
                                    "bsonType": "string",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                },
                                {
                                    "keyId": {
                                        "$uuid": "a0d8e31d-8475-40bf-aefd-b0a8459080e1"
                                    },
                                    "path": "age_a",
                                    "bsonType": "long",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                }
                                ]
                        },
                        "testdb.coll_b": {
                            "escCollection": "enxcol_.coll_b.esc",
                            "ecocCollection": "enxcol_.coll_b.ecoc",
                            "fields":
                                [
                                {
                                    "keyId": {
                                        "$uuid": "c7d050cb-e8c1-4108-8dd1-10f33f2c6dc3"
                                    },
                                    "path": "ssn_b",
                                    "bsonType": "string",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                },
                                {
                                    "keyId": {
                                        "$uuid": "a0d8e31d-8475-40bf-aefd-b0a8459080e1"
                                    },
                                    "path": "age_b",
                                    "bsonType": "long",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                }
                                ]
                        },
                        "testdb.coll_c": {
                            "escCollection": "enxcol_.coll_c.esc",
                            "ecocCollection": "enxcol_.coll_c.ecoc",
                            "fields":
                                [
                                {
                                    "keyId": {
                                        "$uuid": "c7d050cb-e8c1-4108-8dd1-10f33f2c6dc3"
                                    },
                                    "path": "ssn_c",
                                    "bsonType": "string",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                },
                                {
                                    "keyId": {
                                        "$uuid": "a0d8e31d-8475-40bf-aefd-b0a8459080e1"
                                    },
                                    "path": "age_c",
                                    "bsonType": "long",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                }
                                ]
                        }
                    }   
                }
           }
    )"),
    .namespaceToEncryptedFieldsMap =
        {{NamespaceString::createNamespaceString_forTest("testdb.coll_a"), {"ssn_a", "age_a"}},
         {NamespaceString::createNamespaceString_forTest("testdb.coll_b"), {"ssn_b", "age_b"}},
         {NamespaceString::createNamespaceString_forTest("testdb.coll_c"), {"ssn_c", "age_c"}}},
    .fleVersion = FleVersion::kFle2};
/**
 * Parses 'schema' into an encryption schema tree and verifies that 'path' is not encrypted.
 */
void assertNotEncrypted(BSONObj schema, std::string path) {
    auto result = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(result->getEncryptionMetadataForPath(FieldRef(path)));
}

constexpr int kExpectDefaultSparsity = 2;
void checkRangeQueryTypeConfig(const QueryTypeConfig& qtc,
                               BSONType type,
                               mongo::Value min,
                               mongo::Value max,
                               std::int64_t sparsity,
                               boost::optional<std::int32_t> trimFactor = boost::none,
                               boost::optional<std::int32_t> precision = boost::none) {
    ASSERT_EQUALS(qtc.getQueryType(), QueryTypeEnum::Range);
    ASSERT(qtc.getMin().has_value());
    ASSERT(qtc.getMax().has_value());
    ASSERT_EQUALS(type, min.getType());
    ASSERT_EQUALS(type, max.getType());
    ASSERT_EQUALS(type, qtc.getMin()->getType());
    ASSERT_EQUALS(type, qtc.getMax()->getType());
    switch (type) {
        case NumberInt:
            ASSERT_EQUALS(min.getInt(), qtc.getMin()->getInt());
            ASSERT_EQUALS(max.getInt(), qtc.getMax()->getInt());
            break;
        case NumberLong:
            ASSERT_EQUALS(min.getLong(), qtc.getMin()->getLong());
            ASSERT_EQUALS(max.getLong(), qtc.getMax()->getLong());
            break;
        case Date:
            ASSERT_EQUALS(min.getDate(), qtc.getMin()->getDate());
            ASSERT_EQUALS(max.getDate(), qtc.getMax()->getDate());
            break;
        case NumberDouble:
            ASSERT_EQUALS(min.getDouble(), qtc.getMin()->getDouble());
            ASSERT_EQUALS(max.getDouble(), qtc.getMax()->getDouble());
            break;
        case NumberDecimal:
            ASSERT_EQUALS(min.getDecimal(), qtc.getMin()->getDecimal());
            ASSERT_EQUALS(max.getDecimal(), qtc.getMax()->getDecimal());
            break;
        default:
            MONGO_UNREACHABLE;
    }

    ASSERT(qtc.getSparsity().has_value());
    ASSERT_EQUALS(qtc.getSparsity().value(), sparsity);
    ASSERT(precision.has_value() == qtc.getPrecision().has_value());
    if (precision) {
        ASSERT(precision.value() == qtc.getPrecision().value());
    }
    ASSERT(trimFactor.has_value() == qtc.getTrimFactor().has_value());
    if (trimFactor) {
        ASSERT(trimFactor.value() == qtc.getTrimFactor().value());
    }
}

class EncryptionSchemaTreeTest : public FLETestFixture {};

TEST(EncryptionSchemaTreeTest, MarksTopLevelFieldAsEncrypted) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    BSONObj schema = fromjson(
        R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [)" +
        uuid.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]
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

    BSONObj schema = fromjson(
        R"({
        type: "object",
        properties: {
            user: {
                type: "object",
                properties: {
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [)" +
        uuid.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]
                        }
                    }
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT(extractMetadata(schema, "user.ssn") == metadata);
    assertNotEncrypted(schema, "user");
    assertNotEncrypted(schema, "user.name");
}

TEST(EncryptionSchemaTreeTest, MarksNumericFieldNameAsEncrypted) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};
    BSONObj encryptObj = fromjson(
        R"({
        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
        keyId: [)" +
        uuid.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]})");

    BSONObj schema = BSON("type"
                          << "object"
                          << "properties" << BSON("0" << BSON("encrypt" << encryptObj)));
    ASSERT(extractMetadata(schema, "0") == metadata);

    schema = BSON(
        "type"
        << "object"
        << "properties"
        << BSON("nested" << BSON("type"
                                 << "object"
                                 << "properties" << BSON("0" << BSON("encrypt" << encryptObj)))));
    ASSERT(extractMetadata(schema, "nested.0") == metadata);
}

TEST(EncryptionSchemaTreeTest, MarksMultipleFieldsAsEncrypted) {
    const auto uuid = UUID::gen();
    const auto uuidStr = uuid.toBSON().getField("uuid").jsonString(
        JsonStringFormat::ExtendedCanonicalV2_0_0, false, false);
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
    const auto uuidStr = uuid.toBSON().getField("uuid").jsonString(
        JsonStringFormat::ExtendedCanonicalV2_0_0, false, false);
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

    const IDLParserContext encryptCtxt("encrypt");
    auto encryptObj = BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "bsonType"
                           << "string"
                           << "keyId" << BSON_ARRAY(uuid));
    BSONObj schema = BSON("type"
                          << "object"
                          << "properties" << BSON("ssn" << BSON("encrypt" << encryptObj)));
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
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            blah: {}
        },
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                bsonType: "int"
            }
        }
    })");
    ASSERT_THROWS_CODE(extractMetadata(schema, "path.extends.encrypt"), AssertionException, 51102);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::FailedToParse);

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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::FailedToParse);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51078);

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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51078);

    schema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    },
                    minItems: 1
                }
            }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kRemote),
                       AssertionException,
                       51078);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);

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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);

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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);

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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    auto result = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    auto result = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    assertNotEncrypted(schema, "a.c");
    ASSERT(result->getEncryptionMetadataForPath(FieldRef{"abc"}));
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidSchema) {
    BSONObj schema = fromjson(R"({properties: "invalid"})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::TypeMismatch);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: "invalid"
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseUnknownFieldInEncrypt) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {unknownField: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       40415);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::BadValue);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidKeyIdUUID) {
    BSONObj schema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("ssn" << BSON("encrypt" << BSON("keyId" << BSON_ARRAY(BSONBinData(
                                                         nullptr, 0, BinDataType::MD5Type))))));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51084);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptBelowAdditionalPropertiesWithoutTypeObject) {
    BSONObj schema = fromjson(R"({
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfAdditionalPropertiesIsWrongType) {
    BSONObj schema = fromjson("{additionalProperties: [{type: 'string'}]}");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::TypeMismatch);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesAllPropertiesCorrectlyReportedAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                bsonType: "int"
            }
        }
    })");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "int"
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                    bsonType: "int"
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    BSONObj schema = fromjson(
        R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
            keyId: [)" +
        uuid.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]
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

    BSONObj schema = fromjson(
        R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
        },
        type: "object",
        properties: {
            super: {
                encryptMetadata: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [)" +
        uuid1.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]
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
        uuid2.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]
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

    BSONObj schema = fromjson(
        R"({
        encryptMetadata: {
            keyId: [)" +
        uuid.toBSON().getField("uuid").jsonString(
            JsonStringFormat::ExtendedCanonicalV2_0_0, false, false) +
        R"(]
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51099);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51097);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPatternPropertiesIsNotAnObject) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: true
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPatternPropertiesHasNonObjectProperty) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: true
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfPatternPropertiesHasAnIllFormedRegex) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            "(": {}
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       ErrorCodes::BadValue);
}

TEST(EncryptionSchemaTreeTest, LegalPatternPropertiesParsesSuccessfully) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            foo: {},
            bar: {}
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                bsonType: "int"
            }
        }
    })");

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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

    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"ab"}));
}

TEST(EncryptionSchemaTreeTest, PatternPropertiesInheritsParentEncryptMetadata) {
    const auto uuid = UUID::gen();
    ResolvedEncryptionInfo expectedMetadata{
        EncryptSchemaKeyId{std::vector<UUID>{uuid}}, FleAlgorithmEnum::kRandom, boost::none};

    auto uuidJsonStr = uuid.toBSON().getField("uuid").jsonString(
        JsonStringFormat::ExtendedCanonicalV2_0_0, false, false);
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

    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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

    ASSERT(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
               ->mayContainEncryptedNode());
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

    ASSERT(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
               ->mayContainEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsFalseIfPropertyDoesNotContainEncryptedNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        }
    })");

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                     ->mayContainEncryptedNode());
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

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                     ->mayContainEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsTrueIfAdditionalPropertiesHasEncryptNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {type: "string"}
        },
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                bsonType: "int"
            }
        }
    })");

    ASSERT_TRUE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                    ->mayContainEncryptedNode());
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

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                     ->mayContainEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, ContainsEncryptReturnsTrueIfPatternPropertiesHasEncryptNode) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                description: "Must be a string",
                type: "string"
            }
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

    ASSERT_TRUE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                    ->mayContainEncryptedNode());
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
    ASSERT_TRUE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                    ->mayContainEncryptedNodeBelowPrefix(FieldRef("a")));
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
    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                     ->mayContainEncryptedNodeBelowPrefix(FieldRef("c")));
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

    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
        ->mayContainEncryptedNodeBelowPrefix(FieldRef("foo"));
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

    ASSERT_FALSE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal)
                     ->mayContainEncryptedNode());
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(schemaTree->mayContainEncryptedNodeBelowPrefix(FieldRef("a")));
    ASSERT_TRUE(schemaTree->mayContainEncryptedNodeBelowPrefix(FieldRef("a.b")));
    ASSERT_TRUE(schemaTree->mayContainEncryptedNodeBelowPrefix(FieldRef("a.b.c")));
}

TEST(EncryptionSchemaTreeTest, ParseFailsIfDeterministicAndNoBSONType) {
    const auto uuid = UUID::gen();
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "keyId" << BSON_ARRAY(uuid)));
    auto schema = BSON("type"
                       << "object"
                       << "properties" << BSON("foo" << encryptObj));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31051);
}

TEST(EncryptionSchemaTreeTest, ParseFailsIfDeterministicAndMultipleBSONType) {
    const auto uuid = UUID::gen();
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "bsonType"
                                             << BSON_ARRAY("string"
                                                           << "int")
                                             << "keyId" << BSON_ARRAY(uuid)));
    auto schema = BSON("type"
                       << "object"
                       << "properties" << BSON("foo" << encryptObj));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31051);
}

TEST(EncryptionSchemaTreeTest, ParseFailsIfDeterministicAndPointerKey) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    bsonType: "string",
                    keyId: "/customKey"
                }
            },
            customKey: {bsonType: "string"}
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31169);
}

TEST(EncryptionSchemaTreeTest, ParseFailsIfInheritedDeterministicAndPointerKey) {
    BSONObj schema = fromjson(R"({
        type: "object",
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
        },
        properties: {
             ssn: {
                encrypt: {
                    bsonType: "string",
                    keyId: "/customKey"
                }
            },
            customKey: {bsonType: "string"}
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31169);
}

TEST(EncryptionSchemaTreeTest, ParseSucceedsIfRandomAndPointerKey) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: "/customKey"
                }
            },
            customKey: {bsonType: "string"}
        }
    })");
    ASSERT(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal));
}

TEST(EncryptionSchemaTreeTest, ParseSucceedsIfLengthOneTypeArray) {
    const auto uuid = UUID::gen();
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "bsonType" << BSON_ARRAY("string") << "keyId"
                                             << BSON_ARRAY(uuid)));
    auto schema = BSON("type"
                       << "object"
                       << "properties" << BSON("foo" << encryptObj));
    // Just making sure the parse succeeds, don't care about the result.
    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31077);
}

void verifySchemaKeywordWorksOnlyForRemoteSchema(StringData schema, bool hasEncryptedNode) {
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(fromjson(schema.rawData()), EncryptionSchemaType::kLocal),
        AssertionException,
        31068);
    ASSERT_EQ(
        EncryptionSchemaTreeNode::parse(fromjson(schema.rawData()), EncryptionSchemaType::kRemote)
            ->mayContainEncryptedNode(),
        hasEncryptedNode);
};

TEST(EncryptionSchemaTreeTest, VerifyRemoteSchemaKeywordsAreAllowed) {
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {enum: [1, 2, 3]}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema(
        "{properties: {foo: {exclusiveMaximum: true, maximum: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema(
        "{properties: {foo: {exclusiveMinimum: true, minimum: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {maxItems: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {minItems: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {maxLength: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {minLength: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{maxProperties: 1}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{minProperties: 1}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {maximum: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {minimum: 1}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {multipleOf: 2}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {pattern: '^bar'}}}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{required: ['a', 'b']}", false);
    verifySchemaKeywordWorksOnlyForRemoteSchema("{properties: {foo: {uniqueItems: true}}}", false);
}

TEST(EncryptionSchemaTreeTest, VerifyRemoteSchemaKeywordsInNestedObjectAreAllowed) {
    StringData schemaWithKeywordsInNestedObject = R"({
            type: "object",
            title: "title",
            minProperties: 4,
            maxProperties: 3,
            properties: {
                user: {
                    type: "object",
                    minProperties: 4,
                    maxProperties: 3,
                    properties: {}
                },
                a: {type: "string", minLength: 1}
            },
            patternProperties: {
                "^b": {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                    }
                }
            }
        })";
    verifySchemaKeywordWorksOnlyForRemoteSchema(schemaWithKeywordsInNestedObject, true);
}

TEST(EncryptionSchemaTreeTest, VerifyRemoteSchemaKeywordsInsideArrayAreAllowed) {
    StringData schemaWithKeywordsInArray = R"({
            type: "object",
            properties: {
                user: {
                    type: "object",
                    properties: {
                        arr: {
                            type: "array",
                            "minItems": 2,
                            "maxItems": 3,
                            items: {
                            }
                        },
                        ssn : {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                            }
                        }
                    }
                }

            }
            })";
    verifySchemaKeywordWorksOnlyForRemoteSchema(schemaWithKeywordsInArray, true);
}

TEST(EncryptionSchemaTreeTest, VerifyTitleAndDescriptionKeywordsAlwaysPermitted) {
    const auto verifySchemaKeywordWorks = [](StringData schema, bool hasEncryptedNode) {
        ASSERT_EQ(EncryptionSchemaTreeNode::parse(fromjson(schema.rawData()),
                                                  EncryptionSchemaType::kLocal)
                      ->mayContainEncryptedNode(),
                  hasEncryptedNode);
        ASSERT_EQ(EncryptionSchemaTreeNode::parse(fromjson(schema.rawData()),
                                                  EncryptionSchemaType::kRemote)
                      ->mayContainEncryptedNode(),
                  hasEncryptedNode);
    };
    verifySchemaKeywordWorks("{description: 'description'}", false);
    verifySchemaKeywordWorks("{title: 'title'}", false);

    StringData schema = R"({
                type: "object",
                title: "title",
                description: "description",
                properties: {
                    a: {type: "string", title: "title"}
                },
                patternProperties: {
                    "^b": {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            })";
    verifySchemaKeywordWorks(schema, true);
}

TEST(EncryptionSchemaTreeTest, VerifyIllegalSchemaRejected) {
    const auto verifyIllegalSchemaRejected = [](StringData schema, int errorCode) {
        ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(fromjson(schema.rawData()),
                                                           EncryptionSchemaType::kLocal),
                           AssertionException,
                           errorCode);
        ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(fromjson(schema.rawData()),
                                                           EncryptionSchemaType::kRemote),
                           AssertionException,
                           errorCode);
    };
    verifyIllegalSchemaRejected("{unsupportedKeyword: {foo: {pattern: '(bar[bar'}}}",
                                ErrorCodes::FailedToParse);

    // Negative number of items.
    verifyIllegalSchemaRejected("{properties: {foo: {maxItems: -10}}}", ErrorCodes::FailedToParse);
    verifyIllegalSchemaRejected("{properties: {foo: {minItems: -10}}}", ErrorCodes::FailedToParse);

    // Type mismatch.
    verifyIllegalSchemaRejected("{properties: {foo: {enum: {invalid: 'field'}}}}",
                                ErrorCodes::TypeMismatch);
    verifyIllegalSchemaRejected("{properties: {foo: {uniqueItems: 1}}}", ErrorCodes::TypeMismatch);

    // Invalid regex.
    verifyIllegalSchemaRejected("{properties: {foo: {pattern: '(bar[bar'}}}", 51091);

    // mongocryptd currently doesn't support 'dependencies'. This can be removed when SERVER-41288
    // is completed.
    verifyIllegalSchemaRejected("{dependencies: {foo: {properties: {bar: {type: 'string'}}}}}",
                                31126);
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

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
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
                                            << "keyId" << BSON_ARRAY(uuid)));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithBSONTypeObjectInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId" << BSON_ARRAY(uuid) << "bsonType"
                                            << "object"));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31122);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithEmptyArrayBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema =
        BSON("encrypt" << BSON("algorithm"
                               << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                               << "keyId" << BSON_ARRAY(uuid) << "bsonType" << BSONArray()));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithMultipleElementArrayBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId" << BSON_ARRAY(uuid) << "bsonType"
                                            << BSON_ARRAY("int"
                                                          << "string")));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31051);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithObjectInArrayBSONTypeInDeterministicEncrypt) {
    auto uuid = UUID::gen();
    BSONObj schema = BSON("encrypt" << BSON("algorithm"
                                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                            << "keyId" << BSON_ARRAY(uuid) << "bsonType"
                                            << BSON_ARRAY("object")));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31122);
}

TEST(EncryptionSchemaTreeTest, FailsToParseWithSingleValueBSONTypeInEncryptObject) {
    auto uuid = UUID::gen();
    // Test MinKey
    BSONObj encrypt = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                             << "bsonType"
                                             << "minKey"
                                             << "keyId" << BSON_ARRAY(uuid)));
    BSONObj schema = BSON("type"
                          << "object"
                          << "properties" << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31122);
    // Test MaxKey
    encrypt = BSON("encrypt" << BSON("algorithm"
                                     << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                     << "bsonType"
                                     << "maxKey"
                                     << "keyId" << BSON_ARRAY(uuid)));
    schema = BSON("type"
                  << "object"
                  << "properties" << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31122);
    // Test Undefined
    encrypt = BSON("encrypt" << BSON("algorithm"
                                     << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                     << "bsonType"
                                     << "undefined"
                                     << "keyId" << BSON_ARRAY(uuid)));
    schema = BSON("type"
                  << "object"
                  << "properties" << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31122);
    // Test Null
    encrypt = BSON("encrypt" << BSON("algorithm"
                                     << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                     << "bsonType"
                                     << "null"
                                     << "keyId" << BSON_ARRAY(uuid)));
    schema = BSON("type"
                  << "object"
                  << "properties" << BSON("foo" << encrypt));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       31122);
}

TEST(EncryptionSchemaTreeTest, IdEncryptedWithDeterministicAlgorithmSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            _id: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                    bsonType: "string"
                }
            }
        }
    })");
    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
}

TEST(EncryptionSchemaTreeTest, NestedIdEncryptedWithRandomAlgorithmSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                type: "object",
                properties: {
                    _id: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");
    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
}

TEST(EncryptionSchemaTreeTest, IdEncryptedWithRandomAlgorithmFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            _id: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51194);
}

TEST(EncryptionSchemaTreeTest, IdDescendantEncryptedWithRandomAlgorithmFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            _id: {
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
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51194);
}

TEST(EncryptionSchemaTreeTest,
     TopLevelAdditionalPropertiesEncryptedWithRandomAlgorithmExcludingIdSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            _id: {
                type: "string"
            }
        },
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }
    })");
    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
}

TEST(EncryptionSchemaTreeTest,
     TopLevelAdditionalPropertiesEncryptedWithRandomAlgorithmIncludingIdFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51194);
}

TEST(EncryptionSchemaTreeTest,
     NestedPatternPropertiesMatchingIdEncryptedWithRandomAlgorithmSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            foo: {
                type: "object",
                patternProperties: {
                    "^_": {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                        }
                    }
                }
            }
        }
    })");
    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
}

TEST(EncryptionSchemaTreeTest,
     TopLevelPatternPropertiesMatchingIdEncryptedWithDeterministicAlgorithmSucceeds) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            "^_": {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                    bsonType: "string"
                }
            }
        }
    })");
    EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
}

TEST(EncryptionSchemaTreeTest,
     TopLevelPatternPropertiesMatchingIdEncryptedWithRandomAlgorithmFails) {
    BSONObj schema = fromjson(R"({
        type: "object",
        patternProperties: {
            "^_": {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                }
            }
        }
    })");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal),
                       AssertionException,
                       51194);
}

TEST(EncryptionSchemaTreeTest, SingleUnencryptedPropertySchemasAreEqual) {
    BSONObj BSONSchema = BSON("type"
                              << "object"
                              << "properties"
                              << BSON("user" << BSON("type"
                                                     << "string")));
    auto firstSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, SingleEncryptedPropertySchemasAreEqual) {
    BSONObj BSONSchema = BSON("type"
                              << "object"
                              << "properties" << BSON("user" << encryptObj));
    auto firstSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, OutOfOrderSchemasAreEqual) {
    BSONObj firstBSON = BSON("type"
                             << "object"
                             << "properties"
                             << BSON("user" << encryptObj << "person" << encryptObj));
    BSONObj secondBSON = BSON("type"
                              << "object"
                              << "properties"
                              << BSON("person" << encryptObj << "user" << encryptObj));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentEncryptionMetadataAreNotEqual) {
    BSONObj firstBSON = BSON("type"
                             << "object"
                             << "properties" << BSON("user" << encryptObj));
    BSONObj secondBSON = BSON("type"
                              << "object"
                              << "properties" << BSON("user" << encryptObjNumber));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentFieldNamesAreNotEqual) {
    BSONObj firstBSON = BSON("type"
                             << "object"
                             << "properties" << BSON("user" << encryptObj));
    BSONObj secondBSON = BSON("type"
                              << "object"
                              << "properties" << BSON("person" << encryptObj));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, ExtraUnencryptedLeafNodeAreNotEqual) {
    BSONObj firstBSON = BSON("type"
                             << "object"
                             << "properties" << BSON("user" << encryptObj));
    BSONObj secondBSON = BSON("type"
                              << "object"
                              << "properties"
                              << BSON("user" << encryptObj << "extra"
                                             << BSON("type"
                                                     << "string")));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, IdenticalNestedSchemaAreEqual) {
    const BSONObj BSONSchema = BSON(
        "type"
        << "object"
        << "properties"
        << BSON("person" << encryptObj << "user"
                         << BSON("type"
                                 << "object"
                                 << "properties"
                                 << BSON("ssn"
                                         << encryptObj << "address" << encryptObj << "accounts"
                                         << BSON("type"
                                                 << "object"
                                                 << "properties" << BSON("bank" << encryptObj))))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentMetadataNestedSchemaAreNotEqual) {
    const BSONObj firstBSON = BSON(
        "type"
        << "object"
        << "properties"
        << BSON("person" << encryptObj << "user"
                         << BSON("type"
                                 << "object"
                                 << "properties"
                                 << BSON("ssn"
                                         << encryptObj << "address" << encryptObj << "accounts"
                                         << BSON("type"
                                                 << "object"
                                                 << "properties" << BSON("bank" << encryptObj))))));
    const BSONObj secondBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObjNumber << "address" << encryptObj
                                                    << "accounts"
                                                    << BSON("type"
                                                            << "object"
                                                            << "properties"
                                                            << BSON("bank" << encryptObj))))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentPathNestedSchemaAreNotEqual) {
    const BSONObj firstBSON = BSON(
        "type"
        << "object"
        << "properties"
        << BSON("person" << encryptObj << "user"
                         << BSON("type"
                                 << "object"
                                 << "properties"
                                 << BSON("ssn"
                                         << encryptObj << "address" << encryptObj << "accounts"
                                         << BSON("type"
                                                 << "object"
                                                 << "properties" << BSON("bank" << encryptObj))))));
    const BSONObj secondBSON = BSON(
        "type"
        << "object"
        << "properties"
        << BSON("person" << encryptObj << "user"
                         << BSON("type"
                                 << "object"
                                 << "properties"
                                 << BSON("ssn"
                                         << encryptObj << "homeLocation" << encryptObj << "accounts"
                                         << BSON("type"
                                                 << "object"
                                                 << "properties" << BSON("bank" << encryptObj))))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, ExtraNestedAdditionalPropertiesAreNotEqual) {
    const BSONObj firstBSON = BSON("type"
                                   << "object"
                                   << "properties"
                                   << BSON("person" << encryptObj << "user"
                                                    << BSON("type"
                                                            << "object"
                                                            << "properties"
                                                            << BSON("ssn" << encryptObj << "address"
                                                                          << encryptObj))));
    const BSONObj secondBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "additionalProperties" << encryptObjNumber)));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentNestedAdditionalPropertiesAreNotEqual) {
    const BSONObj firstBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "additionalProperties" << encryptObj)));
    const BSONObj secondBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "additionalProperties" << encryptObjNumber)));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, IdenticalNestedAdditionalPropertiesAreEqual) {
    const BSONObj BSONSchema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "additionalProperties" << encryptObj)));
    auto firstSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, OutOfOrderPatternSchemasAreEqual) {
    BSONObj firstBSON = BSON("type"
                             << "object"
                             << "patternProperties"
                             << BSON("user" << encryptObj << "person" << encryptObj));
    BSONObj secondBSON = BSON("type"
                              << "object"
                              << "patternProperties"
                              << BSON("person" << encryptObj << "user" << encryptObj));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}
TEST(EncryptionSchemaTreeTest, IdenticalNestedPatternPropertiesAreEqual) {
    const BSONObj BSONSchema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "patternProperties" << BSON("foo" << encryptObj))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(BSONSchema, EncryptionSchemaType::kLocal);
    ASSERT_TRUE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentNameNestedPatternPropertiesAreNotEqual) {
    const BSONObj firstBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "patternProperties" << BSON("foo" << encryptObj))));
    const BSONObj secondBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "patternProperties" << BSON("bar" << encryptObj))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, DifferentMetadataNestedPatternPropertiesAreNotEqual) {
    const BSONObj firstBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "patternProperties" << BSON("foo" << encryptObj))));
    const BSONObj secondBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "patternProperties" << BSON("foo" << encryptObjNumber))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, MissingNestedPatternPropertiesAreNotEqual) {
    const BSONObj firstBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj)
                                      << "patternProperties" << BSON("foo" << encryptObj))));
    const BSONObj secondBSON =
        BSON("type"
             << "object"
             << "properties"
             << BSON("person" << encryptObj << "user"
                              << BSON("type"
                                      << "object"
                                      << "properties"
                                      << BSON("ssn" << encryptObj << "address" << encryptObj))));
    auto firstSchema = EncryptionSchemaTreeNode::parse(firstBSON, EncryptionSchemaType::kLocal);
    auto secondSchema = EncryptionSchemaTreeNode::parse(secondBSON, EncryptionSchemaType::kLocal);
    ASSERT_FALSE(*firstSchema == *secondSchema);
}

TEST(EncryptionSchemaTreeTest, StateMixedNodeGetEncryptionMetadataFails) {
    EncryptionSchemaStateMixedNode node{FleVersion::kFle1};
    ASSERT_THROWS_CODE(node.getEncryptionMetadata(), AssertionException, 31133);
}

TEST(EncryptionSchemaTreeTest, StateMixedNodeContainsEncryptedNodeReturnsTrue) {
    EncryptionSchemaStateMixedNode node{FleVersion::kFle1};
    ASSERT_TRUE(node.mayContainEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, StateMixedNodeContainsRandomEncryptedNodeReturnsTrue) {
    EncryptionSchemaStateMixedNode node{FleVersion::kFle1};
    ASSERT_TRUE(node.mayContainRandomlyEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, Fle2StateMixedNodeGetEncryptionMetadataFails) {
    EncryptionSchemaStateMixedNode node{FleVersion::kFle2};
    ASSERT_THROWS_CODE(node.getEncryptionMetadata(), AssertionException, 31133);
}

TEST(EncryptionSchemaTreeTest, Fle2StateMixedNodeContainsEncryptedNodeReturnsTrue) {
    EncryptionSchemaStateMixedNode node{FleVersion::kFle2};
    ASSERT_TRUE(node.mayContainEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, Fle2StateMixedNodeContainsRandomEncryptedNodeReturnsTrue) {
    EncryptionSchemaStateMixedNode node{FleVersion::kFle2};
    ASSERT_TRUE(node.mayContainRandomlyEncryptedNode());
}

TEST(EncryptionSchemaTreeTest, GetEncryptionMetadataFailsIfPathHitsStateMixedNode) {
    BSONObj schema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                        bsonType: ["string", "int", "long"]
                    }

                },
                name: {
                    type: "string"
                }
            }
        })");
    auto root = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    root->addChild(FieldRef("ticker"),
                   std::make_unique<EncryptionSchemaStateMixedNode>(FleVersion::kFle1));
    ASSERT_THROWS_CODE(
        root->getEncryptionMetadataForPath(FieldRef("ticker")), AssertionException, 31133);
    ASSERT_TRUE(root->mayContainEncryptedNode());
    ASSERT(root->getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(root->getEncryptionMetadataForPath(FieldRef("name")));
}

TEST(EncryptionSchemaTreeTest, StateMixedNodeGetMetadataForStateMixedNodeNestedCorrectlyFails) {
    auto root = std::make_unique<EncryptionSchemaNotEncryptedNode>(FleVersion::kFle1);
    root->addChild(FieldRef("name.ticker"),
                   std::make_unique<EncryptionSchemaStateMixedNode>(FleVersion::kFle1));
    ASSERT_THROWS_CODE(
        root->getEncryptionMetadataForPath(FieldRef("name.ticker")), AssertionException, 31133);
}

TEST(EncryptionSchemaTreeTest, StateMixedNodeInAdditionalPropertiesFailsOnUndefinedFieldPath) {
    BSONObj schema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                        bsonType: ["string", "int", "long"]
                    }

                },
                name: {
                    type: "string"
                }
            }
        })");
    auto root = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    root->addAdditionalPropertiesChild(
        std::make_unique<EncryptionSchemaStateMixedNode>(root->parsedFrom));
    ASSERT(root->getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(root->getEncryptionMetadataForPath(FieldRef("name")));
    ASSERT_TRUE(root->mayContainEncryptedNode());
    ASSERT_THROWS_CODE(
        root->getEncryptionMetadataForPath(FieldRef("undefinedField")), AssertionException, 31133);
}

TEST(EncryptionSchemaTreeTest, StateMixedNodeInPatternPropertiesMetadataFailOnMatchedProperty) {
    auto root = std::make_unique<EncryptionSchemaNotEncryptedNode>(FleVersion::kFle1);
    root->addPatternPropertiesChild(
        R"(tickers+)", std::make_unique<EncryptionSchemaStateMixedNode>(FleVersion::kFle1));
    ASSERT_FALSE(root->getEncryptionMetadataForPath(FieldRef("some.path")));
    ASSERT_TRUE(root->mayContainEncryptedNode());
    ASSERT_THROWS_CODE(
        root->getEncryptionMetadataForPath(FieldRef("tickerssss")), AssertionException, 31133);
}

DEATH_TEST_REGEX_F(EncryptionSchemaTreeTest,
                   Fle2AddingPatternPropertiesFails,
                   "Tripwire assertion.*6329205") {
    auto root = std::make_unique<EncryptionSchemaNotEncryptedNode>(FleVersion::kFle2);
    ASSERT_THROWS_CODE(
        root->addPatternPropertiesChild(
            R"(tickers+)", std::make_unique<EncryptionSchemaStateMixedNode>(FleVersion::kFle2)),
        AssertionException,
        6329205);
}

TEST(EncryptionSchemaTreeTest, AddChildThrowsIfAddingToEncryptedNode) {
    BSONObj schema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                        bsonType: ["string", "int", "long"]
                    }
                },
                name: {
                    type: "string"
                }
            }
        })");
    auto root = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        root->addChild(FieldRef{"ssn.test"},
                       std::make_unique<EncryptionSchemaNotEncryptedNode>(root->parsedFrom)),
        AssertionException,
        51096);
}

DEATH_TEST(EncryptionSchemaTreeTest, AddChildThrowsIfParsedFromVersionsDontMatch, "invariant") {
    auto root = std::make_unique<EncryptionSchemaNotEncryptedNode>(FleVersion::kFle1);
    root->addChild(FieldRef("foo"),
                   std::make_unique<EncryptionSchemaNotEncryptedNode>(FleVersion::kFle2));
}

TEST_F(EncryptionSchemaTreeTest, CanAffixLiteralsToEncryptedNodesButNotToNotEncryptedNodes) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            a: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "int"
                    }
                }
            }
        }
    })");
    // EncryptionSchemaNotEncryptedNodes have no space for literals.
    auto root = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT(root->literals() == boost::none);
    // EncryptionSchemaEncryptedNodes allow access to mutable space for literals.
    auto* leaf = root->getNode(FieldRef{"a"});
    ASSERT(leaf->literals() != boost::none);
    auto literal = ExpressionConstant::create(getExpCtxRaw(), Value{23});
    leaf->literals()->push_back(*literal);
    ASSERT_FALSE(leaf->literals()->empty());
}


/**
 * Tests new parser for EncryptedFieldConfig and new FLE 2 algorithm enum.
 */
TEST_F(EncryptionSchemaTreeTest, Fle2ParseDottedAndTopLevelFields) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "string",
                       "queries": [
                           {"queryType": "equality"}
                       ]
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": {"queryType": "equality"}
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    // "a" with type "string" should be encrypted and queryable.
    auto leafA = root->getNode(FieldRef{"a"});
    auto metadataA = leafA->getEncryptionMetadata();
    ASSERT(metadataA->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataA->bsonTypeSet == MatcherTypeSet(BSONType::String));

    // "b.c" with type "int" should be encrypted and queryable.
    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberInt));
}

TEST_F(EncryptionSchemaTreeTest, Fle2ParseNotQueryableField) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "date"
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "long"
                   }]
           }
    )");

    // The root is not encrypted.
    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    // "a" with type "date" should be encrypted but not queryable.
    auto leafA = root->getNode(FieldRef{"a"});
    auto metadataA = leafA->getEncryptionMetadata();
    ASSERT(metadataA->algorithmIs(Fle2AlgorithmInt::kUnindexed));
    ASSERT(metadataA->bsonTypeSet == MatcherTypeSet(BSONType::Date));

    // "b" is not encryped.
    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());

    // "b.c" with type "long" should be encrypted but not queryable.
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kUnindexed));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberLong));
}

TEST_F(EncryptionSchemaTreeTest, Fle2EncryptedFieldsCanHaveSharedPrefix) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "string",
                       "queries": {"queryType": "equality"}
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.d",
                       "bsonType": "string",
                       "queries": {"queryType": "equality"}
                   }]
           }
    )");
    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    // "a.b" is not encryped.
    auto nodeAB = root->getNode(FieldRef{"a.b"});
    ASSERT_FALSE(nodeAB->getEncryptionMetadata());

    // "a.b.c" should be encrypted and queryable.
    auto leafABC = nodeAB->getNode(FieldRef{"c"});
    auto metadataABC = leafABC->getEncryptionMetadata();
    ASSERT(metadataABC->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataABC->bsonTypeSet == MatcherTypeSet(BSONType::String));

    // "a.b.d" should also be encrypted and queryable.
    auto leafABD = nodeAB->getNode(FieldRef{"d"});
    auto metadataABD = leafABD->getEncryptionMetadata();
    ASSERT(metadataABD->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataABD->bsonTypeSet == MatcherTypeSet(BSONType::String));
}

TEST_F(EncryptionSchemaTreeTest, Fle2MarksNumericFieldNameAsEncrypted) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.0",
                       "bsonType": "string",
                       "queries": {"queryType": "equality"}
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.0.c",
                       "bsonType": "string",
                       "queries": {"queryType": "equality"}
                   }]
           }
    )");
    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);

    // "a.0" should be encrypted and queryable.
    auto leafA0 = root->getNode(FieldRef{"a.0"});
    auto metadataA0 = leafA0->getEncryptionMetadata();
    ASSERT(metadataA0->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataA0->bsonTypeSet == MatcherTypeSet(BSONType::String));

    // "b.0.c" should also be encrypted and queryable.
    auto leafB0C = root->getNode(FieldRef{"b.0.c"});
    auto metadataB0C = leafB0C->getEncryptionMetadata();
    ASSERT(metadataB0C->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataB0C->bsonTypeSet == MatcherTypeSet(BSONType::String));
}

TEST_F(EncryptionSchemaTreeTest, Fle2CannotEncryptPrefixOfAnotherField) {
    BSONObj badPathPrefixFirst = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b",
                       "bsonType": "string"
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "string"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(badPathPrefixFirst),
                       AssertionException,
                       6338403);

    BSONObj badPathPrefixSecond = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "string"
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b",
                       "bsonType": "string"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(badPathPrefixSecond),
                       AssertionException,
                       6338403);

    BSONObj pathsAreIdentical = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "string"
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "string"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(pathsAreIdentical),
                       AssertionException,
                       6338402);
}

TEST_F(EncryptionSchemaTreeTest, Fle2CannotParseEmptyFieldPath) {
    BSONObj emptyFieldName = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "",
                       "bsonType": "string"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(emptyFieldName),
                       AssertionException,
                       6316402);
}

TEST_F(EncryptionSchemaTreeTest, Fle2BSONTypeMustBeSingleValue) {
    BSONObj bsonTypeList = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": ["string", "int"]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(bsonTypeList),
                       AssertionException,
                       ErrorCodes::TypeMismatch);
}

TEST_F(EncryptionSchemaTreeTest, Fle2BadBSONTypeForEncryptedField) {
    // Certain BSON types should not be allowed when the path is queryable.
    BSONObj badBSONTypeQueryable = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "object",
                       "queries": {"queryType": "equality"}
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(badBSONTypeQueryable),
                       AssertionException,
                       6338405);

    // However, those types are allowed when the path is not queryable.
    BSONObj badBSONTypeNotQueryable = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "object"
                   }]
           }
    )");
    auto result = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(badBSONTypeNotQueryable);
    auto metadataABC = result->getNode(FieldRef{"a.b.c"})->getEncryptionMetadata();
    ASSERT(metadataABC && metadataABC->algorithmIs(Fle2AlgorithmInt::kUnindexed));

    // Other BSON types are forbidden whether or not the path is queryable.
    BSONObj alwaysForbiddenBSONType = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b.c",
                       "bsonType": "null"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(alwaysForbiddenBSONType),
                       AssertionException,
                       6338406);
}

TEST_F(EncryptionSchemaTreeTest, Fle2CannotEncryptFieldPrefixedById) {
    BSONObj pathIsId = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "_id",
                       "bsonType": "object"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parseEncryptedFieldConfig(pathIsId), AssertionException, 6316403);

    BSONObj pathPrefixedById = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "_id.a.b",
                       "bsonType": "object"
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(pathPrefixedById),
                       AssertionException,
                       6316403);
}

TEST(EncryptionSchemaTreeTest, Fle2CannotGetMetadataForPathContainingEncryptedPrefix) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b",
                       "bsonType": "date"
                   }]
           }
    )");

    auto result = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_THROWS_CODE(
        result->getEncryptionMetadataForPath(FieldRef("a.b.c")), AssertionException, 51102);
}

TEST(EncryptionSchemaTreeTest, Fle2SupportedQueriesMustHaveSupportedType) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a.b",
                       "bsonType": "date",
                       "queries": {"queryType": "garbage"}
                   }]
           }
    )");

    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       ErrorCodes::BadValue);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBasicFunctional) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberInt: "0"},
                             "max": {$numberInt: "10"}
                            }
                        ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kRange));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberInt));

    checkRangeQueryTypeConfig(metadataBC->fle2SupportedQueries.value().front(),
                              NumberInt,
                              mongo::Value(int(0)),
                              mongo::Value(int(10)),
                              1);
}


TEST_F(EncryptionSchemaTreeTest, Fle2RangeBasicWithAdditionalEqualityIndex) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "string",
                       "queries": [
                           {"queryType": "equality"}
                       ]
                   }, {
                       "keyId": {$binary: "gkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberInt: "0"},
                             "max": {$numberInt: "10"}
                            }
                        ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto leafA = root->getNode(FieldRef{"a"});
    auto metadataA = leafA->getEncryptionMetadata();
    ASSERT(metadataA->algorithmIs(Fle2AlgorithmInt::kEquality));
    ASSERT(metadataA->bsonTypeSet == MatcherTypeSet(BSONType::String));

    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kRange));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberInt));
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeWithSparsityParam) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "min": {$numberInt: "-10"},
                             "max": {$numberInt: "20"},
                             "sparsity": 2}
                        ]
                   }]
           }
    )");
    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);

    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kRange));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberInt));

    checkRangeQueryTypeConfig(metadataBC->fle2SupportedQueries.value().front(),
                              NumberInt,
                              mongo::Value(int(-10)),
                              mongo::Value(int(20)),
                              2);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeWithContentionParam) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberInt: "-10"},
                             "max": {$numberInt: "20"},
                             "contention": 1}
                        ]
                   }]
           }
    )");
    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);

    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kRange));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberInt));
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeAllParams) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "min": {$numberInt: "-10"},
                             "max": {$numberInt: "20"},
                             "sparsity": 1,
                             "contention": 1}
                        ]
                   }]
           }
    )");
    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);

    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kRange));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::NumberInt));
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBoundsEqual) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberInt: "0"},
                             "max": {$numberInt: "0"}
                            }
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       6720005);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBadTypeStringForMinMax) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": "hi",
                             "max": "zebra"}
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       ErrorCodes::TypeMismatch);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBadTypeMismatchDoubleForBoundsIntForField) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": 0.04535245,
                             "max": 356356.245345}
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       7018200);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBoundsValueTooBigForCoercionToInt) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberLong: "0"},
                             "max": {$numberLong: "4147483647"}
                            }
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       7018200);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBoundsTypeWiderAndNotCoercible) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberLong: "40"},
                             "max": {$numberLong: "1200"}
                            }
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       7018200);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeMinLargerThanMax) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberInt: "10"},
                             "max": {$numberInt: "-2"}
                            }
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       6720005);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeBadTypeMinMaxDiffTypes) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "range",
                             "sparsity": 1,
                             "min": {$numberInt: "5"},
                             "max": 10.0}
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       7018201);
}

TEST_F(EncryptionSchemaTreeTest, Fle2EqualityUsingRangeParams) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "b.c",
                       "bsonType": "int",
                       "queries": [
                            {"queryType": "equality",
                             "sparsity": 1,
                             "min": {$numberInt: "5"},
                             "max": {$numberInt: "10"}
                            }
                        ]
                   }]
           }
    )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       6775205);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeDateExpectedBehavior) {
    BSONObj encryptedFields = fromjson(R"({
                "fields": [{
                        "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                        "path": "b.c",
                        "bsonType": "date",
                        "queries": [
                                {"queryType": "range",
                                 "sparsity": 1,
                                 "min": {$date: "2000-01-06T19:10:54.246Z"},
                                 "max": {$date: "2020-01-06T19:10:54.246Z"}
                                 }
                            ]
                    }]
            }
        )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);

    auto nodeB = root->getNode(FieldRef{"b"});
    ASSERT_FALSE(nodeB->getEncryptionMetadata());
    auto leafBC = nodeB->getNode(FieldRef{"c"});
    auto metadataBC = leafBC->getEncryptionMetadata();
    ASSERT(metadataBC->algorithmIs(Fle2AlgorithmInt::kRange));
    ASSERT(metadataBC->bsonTypeSet == MatcherTypeSet(BSONType::Date));
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeDateMinLargerThanMax) {
    BSONObj encryptedFields = fromjson(R"({
                "fields": [{
                        "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                        "path": "b.c",
                        "bsonType": "date",
                        "queries": [
                                {"queryType": "range",
                                 "sparsity": 1,
                                 "min": {$date: "2020-01-06T19:10:54.246Z"},
                                 "max": {$date: "2000-01-06T19:10:54.246Z"}
                                 }
                            ]
                    }]
            }
        )");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       6720005);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeFieldDateMiMnaxNot) {
    BSONObj encryptedFields = fromjson(R"({
                "fields": [{
                        "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                        "path": "b.c",
                        "bsonType": "date",
                        "queries": [
                                {"queryType": "range",
                                 "sparsity": 1,
                                 "min": 0,
                                 "max": 100}
                            ]
                    }]
            }
        )");

    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields),
                       AssertionException,
                       7018200);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeIntDefaults) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "int",
                       "queries": [ {"queryType": "range", "trimFactor": 1} ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto leaf = root->getNode(FieldRef{"a"});
    auto metadata = leaf->getEncryptionMetadata();

    checkRangeQueryTypeConfig(metadata->fle2SupportedQueries.value().front(),
                              NumberInt,
                              mongo::Value(std::numeric_limits<int>::min()),
                              mongo::Value(std::numeric_limits<int>::max()),
                              kExpectDefaultSparsity,
                              1);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeLongDefaults) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "long",
                       "queries": [ {"queryType": "range", "trimFactor": 1} ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto leaf = root->getNode(FieldRef{"a"});
    auto metadata = leaf->getEncryptionMetadata();
    checkRangeQueryTypeConfig(metadata->fle2SupportedQueries.value().front(),
                              NumberLong,
                              mongo::Value(std::numeric_limits<long long>::min()),
                              mongo::Value(std::numeric_limits<long long>::max()),
                              kExpectDefaultSparsity,
                              1);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeDateDefaults) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "date",
                       "queries": [ {"queryType": "range", "trimFactor": 1} ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto leaf = root->getNode(FieldRef{"a"});
    auto metadata = leaf->getEncryptionMetadata();
    checkRangeQueryTypeConfig(metadata->fle2SupportedQueries.value().front(),
                              Date,
                              mongo::Value(Date_t::min()),
                              mongo::Value(Date_t::max()),
                              kExpectDefaultSparsity,
                              1);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeDoubleDefaults) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "double",
                       "queries": [ {"queryType": "range", "trimFactor": 1} ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto leaf = root->getNode(FieldRef{"a"});
    auto metadata = leaf->getEncryptionMetadata();
    checkRangeQueryTypeConfig(metadata->fle2SupportedQueries.value().front(),
                              NumberDouble,
                              mongo::Value(std::numeric_limits<double>::lowest()),
                              mongo::Value(std::numeric_limits<double>::max()),
                              kExpectDefaultSparsity,
                              1);
}

TEST_F(EncryptionSchemaTreeTest, Fle2RangeDecimal128Defaults) {
    BSONObj encryptedFields = fromjson(R"({
               "fields": [{
                       "keyId": {$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"},
                       "path": "a",
                       "bsonType": "decimal",
                       "queries": [ {"queryType": "range", "trimFactor": 1} ]
                   }]
           }
    )");

    auto root = EncryptionSchemaTreeNode::parseEncryptedFieldConfig(encryptedFields);
    ASSERT_FALSE(root->getEncryptionMetadata());

    auto leaf = root->getNode(FieldRef{"a"});
    auto metadata = leaf->getEncryptionMetadata();
    checkRangeQueryTypeConfig(metadata->fle2SupportedQueries.value().front(),
                              NumberDecimal,
                              mongo::Value(Decimal128::kLargestNegative),
                              mongo::Value(Decimal128::kLargestPositive),
                              kExpectDefaultSparsity,
                              1);
}

TEST_F(EncryptionSchemaTreeTest, QueryAnalysisParams_MultiSchemaAssertConstraints) {
    NamespaceString ns = NamespaceString::createNamespaceString_forTest("testdb.coll_a");

    // Test 1: Legacy style json schema without isRemoteSchema fails.
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
               "jsonSchema": {}
        }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686706);

    // Test 2:  Must provide at least one of jsonSchema, encryptionInformation or
    // csfleEncryptionSchemas
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
               "isRemoteSchema": true 
           }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686705);

    // Test 3: Providing conflicting schemas is prohibited (legacy json schema w/ FLE2)
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
               "jsonSchema": {},
               "isRemoteSchema": true,
               "encryptionInformation": {
                    "type": 1,
                    "schema":{
                        "testdb.coll_a": {}
                        }
                }
           }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686708);

    // Test 4: Providing conflicting schemas is prohibited (legacy json schema w/ FLE1
    // csfleEncryptionSchemas)
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
               "jsonSchema": {},
               "isRemoteSchema": true,
               "csfleEncryptionSchemas": {}
           }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686707);

    // Test 5: Providing csfleEncryptionSchemas w/ isRemoteSchema is forbidden.
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
               "isRemoteSchema": true,
               "csfleEncryptionSchemas": {}
           }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686709);

    // Test 6: Providing encryptionInformation w/ isRemoteSchema is forbidden.
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
               "isRemoteSchema": true,
               "encryptionInformation": {
                    "type": 1,
                    "schema":{
                        "testdb.coll_a": {}
                        }
                }
           }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686709);

    // Test 7: Providing both encryptionInformation & csfleEncryptionSchemas is forbidden.
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
                "encryptionInformation": {
                    "type": 1,
                    "schema":{
                        "testdb.coll_a": {}
                        }
                },
                "csfleEncryptionSchemas": {}

           }
    )"),
                                               ns,
                                               true),
                       AssertionException,
                       9686710);

    // Test 8: Providing csfleEncryptionSchemas when expecting a single schema should uassert.
    ASSERT_THROWS_CODE(extractCryptdParameters(fromjson(R"({
                "csfleEncryptionSchemas": {}
           }
    )"),
                                               ns,
                                               false),
                       AssertionException,
                       9686713);
}

namespace {
// Validates that all expected fields exist as encrypted fields in the encryption schema.
void validateSchemaWithEncryptedFields(const EncryptionSchemaTreeNode& schemaTree,
                                       const std::vector<std::string>& encryptedFields,
                                       FleVersion fleVersion) {
    for (const auto& fieldname : encryptedFields) {
        auto metadata = schemaTree.getEncryptionMetadataForPath(FieldRef(fieldname));
        ASSERT_TRUE(metadata);
        if (fleVersion == FleVersion::kFle1) {
            ASSERT_FALSE(metadata->isFle2Encrypted());
        } else {
            ASSERT_TRUE(metadata->isFle2Encrypted());
        }
    }
}

// Validates that schema map contains all expected encrypted fields for each namespace in the
// schema.
void validateSchemaMapWithEncryptedFields(
    const EncryptionSchemaMap& schemaMap,
    const std::map<NamespaceString, std::vector<std::string>>& namespaceAndEncryptedFields,
    FleVersion fleVersion) {
    ASSERT_TRUE(schemaMap.size() == namespaceAndEncryptedFields.size());

    for (const auto& namespaceAndEncryptedField : namespaceAndEncryptedFields) {
        auto iter = schemaMap.find(namespaceAndEncryptedField.first);
        ASSERT_TRUE(iter != schemaMap.end());
        const auto& schema = iter->second;
        invariant(schema);
        validateSchemaWithEncryptedFields(*schema, namespaceAndEncryptedField.second, fleVersion);
    }
}

// Validates that provided params contain the expected variant type.
void validateQueryAnalysisParams(const QueryAnalysisParams& params, FleVersion fleVersion) {
    if (fleVersion == FleVersion::kFle1) {
        ASSERT_TRUE(params.fleVersion() == FleVersion::kFle1 &&
                    std::holds_alternative<QueryAnalysisParams::FLE1SchemaMap>(params.schema));
    } else {
        ASSERT_TRUE(params.fleVersion() == FleVersion::kFle2 &&
                    std::holds_alternative<QueryAnalysisParams::FLE2SchemaMap>(params.schema));
    }
}

/**
 * Test helper, validates extracting QueryAnalysisParams and parsing them as an EncryptionSchemaMap
 * or a single EncryptionSchemaTreeNode*, verifying the resulting schemas against the expected
 * results.
 */
void validateExtractParamsAndParse(const EncryptionSchemasTestData& testData,
                                   bool extractAsMultiSchema,
                                   bool testParseAsSchemaMap,
                                   bool testParseAsSingleSchema) {
    auto params = extractCryptdParameters(testData.cmdObj,
                                          testData.namespaceToEncryptedFieldsMap.begin()->first,
                                          extractAsMultiSchema);

    validateQueryAnalysisParams(params, testData.fleVersion);

    if (testParseAsSchemaMap) {
        auto schemaMap = EncryptionSchemaTreeNode::parse<EncryptionSchemaMap>(params);
        validateSchemaMapWithEncryptedFields(
            schemaMap, testData.namespaceToEncryptedFieldsMap, testData.fleVersion);
    }

    if (testParseAsSingleSchema) {
        ASSERT_TRUE(testData.namespaceToEncryptedFieldsMap.size() == 1U);

        auto schemaTree =
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params);
        ASSERT_TRUE(schemaTree);
        validateSchemaWithEncryptedFields(*schemaTree,
                                          testData.namespaceToEncryptedFieldsMap.begin()->second,
                                          testData.fleVersion);
    }
}
}  // namespace

/**
 * QueryAnalysisParams_ParseMultiSchemaAsSchemaMap_CsfleEncryptionSchemas: Test that extracting
 * csfleEncryptionSchemas as multi-schema succeeds, and can be parsed into a schema map matching our
 * expected results.
 */
TEST_F(EncryptionSchemaTreeTest,
       QueryAnalysisParams_ParseMultiSchemaAsSchemaMap_CsfleEncryptionSchemas) {
    validateExtractParamsAndParse(kCsfleEncryptionSchemasTestData,
                                  true /*extractAsMultiSchema*/,
                                  true /*testParseAsSchemaMap*/,
                                  false /*testParseAsSingleSchema*/);
}

/**
 * QueryAnalysisParams_ParseMultiSchemaAsSchemaMap_EncryptionInformation: Test that extracting
 * EncryptionInformation with multiple schemas as multi-schema succeeds, and can be parsed into a
 * schema map matching our expected results.
 */
TEST_F(EncryptionSchemaTreeTest,
       QueryAnalysisParams_ParseMultiSchemaAsSchemaMap_EncryptionInformation) {
    validateExtractParamsAndParse(kEncryptionInformationSchemasTestData,
                                  true /*extractAsMultiSchema*/,
                                  true /*testParseAsSchemaMap*/,
                                  false /*testParseAsSingleSchema*/);
}

/**
 * QueryAnalysisParams_ExtractMultiSchemaAsSingleSchema_EncryptionInformation: Test that extracting
 * multi schema encryption information as single schema fails. This test is required because
 * EncryptionInformation is also used in single schema commands, which forbid providing multiple
 * encryption schemas.
 */
TEST_F(EncryptionSchemaTreeTest,
       QueryAnalysisParams_ExtractMultiSchemaAsSingleSchema_EncryptionInformation) {
    {
        ASSERT_THROWS_CODE(
            extractCryptdParameters(
                kEncryptionInformationSchemasTestData.cmdObj,
                kEncryptionInformationSchemasTestData.namespaceToEncryptedFieldsMap.begin()->first,
                false),
            AssertionException,
            6327503);
    }
}

/**
 * QueryAnalysisParams_ParseMultiSchemaAsSingleSchema_CsfleEncryptionSchemas: Parsing a schema map
 * with multiple schemas into a single encryption schema is not supported, and fails with a tassert.
 */
DEATH_TEST(EncryptionSchemaTreeTest,
           QueryAnalysisParams_ParseMultiSchemaAsSingleSchema_CsfleEncryptionSchemas,
           "9686715") {
    auto params = extractCryptdParameters(
        kCsfleEncryptionSchemasTestData.cmdObj,
        kCsfleEncryptionSchemasTestData.namespaceToEncryptedFieldsMap.begin()->first,
        true);
    {
        ASSERT_THROWS_CODE(
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params),
            AssertionException,
            9686715);
    }
}

/**
 * QueryAnalysisParams_ParseMultiSchemaAsSingleSchema_EncryptionInformation: Parsing a schema map
 * with multiple schemas into a single encryption schema is not supported, and fails with a tassert.
 */
DEATH_TEST(EncryptionSchemaTreeTest,
           QueryAnalysisParams_ParseMultiSchemaAsSingleSchema_EncryptionInformation,
           "9686716") {
    auto params = extractCryptdParameters(
        kEncryptionInformationSchemasTestData.cmdObj,
        kEncryptionInformationSchemasTestData.namespaceToEncryptedFieldsMap.begin()->first,
        true);
    {
        ASSERT_THROWS_CODE(
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params),
            AssertionException,
            9686716);
    }
}

/**
 * QueryAnalysisParams_ExtractAndParseSingleSchema_LegacyCsfle: Test that a legacy style
 * csfle encryption schema can be extracted as both a single schema or multi schema
 * QueryAnalysisParams, and in each of those cases, the QueryAnalysisParams can be parsed into
 * either a single schema or a schema map with one entry.
 *
 * In production, we only expect the following scenarios:
 * 1) Single schema commands (i.e Find, Count, etc.): Receive legacy csfle schema, extract as a
 *    single schema QueryAnalysisParams, and parse into a single encryption schema.
 *
 * 2) Multi schema commands (Agg.): Receive a legacy single csfle schema, extract as a multi schema
 *    QueryAnalysisParams and parse into a schema map.
 *
 * However, this test also checks the unexpected code paths:
 * 3) Multi schema command (Agg.): Receive a legacy single csfle schema, extract as multi schema
 *    QueryAnalysisParams and parse into a single encryption schema.
 *
 * 4) Single schema commands (i.e Find, Count, etc.):  Receive legacy style csfle encryption schema,
 *    extract as single schema QueryAnalysisParams, parse into a schema map.
 */
TEST_F(EncryptionSchemaTreeTest, QueryAnalysisParams_ExtractAndParseSingleSchema_LegacyCsfle) {
    const EncryptionSchemasTestData testData{
        .cmdObj = fromjson(R"({
                        "jsonSchema": { 
                                type: "object",
                                properties: {
                                    a: {
                                        encrypt: {
                                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}]
                                        }
                                    }
                                }
                        },
                        "isRemoteSchema": false }
           }
    )"),
        .namespaceToEncryptedFieldsMap =
            {{NamespaceString::createNamespaceString_forTest("testdb.coll_a"), {"a"}}},
        .fleVersion = FleVersion::kFle1};

    // Extract legacy schema as multi schema and parse.
    validateExtractParamsAndParse(testData,
                                  true /*extractAsMultiSchema*/,
                                  true /*testParseAsSchemaMap*/,
                                  true /*testParseAsSingleSchema*/);

    // Extract legacy schema as single schema and parse.
    validateExtractParamsAndParse(testData,
                                  false /*extractAsMultiSchema*/,
                                  true /*testParseAsSchemaMap*/,
                                  true /*testParseAsSingleSchema*/);
}

/**
 * QueryAnalysisParams_ExtractAndParseSingleSchema_EncryptionInformation: Test that
 * an EncryptionInformation containing a single schema can be extracted as both a single schema
 * or multi schema QueryAnalysisParams, and in each of those cases, the QueryAnalysisParams can
 * be parsed into either a single schema or a schema map with one entry.
 *
 * In production, we only expect the following scenarios:
 * 1) Single schema commands (i.e Find, Count, etc.): Receive EncryptionInformation with one
 * entry, extract as a single schema QueryAnalysisParams, and parse into a single encryption
 * schema.
 *
 * 2) Multi schema commands (Agg.): Receive EncryptionInformation with one entry, extract as
 *    multi schema QueryAnalysisParams and parse it into a schema map.
 *
 * However, this test also checks the unexpected code paths:
 * 3) Multi schema command (Agg.): Receive EncryptionInformation with one entry, extract as
 *    multi schema QueryAnalysisParams and parse into a single encryption schema.
 *
 * 4) Single schema commands (i.e Find, Count, etc.): Receive EncryptionInformation with one
 *    entry, extract as single schema QueryAnalysisParams, parse into a schema map.
 */
TEST_F(EncryptionSchemaTreeTest,
       QueryAnalysisParams_ExtractAndParseSingleSchema_EncryptionInformation) {
    const EncryptionSchemasTestData testData{
        .cmdObj = fromjson(R"({
               "encryptionInformation": {
                    "type": 1,
                    "schema":{
                        "testdb.coll_a": {
                            "escCollection": "enxcol_.coll_a.esc",
                            "ecocCollection": "enxcol_.coll_a.ecoc",
                            "fields":
                                [
                                {
                                    "keyId": {
                                        "$uuid": "c7d050cb-e8c1-4108-8dd1-10f33f2c6dc3"
                                    },
                                    "path": "ssn_a",
                                    "bsonType": "string",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                },
                                {
                                    "keyId": {
                                        "$uuid": "a0d8e31d-8475-40bf-aefd-b0a8459080e1"
                                    },
                                    "path": "age_a",
                                    "bsonType": "long",
                                    "queries": { "queryType": "equality", "contention": 8 }
                                }
                                ]
                        }
                    }   
                }
           }
    )"),
        .namespaceToEncryptedFieldsMap =
            {{NamespaceString::createNamespaceString_forTest("testdb.coll_a"), {"ssn_a", "age_a"}}},
        .fleVersion = FleVersion::kFle2};

    // Test extract as multi schema and parse.
    validateExtractParamsAndParse(testData,
                                  true /*extractAsMultiSchema*/,
                                  true /*testParseAsSchemaMap*/,
                                  true /*testParseAsSingleSchema*/);

    // Test extract as single schema and parse.
    validateExtractParamsAndParse(testData,
                                  false /*extractAsMultiSchema*/,
                                  true /*testParseAsSchemaMap*/,
                                  true /*testParseAsSingleSchema*/);
}

}  // namespace
}  // namespace mongo
