/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

void assertEncryptedCorrectly(PlaceHolderResult response,
                              BSONElement elem,
                              BSONObj metadataobj,
                              BSONElement orig) {
    ASSERT_TRUE(response.hasEncryptionPlaceholders);
    int len;
    auto rawBinData = elem.binData(len);
    ASSERT_GT(len, 0);
    ASSERT_EQ(rawBinData[0], 0);
    ASSERT_TRUE(elem.isBinData(BinDataType::Encrypt));
    IDLParserErrorContext ctx("queryAnalysis");
    auto correctPlaceholder =
        buildEncryptPlaceholder(orig, EncryptionMetadata::parse(ctx, metadataobj["encrypt"].Obj()));
    ASSERT_BSONELT_EQ(correctPlaceholder[elem.fieldNameStringData()], elem);
}

TEST(IsEncryptionNeededTests, IsEncryptedNotPresent) {
    auto input = BSON("properties" << BSON("foo" << BSONObj()));

    ASSERT_FALSE(isEncryptionNeeded(input));
}

TEST(IsEncryptionNeededTests, isEncryptionNeededEmptyEncrypt) {
    uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
    auto encryptObj = BSON("keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID)) << "algorithm"
                                   << "AEAD_AES_256_CBC_HMAC_SHA_512-Random");
    auto input = BSON("properties" << BSON("a" << BSON("encrypt" << encryptObj)) << "type"
                                   << "object");
    ASSERT_TRUE(isEncryptionNeeded(input));
}


TEST(IsEncryptionNeededTests, isEncryptionNeededDeepEncrypt) {
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

TEST(ReplaceEncryptedFieldsTest, ReplacesTopLevelFieldCorrectly) {
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                             << "keyId"
                                             << BSON_ARRAY(UUID::gen())));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << encryptObj));
    auto doc = BSON("foo"
                    << "toEncrypt");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    BSONElement encryptedElem = replaceRes.result["foo"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["foo"]);
}

TEST(ReplaceEncryptedFieldsTest, ReplacesSecondLevelFieldCorrectly) {
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                             << "keyId"
                                             << BSON_ARRAY(UUID::gen())));
    auto schema = BSON("properties" << BSON("a" << BSON("type"
                                                        << "object"
                                                        << "properties"
                                                        << BSON("b" << encryptObj))
                                                << "c"
                                                << BSONObj())
                                    << "type"
                                    << "object");
    auto doc = BSON("a" << BSON("b"
                                << "foo")
                        << "c"
                        << "bar");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    BSONElement encryptedElem = replaceRes.result["a"]["b"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["a"]["b"]);
    BSONElement notEncryptedElem = replaceRes.result["c"];
    ASSERT_FALSE(notEncryptedElem.type() == BSONType::BinData);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentTreatedAsFieldName) {
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                             << "keyId"
                                             << BSON_ARRAY(UUID::gen())));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << encryptObj))));
    auto doc = BSON("foo" << BSON(0 << "encrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    BSONElement encryptedElem = replaceRes.result["foo"][0];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["foo"][0]);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentNotTreatedAsArrayIndex) {
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                             << "keyId"
                                             << BSON_ARRAY(UUID::gen())));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << encryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto doc = BSON("foo" << BSON_ARRAY("notEncrypted"));
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    ASSERT_FALSE(replaceRes.hasEncryptionPlaceholders);
    doc = BSON("foo" << BSON_ARRAY(BSON(0 << "notEncrypted") << BSON(0 << "alsoNotEncrypted")));
    replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    ASSERT_FALSE(replaceRes.hasEncryptionPlaceholders);
}

TEST(ReplaceEncryptedFieldsTest, ObjectInArrayWithSameNameNotEncrypted) {
    auto encryptObj = BSON("encrypt" << BSON("algorithm"
                                             << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                             << "keyId"
                                             << BSON_ARRAY(UUID::gen())));
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON("bar" << encryptObj))));
    auto doc = BSON("foo" << BSON_ARRAY("bar"
                                        << "notEncrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    ASSERT_BSONOBJ_EQ(doc, replaceRes.result);
}

}  // namespace
}  // namespace mongo
