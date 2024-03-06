/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */


#include "mongo/platform/basic.h"

#include <limits>
#include <string.h>

#include "encryption_update_visitor.h"
#include "fle2_test_fixture.h"
#include "fle_test_fixture.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_crypto.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/crypto/fle_fields_util.h"
#include "mongo/db/basic_types_gen.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/collation/collator_interface_mock.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo::query_analysis {
namespace {
static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
static const BSONObj randomEncryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))));
static const BSONObj pointerEncryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId"
                           << "/key"));
static const BSONObj pointerEncryptObjUsingDeterministicAlgo =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "keyId"
                           << "/key"));

static const BSONObj encryptMetadataDeterministicObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))));

const NamespaceString kTestEmptyNss = NamespaceString("");

/**
 * Builds a schema with a single encrypted field using the passed in 'encrypt'
 * specification. The 'encrypt' data is a single-element object of the format
 * {encrypt: {...}}.
 */
BSONObj buildBasicSchema(BSONObj encryptData) {
    return BSON("properties" << BSON("foo" << encryptData) << "type"
                             << "object");
}

void verifyBinData(const char* rawBuffer, int length) {
    ASSERT(rawBuffer);

    // First byte is the type, with '0' indicating that this is an intent-to-encrypt marking.
    ASSERT_GT(length, 1);
    ASSERT_EQ(rawBuffer[0], 0);

    // The remaining bytes are encoded as BSON.
    BSONObj placeholderBSON(&rawBuffer[1]);
    ASSERT_BSONOBJ_EQ(placeholderBSON, fromjson(R"({
    	        a: 1,
    	        ki: {$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                v: '5'
    	    })"));
}

void assertEncryptedCorrectly(ResolvedEncryptionInfo info,
                              const PlaceHolderResult& response,
                              BSONElement elem,
                              BSONElement orig,
                              EncryptedBinDataType subSubType) {
    ASSERT_TRUE(response.hasEncryptionPlaceholders);
    ASSERT_TRUE(elem.isBinData(BinDataType::Encrypt));
    int len;
    auto rawBinData = elem.binData(len);
    ASSERT_GT(len, 0);
    ASSERT_EQ(rawBinData[0], static_cast<int32_t>(subSubType));
    auto correctPlaceholder =
        buildEncryptPlaceholder(orig, info, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_BSONELT_EQ(correctPlaceholder[elem.fieldNameStringData()], elem);
}

BSONObj encodePlaceholder(std::string fieldName, EncryptionPlaceholder toSerialize) {
    BSONObjBuilder bob;
    toSerialize.serialize(&bob);
    auto markingObj = bob.done();

    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(0);
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        fieldName, binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

TEST(ReplaceEncryptedFieldsTest, ReplacesTopLevelFieldCorrectly) {
    auto schema = buildBasicSchema(randomEncryptObj);
    auto doc = BSON("foo"
                    << "toEncrypt");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["foo"];
    auto info = ResolvedEncryptionInfo{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    assertEncryptedCorrectly(
        info, std::move(replaceRes), encryptedElem, doc["foo"], EncryptedBinDataType::kPlaceholder);
}

TEST(ReplaceEncryptedFieldsTest, ReplacesSecondLevelFieldCorrectly) {
    auto schema =
        BSON("properties" << BSON("a" << BSON("type"
                                              << "object"
                                              << "properties" << BSON("b" << randomEncryptObj))
                                      << "c" << BSONObj())
                          << "type"
                          << "object");
    auto doc = BSON("a" << BSON("b"
                                << "foo")
                        << "c"
                        << "bar");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement notEncryptedElem = replaceRes.result["c"];
    ASSERT_FALSE(notEncryptedElem.type() == BSONType::BinData);
    BSONElement encryptedElem = replaceRes.result["a"]["b"];
    auto info = ResolvedEncryptionInfo{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    assertEncryptedCorrectly(info,
                             std::move(replaceRes),
                             encryptedElem,
                             doc["a"]["b"],
                             EncryptedBinDataType::kPlaceholder);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentTreatedAsFieldName) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("0" << randomEncryptObj))));
    auto doc = BSON("foo" << BSON("0"
                                  << "encrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["foo"]["0"];
    auto info = ResolvedEncryptionInfo{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    assertEncryptedCorrectly(info,
                             std::move(replaceRes),
                             encryptedElem,
                             doc["foo"]["0"],
                             EncryptedBinDataType::kPlaceholder);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentNotTreatedAsArrayIndex) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("0" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto doc = BSON("foo" << BSON_ARRAY("notEncrypted"));
    ASSERT_THROWS_CODE(
        replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr),
        AssertionException,
        31006);
    doc = BSON("foo" << BSON_ARRAY(BSON("0"
                                        << "notEncrypted")
                                   << BSON("0"
                                           << "alsoNotEncrypted")));
    ASSERT_THROWS_CODE(
        replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr),
        AssertionException,
        31006);
}

TEST(ReplaceEncryptedFieldsTest, ObjectInArrayWithSameNameNotEncrypted) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("bar" << randomEncryptObj))));
    auto doc = BSON("foo" << BSON_ARRAY("bar"
                                        << "notEncrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr),
        AssertionException,
        31006);
}

TEST(ReplaceEncryptedFieldsTest, FailIfSchemaHasKeyIdWithEmptyOrigDoc) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto doc = BSON("foo"
                    << "bar"
                    << "key"
                    << "string");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr),
        AssertionException,
        51093);
}


TEST_F(FLETestFixture, VerifyCorrectBinaryFormatForGeneratedPlaceholder) {
    BSONObj placeholder = buildEncryptPlaceholder(BSON("foo"
                                                       << "5")
                                                      .firstElement(),
                                                  kDefaultMetadata,
                                                  EncryptionPlaceholderContext::kComparison,
                                                  nullptr);
    auto binDataElem = placeholder.firstElement();
    ASSERT_EQ(binDataElem.type(), BSONType::BinData);
    ASSERT_EQ(binDataElem.binDataType(), BinDataType::Encrypt);

    int length = 0;
    auto rawBuffer = binDataElem.binData(length);
    verifyBinData(rawBuffer, length);
}

TEST_F(FLETestFixture, VerifyCorrectBinaryFormatForGeneratedPlaceholderWithValue) {
    Value binData = buildEncryptPlaceholder(
        Value("5"_sd), kDefaultMetadata, EncryptionPlaceholderContext::kComparison, nullptr);

    ASSERT_EQ(binData.getType(), BSONType::BinData);
    auto binDataElem = binData.getBinData();
    ASSERT_EQ(binDataElem.type, BinDataType::Encrypt);

    verifyBinData(static_cast<const char*>(binDataElem.data), binDataElem.length);
}

TEST(FLE2BuildEncryptPlaceholderValueTest, FailsForNonQueryableRandomEncryption) {
    auto metadata = ResolvedEncryptionInfo(UUID::fromCDR(uuidBytes), BSONType::String, boost::none);
    auto placeholderType = EncryptionPlaceholderContext::kComparison;
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(Value(1), metadata, placeholderType, nullptr),
                       AssertionException,
                       63165);
}

TEST(FLE2BuildEncryptPlaceholderValueTest, FailsForInconsistentTypes) {
    auto fle2Type = std::vector{QueryTypeConfig(QueryTypeEnum::Equality)};
    auto metadata = ResolvedEncryptionInfo(UUID::fromCDR(uuidBytes), BSONType::NumberInt, fle2Type);
    auto placeholderType = EncryptionPlaceholderContext::kComparison;
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(Value("s"_sd), metadata, placeholderType, nullptr),
                       AssertionException,
                       31118);
}

TEST(FLE2BuildEncryptPlaceholderValueTest, SucceedsForRandomQueryableEncryption) {
    auto fle2Type = std::vector{QueryTypeConfig(QueryTypeEnum::Equality)};
    auto metadata = ResolvedEncryptionInfo(UUID::fromCDR(uuidBytes), BSONType::String, fle2Type);
    auto placeholderType = EncryptionPlaceholderContext::kComparison;
    auto binData = buildEncryptPlaceholder(Value("string"_sd), metadata, placeholderType, nullptr);

    ASSERT_EQ(binData.getType(), BSONType::BinData);
    ASSERT_EQ(binData.getBinData().type, BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderValueTest, SucceedsForArrayWithRandomEncryption) {
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto placeholder = buildEncryptPlaceholder(
        Value(BSON_ARRAY("value")), metadata, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_EQ(placeholder.getType(), BSONType::BinData);
}

TEST(BuildEncryptPlaceholderValueTest, FailsForRandomEncryption) {
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(
                           Value(1), metadata, EncryptionPlaceholderContext::kComparison, nullptr),
                       AssertionException,
                       51158);
}

TEST(BuildEncryptPlaceholderValueTest, FailsForStringWithNonSimpleCollation) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::String}};
    auto collator =
        std::make_unique<CollatorInterfaceMock>(CollatorInterfaceMock::MockType::kReverseString);

    ASSERT_THROWS_CODE(buildEncryptPlaceholder(Value("string"_sd),
                                               metadata,
                                               EncryptionPlaceholderContext::kComparison,
                                               collator.get()),
                       AssertionException,
                       31054);
}

TEST(BuildEncryptPlaceholderValueTest, SucceedsForDeterministicEncryptionWithScalar) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::String}};
    auto binData = buildEncryptPlaceholder(
        Value("string"_sd), metadata, EncryptionPlaceholderContext::kComparison, nullptr);

    ASSERT_EQ(binData.getType(), BSONType::BinData);
    ASSERT_EQ(binData.getBinData().type, BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, JSONPointerResolvesCorrectly) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);

    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << "value");
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, IDLAnyType(doc["foo"]));
    expected.setKeyAltName("value"_sd);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    auto response = buildEncryptPlaceholder(doc["foo"],
                                            metadata,
                                            EncryptionPlaceholderContext::kWrite,
                                            nullptr,
                                            doc,
                                            *schemaTree.get());
    auto correctBSON = encodePlaceholder("foo", expected);
    ASSERT_BSONOBJ_EQ(correctBSON, response);
}

TEST(BuildEncryptPlaceholderTest, JSONPointerResolvesCorrectlyThroughArray) {
    auto localEncryptObj = BSON("encrypt" << BSON("algorithm"
                                                  << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                                  << "keyId"
                                                  << "/key/0"));
    auto schema = buildBasicSchema(localEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key" << BSON_ARRAY("value"));
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, IDLAnyType(doc["foo"]));
    expected.setKeyAltName("value"_sd);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key/0"}, FleAlgorithmEnum::kRandom, boost::none};
    auto response = buildEncryptPlaceholder(doc["foo"],
                                            metadata,
                                            EncryptionPlaceholderContext::kWrite,
                                            nullptr,
                                            doc,
                                            *schemaTree.get());
    auto correctBSON = encodePlaceholder("foo", expected);
    ASSERT_BSONOBJ_EQ(correctBSON, response);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToObject) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << BSON("Forbidden"
                            << "key"));
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       51115);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToArray) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    BSONObjBuilder builder;
    builder.append("foo", "encrypt");
    builder.appendCodeWScope("key",
                             "This is javascript code;",
                             BSON("Scope"
                                  << "Here"));
    auto doc = builder.obj();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       51115);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToCode) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << BSON_ARRAY("Forbidden"
                                  << "key"));
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       51115);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerDoesNotEvaluate) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto doc = BSON("foo"
                    << "encrypt");
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, IDLAnyType(doc["foo"]));
    expected.setKeyAltName("value"_sd);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       51114);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToEncryptedField) {
    auto schema =
        BSON("type"
             << "object"
             << "properties" << BSON("foo" << pointerEncryptObj << "key" << randomEncryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << "value");
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       30017);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToBinDataSubtypeSix) {
    auto schema = BSON("type"
                       << "object"
                       << "properties" << BSON("foo" << pointerEncryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    BSONObjBuilder bob;
    bob.append("foo", "encrypt");
    bob.appendBinData("key", 6, BinDataType::Encrypt, "123456");
    auto doc = bob.obj();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       51115);
}

TEST(BuildEncryptPlaceholderTest, FailsOnPointedToUUID) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto uuid = UUID::gen();
    BSONObjBuilder bob;
    bob.append("foo", "encrypt");
    uuid.appendToBuilder(&bob, "key");
    auto doc = bob.obj();

    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, IDLAnyType(doc["foo"]));
    expected.setKeyId(uuid);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       51115);
}

TEST(BuildEncryptPlaceholderTest, FailsForRandomEncryptionInComparisonContext) {
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto doc = BSON("foo" << 1);
    ASSERT_THROWS_CODE(
        buildEncryptPlaceholder(
            doc.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr),
        AssertionException,
        51158);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForDeterministicEncryptionInComparisonContext) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::NumberInt}};
    auto doc = BSON("foo" << 1);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForRandomEncryptionInWriteContext) {
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto doc = BSON("foo" << 1);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForDeterministicEncryptionInWriteContext) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::NumberInt}};
    auto doc = BSON("foo" << 1);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, FailsForStringWithNonSimpleCollationInComparisonContext) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::String}};
    auto doc = BSON("foo"
                    << "string");
    auto collator =
        std::make_unique<CollatorInterfaceMock>(CollatorInterfaceMock::MockType::kReverseString);
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc.firstElement(),
                                               metadata,
                                               EncryptionPlaceholderContext::kComparison,
                                               collator.get()),
                       AssertionException,
                       31054);
}

TEST(BuildEncryptPlaceholderTest, FailsForSymbolWithNonSimpleCollationInComparisonContext) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::Symbol}};

    BSONObjBuilder builder;
    builder.append("foo"_sd, "symbol"_sd);
    auto doc = builder.obj();
    auto collator =
        std::make_unique<CollatorInterfaceMock>(CollatorInterfaceMock::MockType::kReverseString);
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc.firstElement(),
                                               metadata,
                                               EncryptionPlaceholderContext::kComparison,
                                               collator.get()),
                       AssertionException,
                       31054);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForStringWithNonSimpleCollationInWriteContext) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::String}};
    auto doc = BSON("foo"
                    << "string");
    auto collator =
        std::make_unique<CollatorInterfaceMock>(CollatorInterfaceMock::MockType::kReverseString);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kWrite, collator.get());
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForStringWithSimpleCollationInComparisonContext) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::String}};
    auto doc = BSON("foo"
                    << "string");
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, FailsIfPointerPointsToNonString) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    const auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto docToEncrypt = BSON("foo"
                             << "test"
                             << "key" << 5);
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{"/key"}, FleAlgorithmEnum::kRandom, boost::none};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(docToEncrypt["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               docToEncrypt,
                                               *schemaTree.get()),
                       AssertionException,
                       51115);
}

TEST(BuildEncryptPlaceholderTest, ResolvedEncryptionInfoCannotIncludeTypeArrayWithDeterministic) {
    ASSERT_THROWS_CODE(ResolvedEncryptionInfo(EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                              FleAlgorithmEnum::kDeterministic,
                                              MatcherTypeSet{BSONType::Array}),
                       AssertionException,
                       31122);
}

TEST(BuildEncryptPlaceholderTest, ResolvedEncryptionInfoCanIncludeTypeArrayWithRandom) {
    ResolvedEncryptionInfo resolvedEncryptionInfo{EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}},
                                                  FleAlgorithmEnum::kRandom,
                                                  MatcherTypeSet{BSONType::Array}};
    ASSERT(resolvedEncryptionInfo.bsonTypeSet->hasType(BSONType::Array));
}

TEST(EncryptionUpdateVisitorTest, ReplaceSingleFieldCorrectly) {
    BSONObj entry = BSON("$set" << BSON("foo"
                                        << "bar"
                                        << "baz"
                                        << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = buildBasicSchema(randomEncryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto correctField = buildEncryptPlaceholder(
        entry["$set"]["foo"], metadata, EncryptionPlaceholderContext::kWrite, nullptr, entry);
    auto correctBSON = BSON("$set" << BSON("baz"
                                           << "boo"
                                           << "foo" << correctField["foo"]));
    ASSERT_BSONOBJ_EQ(correctBSON, newUpdate);
}

TEST(EncryptionUpdateVisitorTest, ReplaceMultipleFieldsCorrectly) {
    BSONObj entry = BSON("$set" << BSON("foo.bar" << 3 << "baz"
                                                  << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("bar" << randomEncryptObj))
                                     << "baz" << randomEncryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto correctBar = buildEncryptPlaceholder(
        entry["$set"]["foo.bar"], metadata, EncryptionPlaceholderContext::kWrite, nullptr, entry);
    auto correctBaz = buildEncryptPlaceholder(
        entry["$set"]["baz"], metadata, EncryptionPlaceholderContext::kWrite, nullptr, entry);
    auto correctBSON =
        BSON("$set" << BSON("baz" << correctBaz["baz"] << "foo.bar" << correctBar["foo.bar"]));
    ASSERT_BSONOBJ_EQ(correctBSON, newUpdate);
}

TEST(EncryptionUpdateVisitorTest, FieldMarkedForEncryptionInRightHandSetObject) {
    BSONObj entry = BSON("$set" << BSON("foo" << BSON("bar" << 5)));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("bar" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());
    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto correctField = buildEncryptPlaceholder(
        entry["$set"]["foo"]["bar"], metadata, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_BSONELT_EQ(newUpdate["$set"]["foo"]["bar"], correctField["bar"]);
}

TEST(EncryptionUpdateVisitorTest, RenameWithEncryptedTargetOnlyFails) {
    BSONObj entry = BSON("$rename" << BSON("boo"
                                           << "foo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = buildBasicSchema(randomEncryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

TEST(EncryptionUpdateVisitorTest, RenameWithEncryptedSourceOnlyFails) {
    BSONObj entry = BSON("$rename" << BSON("foo"
                                           << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = buildBasicSchema(randomEncryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

TEST(EncryptionUpdateVisitorTest, RenameWithNestedTargetEncryptFails) {
    BSONObj entry = BSON("$rename" << BSON("boo"
                                           << "foo.bar"));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("bar" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());
    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

TEST(EncryptionUpdateVisitorTest, RenameWithNestedSourceEncryptFails) {
    BSONObj entry = BSON("$rename" << BSON("foo.bar"
                                           << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(
        new ExpressionContext(nullptr, nullptr, kTestEmptyNss));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(write_ops::UpdateModification::parseFromClassicUpdate(entry), arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties" << BSON("bar" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

class RangePlaceholderTest : public FLE2TestFixture {
protected:
    QueryTypeConfig makeRangeQueryType() {
        auto query = QueryTypeConfig(QueryTypeEnum::Range);
        query.setMin(Value(0));
        query.setMax(Value(255));
        return query;
    }

    ResolvedEncryptionInfo makeMetadata() {
        return ResolvedEncryptionInfo(
            UUID::fromCDR(uuidBytes),
            BSONType::NumberInt,
            boost::optional<std::vector<QueryTypeConfig>>({makeRangeQueryType()}));
    }
};

TEST_F(RangePlaceholderTest, RoundtripPlaceholder) {
    auto range = BSON("" << BSON_ARRAY(23 << 35));
    auto arr = range.firstElement().Array();

    auto metadata = makeMetadata();
    auto expr = buildEncryptedBetweenWithPlaceholder(
        "age", metadata.keyId.uuids()[0], getAgeConfig(), {arr[0], true}, {arr[1], true});
    ASSERT_EQ(expr->path(), "age");

    auto idlObj = parseRangePlaceholder(expr->rhs());
    auto rangeSpec = getEncryptedRange(idlObj);

    ASSERT_TRUE(rangeSpec.getEdgesInfo());

    auto& edgesInfo = rangeSpec.getEdgesInfo().get();

    ASSERT_EQ(edgesInfo.getLowerBound().getElement().Int(), 23);
    ASSERT_EQ(edgesInfo.getUpperBound().getElement().Int(), 35);
}

TEST_F(RangePlaceholderTest, RoundtripPlaceholderWithInfiniteBounds) {
    auto range = BSON("" << BSON_ARRAY(23 << kMaxDouble));
    auto arr = range.firstElement().Array();
    auto metadata = makeMetadata();
    auto expr = buildEncryptedBetweenWithPlaceholder(
        "age", metadata.keyId.uuids()[0], getAgeConfig(), {arr[0], true}, {arr[1], true});
    ASSERT_EQ(expr->path(), "age");

    auto idlObj = parseRangePlaceholder(expr->rhs());
    auto rangeSpec = getEncryptedRange(idlObj);

    ASSERT_TRUE(rangeSpec.getEdgesInfo());

    auto& edgesInfo = rangeSpec.getEdgesInfo().get();

    ASSERT_EQ(edgesInfo.getLowerBound().getElement().Int(), 23);
    ASSERT_EQ(edgesInfo.getUpperBound().getElement().Double(), kMaxDouble.Double());
}
TEST_F(RangePlaceholderTest, RoundtripPlaceholderWithNegativeInfiniteBounds) {
    auto range = BSON("" << BSON_ARRAY(kMinDouble << 35));
    auto arr = range.firstElement().Array();
    auto metadata = makeMetadata();
    auto expr = buildEncryptedBetweenWithPlaceholder(
        "age", metadata.keyId.uuids()[0], getAgeConfig(), {arr[0], true}, {arr[1], true});
    ASSERT_EQ(expr->path(), "age");

    auto idlObj = parseRangePlaceholder(expr->rhs());
    auto rangeSpec = getEncryptedRange(idlObj);

    ASSERT_TRUE(rangeSpec.getEdgesInfo());

    auto& edgesInfo = rangeSpec.getEdgesInfo().get();

    ASSERT_EQ(edgesInfo.getLowerBound().getElement().Double(), kMinDouble.Double());
    ASSERT_EQ(edgesInfo.getUpperBound().getElement().Int(), 35);
}

TEST_F(RangePlaceholderTest, RoundtripWithNonzeroSparsity) {
    auto range = BSON("" << BSON_ARRAY(23 << 35));
    auto arr = range.firstElement().Array();
    auto metadata = makeMetadata();
    auto expr = buildEncryptedBetweenWithPlaceholder(
        "age", metadata.keyId.uuids()[0], getAgeConfig(), {arr[0], true}, {arr[1], true});
    ASSERT_EQ(expr->path(), "age");

    auto idlObj = parseRangePlaceholder(expr->rhs());
    auto rangeSpec = getEncryptedRange(idlObj);

    ASSERT_TRUE(rangeSpec.getEdgesInfo());

    auto& edgesInfo = rangeSpec.getEdgesInfo().get();

    ASSERT_EQ(edgesInfo.getLowerBound().getElement().Int(), 23);
    ASSERT_EQ(edgesInfo.getUpperBound().getElement().Int(), 35);
}

TEST_F(RangePlaceholderTest, ScalarAsValueParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << 6);
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj),
                       AssertionException,
                       6720200);
}


TEST_F(RangePlaceholderTest, ObjectAsValueParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << BSON("age" << 27));
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(
        FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj), AssertionException, 40415);
}

TEST_F(RangePlaceholderTest, ObjectWithNumericKeyAsValueParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << BSON("1" << 27));
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(
        FLE2EncryptionPlaceholder::parse(IDLParserContext("1"), obj), AssertionException, 40415);
}

TEST_F(RangePlaceholderTest, EmptyArrayAsValueParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << BSONObj());

    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(
        FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj), AssertionException, 40414);
}

TEST_F(RangePlaceholderTest, TooSmallArrayAsValueParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << BSON_ARRAY(2));
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(
        FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj), AssertionException, 40415);
}

TEST_F(RangePlaceholderTest, TooLargeArrayAsValueParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << BSON_ARRAY(1 << 2 << 3));
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(
        FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj), AssertionException, 40415);
}

TEST_F(RangePlaceholderTest, WithoutSparsityParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << BSON_ARRAY(1 << 2 << 3 << 4));
    auto arr = elt.firstElement().Array();

    FLE2RangeFindSpecEdgesInfo edgesInfo;
    edgesInfo.setLowerBound(arr[0]);
    edgesInfo.setLbIncluded(true);
    edgesInfo.setUpperBound(arr[1]);
    edgesInfo.setUbIncluded(true);
    edgesInfo.setIndexMin(arr[2]);
    edgesInfo.setIndexMax(arr[3]);
    FLE2RangeFindSpec spec;

    // TODO: change in SERVER-70305
    spec.setOperatorType(StringData("gt"));
    spec.setPayloadId(1234);

    auto specElt = BSON("" << spec.toBSON());
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kRange,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(specElt.firstElement()),
                                                 cm);

    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj),
                       AssertionException,
                       6832501);
}

TEST_F(RangePlaceholderTest, NonRangePlaceholderWithSparsityParseFails) {
    auto metadata = makeMetadata();
    auto ki = metadata.keyId.uuids()[0];
    auto cm = 0;

    auto elt = BSON("" << 4);
    auto placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                 Fle2AlgorithmInt::kEquality,
                                                 ki,
                                                 ki,
                                                 IDLAnyType(elt.firstElement()),
                                                 cm);
    placeholder.setSparsity(1);
    auto obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj),
                       AssertionException,
                       6832500);

    placeholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kInsert,
                                            Fle2AlgorithmInt::kUnindexed,
                                            ki,
                                            ki,
                                            IDLAnyType(elt.firstElement()),
                                            cm);
    placeholder.setSparsity(1);
    obj = placeholder.toBSON();
    ASSERT_THROWS_CODE(FLE2EncryptionPlaceholder::parse(IDLParserContext("age"), obj),
                       AssertionException,
                       6832500);
}

TEST_F(RangePlaceholderTest, QueryBoundCannotBeNaN) {
    {
        auto rangeBoundBSON =
            BSON("" << BSON_ARRAY(23.0 << std::numeric_limits<double>::signaling_NaN()
                                       << std::numeric_limits<double>::quiet_NaN()));

        auto rangeBoundElements = rangeBoundBSON.firstElement().Array();
        auto config = []() {
            auto query = QueryTypeConfig(QueryTypeEnum::Range);
            query.setMin(Value(0.0));
            query.setMax(Value(255.0));
            return query;
        }();
        auto metadata =
            ResolvedEncryptionInfo(UUID::fromCDR(uuidBytes),
                                   BSONType::NumberDouble,
                                   boost::optional<std::vector<QueryTypeConfig>>({config}));
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[0], true},
                                                                {rangeBoundElements[1], true}),
                           AssertionException,
                           6991000);
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[0], true},
                                                                {rangeBoundElements[2], true}),
                           AssertionException,
                           6991000);
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[1], true},
                                                                {rangeBoundElements[0], true}),
                           AssertionException,
                           6991000);
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[2], true},
                                                                {rangeBoundElements[0], true}),
                           AssertionException,
                           6991000);
    }
    {
        auto rangeBoundBSON =
            BSON("" << BSON_ARRAY(23.0 << Decimal128::kPositiveNaN << Decimal128::kNegativeNaN));

        auto rangeBoundElements = rangeBoundBSON.firstElement().Array();
        auto config = []() {
            auto query = QueryTypeConfig(QueryTypeEnum::Range);
            query.setMin(Value(Decimal128(0.0)));
            query.setMax(Value(Decimal128(255.0)));
            return query;
        }();
        auto metadata =
            ResolvedEncryptionInfo(UUID::fromCDR(uuidBytes),
                                   BSONType::NumberDecimal,
                                   boost::optional<std::vector<QueryTypeConfig>>({config}));
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[0], true},
                                                                {rangeBoundElements[1], true}),
                           AssertionException,
                           6991001);
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[0], true},
                                                                {rangeBoundElements[2], true}),
                           AssertionException,
                           6991001);
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[1], true},
                                                                {rangeBoundElements[0], true}),
                           AssertionException,
                           6991001);
        ASSERT_THROWS_CODE(buildEncryptedBetweenWithPlaceholder("age",
                                                                metadata.keyId.uuids()[0],
                                                                config,
                                                                {rangeBoundElements[2], true},
                                                                {rangeBoundElements[0], true}),
                           AssertionException,
                           6991001);
    }
}

using RangeInsertTest = FLE2TestFixture;

TEST_F(RangeInsertTest, BasicInsertMarking) {
    RAIIServerParameterControllerForTest controller("featureFlagFLE2Range", true);
    auto schemaTree = buildSchema(kAgeFields);
    auto metadata = schemaTree->getEncryptionMetadataForPath(FieldRef{"age"});
    auto doc = BSON("age" << 23);

    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["age"];
    assertEncryptedCorrectly(metadata.get(),
                             std::move(replaceRes),
                             encryptedElem,
                             doc["age"],
                             EncryptedBinDataType::kFLE2Placeholder);
    auto placeholder = parseRangePlaceholder(encryptedElem);
    auto rangeSpec = FLE2RangeInsertSpec::parse(IDLParserContext("spec"),
                                                placeholder.getValue().getElement().Obj());
    ASSERT_EQ(rangeSpec.getValue().getElement().Int(), doc["age"].Int());
    ASSERT_EQ(rangeSpec.getMinBound().value().getElement().Int(), 0);
    ASSERT_EQ(rangeSpec.getMaxBound().value().getElement().Int(), 200);
}

TEST_F(RangeInsertTest, NestedInsertMarking) {
    RAIIServerParameterControllerForTest controller("featureFlagFLE2Range", true);
    auto schemaTree = buildSchema(kNestedAge);
    auto metadata = schemaTree->getEncryptionMetadataForPath(FieldRef{"user.age"});
    auto doc = BSON("user" << BSON("age" << 23));

    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["user"]["age"];
    assertEncryptedCorrectly(metadata.get(),
                             std::move(replaceRes),
                             encryptedElem,
                             doc["user"]["age"],
                             EncryptedBinDataType::kFLE2Placeholder);
    auto placeholder = parseRangePlaceholder(encryptedElem);
    auto rangeSpec = FLE2RangeInsertSpec::parse(IDLParserContext("spec"),
                                                placeholder.getValue().getElement().Obj());
    ASSERT_EQ(rangeSpec.getValue().getElement().Int(), doc["user"]["age"].Int());
    ASSERT_EQ(rangeSpec.getMinBound().value().getElement().Int(), 0);
    ASSERT_EQ(rangeSpec.getMaxBound().value().getElement().Int(), 200);
}

TEST_F(RangeInsertTest, InsertMarkingWithRangeAndEquality) {
    RAIIServerParameterControllerForTest controller("featureFlagFLE2Range", true);

    auto doc = BSON("age" << 23 << "ssn"
                          << "abc123");
    auto schemaTree = buildSchema(kAllFields);
    {
        auto metadata = schemaTree->getEncryptionMetadataForPath(FieldRef{"age"});
        auto replaceRes = replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
        BSONElement encryptedElem = replaceRes.result["age"];
        assertEncryptedCorrectly(metadata.get(),
                                 std::move(replaceRes),
                                 encryptedElem,
                                 doc["age"],
                                 EncryptedBinDataType::kFLE2Placeholder);
        auto placeholder = parseRangePlaceholder(encryptedElem);
        auto rangeSpec = FLE2RangeInsertSpec::parse(IDLParserContext("spec"),
                                                    placeholder.getValue().getElement().Obj());
        ASSERT_EQ(rangeSpec.getValue().getElement().Int(), doc["age"].Int());
        ASSERT_EQ(rangeSpec.getMinBound().value().getElement().Int(), 0);
        ASSERT_EQ(rangeSpec.getMaxBound().value().getElement().Int(), 200);
    }
    {
        auto metadata = schemaTree->getEncryptionMetadataForPath(FieldRef{"ssn"});
        auto replaceRes = replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
        BSONElement encryptedElem = replaceRes.result["ssn"];
        assertEncryptedCorrectly(metadata.get(),
                                 std::move(replaceRes),
                                 encryptedElem,
                                 doc["ssn"],
                                 EncryptedBinDataType::kFLE2Placeholder);
    }
}
}  // namespace
}  // namespace mongo::query_analysis
