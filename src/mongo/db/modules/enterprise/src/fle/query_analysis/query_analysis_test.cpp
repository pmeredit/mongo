/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string.h>

#include "encryption_update_visitor.h"
#include "fle_test_fixture.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/query/collation/collator_interface_mock.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
static const BSONObj randomEncryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId"
                           << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))));
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
                           << "keyId"
                           << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))));

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
    	        v: 5
    	    })"));
}

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
    auto correctPlaceholder = buildEncryptPlaceholder(
        orig,
        ResolvedEncryptionInfo{
            EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none},
        EncryptionPlaceholderContext::kWrite,
        nullptr);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["foo"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, randomEncryptObj, doc["foo"]);
}

TEST(ReplaceEncryptedFieldsTest, ReplacesSecondLevelFieldCorrectly) {
    auto schema = BSON("properties" << BSON("a" << BSON("type"
                                                        << "object"
                                                        << "properties"
                                                        << BSON("b" << randomEncryptObj))
                                                << "c"
                                                << BSONObj())
                                    << "type"
                                    << "object");
    auto doc = BSON("a" << BSON("b"
                                << "foo")
                        << "c"
                        << "bar");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["a"]["b"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, randomEncryptObj, doc["a"]["b"]);
    BSONElement notEncryptedElem = replaceRes.result["c"];
    ASSERT_FALSE(notEncryptedElem.type() == BSONType::BinData);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentTreatedAsFieldName) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << randomEncryptObj))));
    auto doc = BSON("foo" << BSON(0 << "encrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["foo"][0];
    assertEncryptedCorrectly(replaceRes, encryptedElem, randomEncryptObj, doc["foo"][0]);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentNotTreatedAsArrayIndex) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto doc = BSON("foo" << BSON_ARRAY("notEncrypted"));
    ASSERT_THROWS_CODE(
        replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr),
        AssertionException,
        31006);
    doc = BSON("foo" << BSON_ARRAY(BSON(0 << "notEncrypted") << BSON(0 << "alsoNotEncrypted")));
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
                                             << "properties"
                                             << BSON("bar" << randomEncryptObj))));
    auto doc = BSON("foo" << BSON_ARRAY("bar"
                                        << "notEncrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_THROWS_CODE(
        replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr),
        AssertionException,
        51093);
}


TEST_F(FLETestFixture, VerifyCorrectBinaryFormatForGeneratedPlaceholder) {
    BSONObj placeholder = buildEncryptPlaceholder(BSON("foo" << 5).firstElement(),
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
        Value(5), kDefaultMetadata, EncryptionPlaceholderContext::kComparison, nullptr);

    ASSERT_EQ(binData.getType(), BSONType::BinData);
    auto binDataElem = binData.getBinData();
    ASSERT_EQ(binDataElem.type, BinDataType::Encrypt);

    verifyBinData(static_cast<const char*>(binDataElem.data), binDataElem.length);
}

TEST(BuildEncryptPlaceholderValueTest, FailsForJSONPointerEncryption) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{"/key"},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::NumberInt}};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(
                           Value(1), metadata, EncryptionPlaceholderContext::kComparison, nullptr),
                       AssertionException,
                       51093);
}

TEST(BuildEncryptPlaceholderValueTest, FailsForArray) {
    ResolvedEncryptionInfo metadata{EncryptSchemaKeyId{"/key"},
                                    FleAlgorithmEnum::kDeterministic,
                                    MatcherTypeSet{BSONType::Array}};
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(Value(BSON_ARRAY("value")),
                                               metadata,
                                               EncryptionPlaceholderContext::kComparison,
                                               nullptr),
                       AssertionException,
                       31009);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << "value");
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, EncryptSchemaAnyType(doc["foo"]));
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << BSON_ARRAY("value"));
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, EncryptSchemaAnyType(doc["foo"]));
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto doc = BSON("foo"
                    << "encrypt");
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, EncryptSchemaAnyType(doc["foo"]));
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
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << pointerEncryptObj << "key" << randomEncryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
                       << "properties"
                       << BSON("foo" << pointerEncryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto uuid = UUID::gen();
    BSONObjBuilder bob;
    bob.append("foo", "encrypt");
    uuid.appendToBuilder(&bob, "key");
    auto doc = bob.obj();

    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, EncryptSchemaAnyType(doc["foo"]));
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
    const auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto docToEncrypt = BSON("foo"
                             << "test"
                             << "key"
                             << 5);
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

TEST(EncryptionUpdateVisitorTest, ReplaceSingleFieldCorrectly) {
    BSONObj entry = BSON("$set" << BSON("foo"
                                        << "bar"
                                        << "baz"
                                        << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = buildBasicSchema(randomEncryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    ResolvedEncryptionInfo metadata{
        EncryptSchemaKeyId{{UUID::fromCDR(uuidBytes)}}, FleAlgorithmEnum::kRandom, boost::none};
    auto correctField = buildEncryptPlaceholder(
        entry["$set"]["foo"], metadata, EncryptionPlaceholderContext::kWrite, nullptr, entry);
    auto correctBSON = BSON("$set" << BSON("baz"
                                           << "boo"
                                           << "foo"
                                           << correctField["foo"]));
    ASSERT_BSONOBJ_EQ(correctBSON, newUpdate);
}

TEST(EncryptionUpdateVisitorTest, ReplaceMultipleFieldsCorrectly) {
    BSONObj entry = BSON("$set" << BSON("foo.bar" << 3 << "baz"
                                                  << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON("bar" << randomEncryptObj))
                                     << "baz"
                                     << randomEncryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON("bar" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
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
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = buildBasicSchema(randomEncryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

TEST(EncryptionUpdateVisitorTest, RenameWithEncryptedSourceOnlyFails) {
    BSONObj entry = BSON("$rename" << BSON("foo"
                                           << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = buildBasicSchema(randomEncryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

TEST(EncryptionUpdateVisitorTest, RenameWithNestedTargetEncryptFails) {
    BSONObj entry = BSON("$rename" << BSON("boo"
                                           << "foo.bar"));
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON("bar" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());
    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}

TEST(EncryptionUpdateVisitorTest, RenameWithNestedSourceEncryptFails) {
    BSONObj entry = BSON("$rename" << BSON("foo.bar"
                                           << "boo"));
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    UpdateDriver driver(expCtx);
    std::map<StringData, std::unique_ptr<ExpressionWithPlaceholder>> arrayFilters;
    driver.parse(entry, arrayFilters);

    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON("bar" << randomEncryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}
}  // namespace
}  // namespace mongo
