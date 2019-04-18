/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string.h>

#include "encryption_update_visitor.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/query/collation/collator_interface_mock.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
static const BSONObj encryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId"
                           << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))));
static const BSONObj pointerEncryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId"
                           << "/key"));
static const BSONObj encryptMetadataDeterministicObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "initializationVector"
                           << BSONBinData(NULL, 0, BinDataType::BinDataGeneral)
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
        buildEncryptPlaceholder(orig,
                                EncryptionMetadata::parse(ctx, metadataobj["encrypt"].Obj()),
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
    auto schema = buildBasicSchema(encryptObj);
    auto doc = BSON("foo"
                    << "toEncrypt");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["foo"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["foo"]);
}

TEST(ReplaceEncryptedFieldsTest, ReplacesSecondLevelFieldCorrectly) {
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
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["a"]["b"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["a"]["b"]);
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
                                             << BSON(0 << encryptObj))));
    auto doc = BSON("foo" << BSON(0 << "encrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(
        doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, {}, boost::none, nullptr);
    BSONElement encryptedElem = replaceRes.result["foo"][0];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["foo"][0]);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentNotTreatedAsArrayIndex) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << encryptObj))));
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
                                             << BSON("bar" << encryptObj))));
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

TEST(BuildEncryptPlaceholderTest, JSONPointerResolvesCorrectly) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << "value");
    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, EncryptSchemaAnyType(doc["foo"]));
    auto keyAltName = BSON("key"
                           << "value");
    expected.setKeyAltName(EncryptSchemaAnyType(keyAltName["key"]));
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
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
    auto keyAltName = BSON("key"
                           << "value");
    expected.setKeyAltName(EncryptSchemaAnyType(keyAltName["key"]));
    EncryptionMetadata metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), localEncryptObj["encrypt"].Obj());
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
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
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
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
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
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
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
    auto keyAltName = BSON("key"
                           << "value");
    expected.setKeyAltName(EncryptSchemaAnyType(keyAltName["key"]));
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
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
                       << BSON("foo" << pointerEncryptObj << "key" << encryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << "value");
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
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
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    ASSERT_THROWS_CODE(buildEncryptPlaceholder(doc["foo"],
                                               metadata,
                                               EncryptionPlaceholderContext::kWrite,
                                               nullptr,
                                               doc,
                                               *schemaTree.get()),
                       AssertionException,
                       30037);
}

TEST(BuildEncryptPlaceholderTest, PointedToUUIDActsAsKeyIdInsteadOfAltName) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto uuid = UUID::gen();
    BSONObjBuilder bob;
    bob.append("foo", "encrypt");
    uuid.appendToBuilder(&bob, "key");
    auto doc = bob.obj();

    EncryptionPlaceholder expected(FleAlgorithmInt::kRandom, EncryptSchemaAnyType(doc["foo"]));
    expected.setKeyId(uuid);
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    auto response = buildEncryptPlaceholder(doc["foo"],
                                            metadata,
                                            EncryptionPlaceholderContext::kWrite,
                                            nullptr,
                                            doc,
                                            *schemaTree.get());
    auto correctBSON = encodePlaceholder("foo", expected);
    ASSERT_BSONOBJ_EQ(correctBSON, response);
}

TEST(BuildEncryptPlaceholderTest, FailsForRandomEncryptionInComparisonContext) {
    auto metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), encryptObj["encrypt"].Obj());
    auto doc = BSON("foo" << 1);
    ASSERT_THROWS_CODE(
        buildEncryptPlaceholder(
            doc.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr),
        AssertionException,
        51158);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForDeterministicEncryptionInComparisonContext) {
    auto metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                              encryptMetadataDeterministicObj["encrypt"].Obj());
    auto doc = BSON("foo" << 1);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForRandomEncryptionInWriteContext) {
    auto metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), encryptObj["encrypt"].Obj());
    auto doc = BSON("foo" << 1);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, SucceedsForDeterministicEncryptionInWriteContext) {
    auto metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                              encryptMetadataDeterministicObj["encrypt"].Obj());
    auto doc = BSON("foo" << 1);
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kWrite, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
}

TEST(BuildEncryptPlaceholderTest, FailsForStringWithNonSimpleCollationInComparisonContext) {
    auto metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                              encryptMetadataDeterministicObj["encrypt"].Obj());
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
    auto metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                              encryptMetadataDeterministicObj["encrypt"].Obj());

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
    auto metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                              encryptMetadataDeterministicObj["encrypt"].Obj());
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
    auto metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                              encryptMetadataDeterministicObj["encrypt"].Obj());
    auto doc = BSON("foo"
                    << "string");
    auto placeholder = buildEncryptPlaceholder(
        doc.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr);
    ASSERT_EQ(placeholder.firstElement().type(), BSONType::BinData);
    ASSERT_EQ(placeholder.firstElement().binDataType(), BinDataType::Encrypt);
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

    auto schema = buildBasicSchema(encryptObj);

    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    EncryptionMetadata metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), encryptObj["encrypt"].Obj());
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
                                             << BSON("bar" << encryptObj))
                                     << "baz"
                                     << encryptObj));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    EncryptionMetadata metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), encryptObj["encrypt"].Obj());
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
                                             << BSON("bar" << encryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());
    driver.visitRoot(&updateVisitor);
    auto newUpdate = driver.serialize().getDocument().toBson();
    EncryptionMetadata metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), encryptObj["encrypt"].Obj());
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

    auto schema = buildBasicSchema(encryptObj);

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

    auto schema = buildBasicSchema(encryptObj);

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
                                             << BSON("bar" << encryptObj))));
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
                                             << BSON("bar" << encryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto updateVisitor = EncryptionUpdateVisitor(*schemaTree.get());

    ASSERT_THROWS_CODE(driver.visitRoot(&updateVisitor), AssertionException, 51160);
}
}  // namespace
}  // namespace mongo
