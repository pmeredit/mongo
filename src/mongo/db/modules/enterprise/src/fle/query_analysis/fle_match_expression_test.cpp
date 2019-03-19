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

#include "fle_match_expression.h"

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {

namespace {

const auto kInvalidExpressionCode = 51092;

class FLEMatchExpressionTest : public mongo::unittest::Test {
protected:
    void setUp() {
        kDefaultSsnSchema = fromjson(R"({
            type: "object", 
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
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
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                            }
                        }
                    }
                }
            }
        })");

        kDefaultMetadata =
            EncryptionMetadata::parse(IDLParserErrorContext("encryptMetadata"), fromjson(R"({
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", 
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            })"));
    }

    /**
     * Wraps 'value' in a BSONElement and returns a BSONObj representing the EncryptionPlaceholder.
     * The first element in the BSONObj will have a value of BinData sub-type 6 for the placeholder.
     */
    template <class T>
    BSONObj buildEncryptElem(T value, const EncryptionMetadata& metadata) {
        auto tempObj = BSON("v" << value);
        return buildEncryptPlaceholder(tempObj.firstElement(), metadata);
    }

    /**
     * Parses the given MatchExpression and replaces any unencrypted values with their appropriate
     * intent-to-encrypt marking according to the schema. Returns a serialization of the marked
     * MatchExpression.
     *
     * Throws an assertion if the schema is invalid or the expression is not allowed on an encrypted
     * field.
     */
    BSONObj serializeMatchForEncryption(const BSONObj& schema, const BSONObj& matchExpression) {
        auto expCtx(new ExpressionContextForTest());
        auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

        // By default, allow all features for testing.
        auto parsedMatch = uassertStatusOK(
            MatchExpressionParser::parse(matchExpression,
                                         expCtx,
                                         ExtensionsCallbackNoop(),
                                         MatchExpressionParser::kAllowAllSpecialFeatures));
        FLEMatchExpression fleMatchExpression{std::move(parsedMatch), *schemaTree};

        // Serialize the modified match expression.
        BSONObjBuilder bob;
        fleMatchExpression.getMatchExpression()->serialize(&bob);
        return bob.obj();
    }

    // Default schema where only the path 'ssn' is encrypted.
    BSONObj kDefaultSsnSchema;

    // Schema which defines a 'user' object with a nested 'ssn' encrypted field.
    BSONObj kDefaultNestedSchema;

    // Default metadata, see initialization above for actual values.
    EncryptionMetadata kDefaultMetadata;
};

TEST_F(FLEMatchExpressionTest, VerifyCorrectBinaryFormatForGeneratedPlaceholder) {
    BSONObj placeholder = buildEncryptElem(5, kDefaultMetadata);
    auto binDataElem = placeholder.firstElement();
    ASSERT_EQ(binDataElem.type(), BSONType::BinData);
    ASSERT_EQ(binDataElem.binDataType(), BinDataType::Encrypt);

    int length = 0;
    auto rawBuffer = binDataElem.binData(length);
    ASSERT(rawBuffer);

    // First byte is the type, with '0' indicating that this is an intent-to-encrypt marking.
    ASSERT_GT(length, 1);
    ASSERT_EQ(rawBuffer[0], 0);

    // The remaining bytes are encoded as BSON.
    BSONObj placeholderBSON(&rawBuffer[1]);
    ASSERT_BSONOBJ_EQ(placeholderBSON, fromjson(R"({
        a: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", 
        ki: {$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
        v: 5
    })"));
}

TEST_F(FLEMatchExpressionTest, MarksElementInEqualityAsEncrypted) {
    auto match = fromjson("{ssn: 5}");
    auto encryptedObj = buildEncryptElem(match["ssn"], kDefaultMetadata);
    auto translatedMatch = BSON("ssn" << BSON("$eq" << encryptedObj.firstElement()));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksNestedElementInEqAsEncrypted) {
    auto match = fromjson("{'user.ssn': 5}");
    auto encryptedObj = buildEncryptElem(match["user.ssn"], kDefaultMetadata);
    auto translatedMatch = BSON("user.ssn" << BSON("$eq" << encryptedObj.firstElement()));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementInRHSObjectOfEqExpression) {
    auto match = fromjson("{user: {$eq: {ssn: 5, notSsn: 1}}}");
    auto encryptedObj = buildEncryptElem(5, kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$eq" << BSON("ssn" << encryptedObj.firstElement() << "notSsn" << 1)));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementInNotExpression) {
    auto match = fromjson("{ssn: {$not: {$eq: 5}}}");

    auto encryptedObj = buildEncryptElem(5, kDefaultMetadata);
    auto translatedMatch =
        BSON("ssn" << BSON("$not" << BSON("$eq" << encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementInNeExpression) {
    auto match = fromjson("{ssn: {$ne: 5}}");

    auto encryptedObj = buildEncryptElem(5, kDefaultMetadata);
    auto translatedMatch =
        BSON("ssn" << BSON("$not" << BSON("$eq" << encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementInNorExpression) {
    auto match = fromjson("{$nor: [{ssn: {$eq: '5'}}]}");

    auto encryptedObj = buildEncryptElem("5", kDefaultMetadata);
    auto translatedMatch =
        BSON("$nor" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << encryptedObj.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementUnderTreeExpression) {
    auto match = fromjson("{$or: [{ssn: 5}, {ssn: 6}]}");

    auto encryptedObjIndex0 = buildEncryptElem(5, kDefaultMetadata);
    auto encryptedObjIndex1 = buildEncryptElem(6, kDefaultMetadata);
    auto translatedMatch = BSON(
        "$or" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << encryptedObjIndex0.firstElement()))
                            << BSON("ssn" << BSON("$eq" << encryptedObjIndex1.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksEncryptedFieldWithNonEncryptedSibling) {
    auto match = fromjson("{ssn: \"not a number\", unrelated: 5}");
    auto encryptedObj = buildEncryptElem(match["ssn"], kDefaultMetadata);
    auto translatedMatch =
        BSON("$and" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << encryptedObj.firstElement()))
                                  << BSON("unrelated" << BSON("$eq" << 5))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementOfInExpression) {
    auto match = fromjson("{ssn: {$in: ['encrypt this']}}");
    auto encryptedObj = buildEncryptElem("encrypt this"_sd, kDefaultMetadata);
    auto translatedMatch = BSON("ssn" << BSON("$in" << BSON_ARRAY(encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementOfInExpressionWithDottedPath) {
    auto match = fromjson("{'user.ssn': {$in: ['encrypt this']}}");
    auto encryptedObj = buildEncryptElem("encrypt this"_sd, kDefaultMetadata);
    auto translatedMatch =
        BSON("user.ssn" << BSON("$in" << BSON_ARRAY(encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementOfRHSObjectWithinInExpression) {
    auto match = fromjson("{user: {$in: ['do not encrypt', {ssn: 'encrypt this'}]}}");
    auto encryptedObj = buildEncryptElem("encrypt this"_sd, kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$in" << BSON_ARRAY("do not encrypt"
                                                << BSON("ssn" << encryptedObj.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, MarksElementOfRHSObjectWithNullWithinInExpression) {
    auto match = fromjson("{user: {$in: [{ssn: null}]}}");
    auto encryptedObj = buildEncryptElem(match["user"]["$in"]["0"]["ssn"], kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$in" << BSON_ARRAY(BSON("ssn" << encryptedObj.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLEMatchExpressionTest, ElementWithEncryptedPrefixCorrectlyFails) {
    auto match = fromjson("{'ssn.nested': {$eq: 5}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51102);

    match = fromjson("{'ssn.nested': {$gt: 5}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51102);

    match = fromjson("{'ssn.nested': {$lt: 5}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51102);

    match = fromjson("{'ssn.nested': {$ne: 5}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51102);

    match = fromjson("{'ssn.nested': {$not: {$eq: 5}}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51102);

    match = fromjson("{'ssn.nested': {$in: [5]}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51102);
}

TEST_F(FLEMatchExpressionTest, DoesNotMarkNonEncryptedFieldsInEquality) {
    auto match = fromjson("{unrelated: {$eq: 5}}");
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), match);
}

TEST_F(FLEMatchExpressionTest, ExistsAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$exists: true}}");
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), match);
}

TEST_F(FLEMatchExpressionTest, TypeNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$type: [5]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaTypeNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaType: [3]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaBinDataSubTypeNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaBinDataSubType: 6}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaUniqueItemsNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaUniqueItems: true}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaAllElemMatchFromIndexNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaAllElemMatchFromIndex: [2, {}]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaFmodNotAllowedOnEncryptedField) {
    auto match =
        fromjson("{ssn: {$_internalSchemaFmod: [NumberDecimal('2.3'), NumberDecimal('1.1')]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaMatchArrayIndexNotAllowedOnEncryptedField) {
    auto match = fromjson(R"({
        ssn: {
            $_internalSchemaMatchArrayIndex: {
                index: 0, 
                namePlaceholder: 'i', 
                expression: {i: {$gt: 7}}
            }
        }
    })");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaMaxItemsNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMaxItems: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaMaxLengthNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMaxLength: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaMinItemsNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMinItems: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaMinLengthNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMinLength: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalExprEqNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalExprEq: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaEqNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaEq: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, BitExpressionsNotAllowed) {
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$bitsAllClear: 5}}")),
        AssertionException,
        kInvalidExpressionCode);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$bitsAllSet: 5}}")),
        AssertionException,
        kInvalidExpressionCode);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$bitsAnyClear: 5}}")),
        AssertionException,
        kInvalidExpressionCode);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$bitsAnySet: 5}}")),
        AssertionException,
        kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, NonEqualityComparisonsNotAllowedOnEncryptedFields) {
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$gt: 5}}")),
                       AssertionException,
                       51118);

    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$gte: 5}}")),
                       AssertionException,
                       51118);

    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$lt: 5}}")),
                       AssertionException,
                       51118);

    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$lte: 5}}")),
                       AssertionException,
                       51118);
}

TEST_F(FLEMatchExpressionTest, NonEqualityComparisonsToObjectsWithEncryptedFieldsNotAllowed) {
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$gt: {ssn: 5}}}")),
        AssertionException,
        51119);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$gte: {ssn: 5}}}")),
        AssertionException,
        51119);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$lt: {ssn: 5}}}")),
        AssertionException,
        51119);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$lte: {ssn: 5}}}")),
        AssertionException,
        51119);
}

TEST_F(FLEMatchExpressionTest, EqualityToRegexNotAllowed) {
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: /^1/}")),
                       AssertionException,
                       kInvalidExpressionCode);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$regex: '/^1/'}}")),
        AssertionException,
        kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, ModExpressionNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$mod: [5, 2]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, ElemMatchObjectNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$elemMatch: {user: 'Ted'}}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, ElemMatchValueNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$elemMatch: {$eq: 'Ted'}}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, SizeExpressionNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$size: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, GeoNearNotAllowedOnEncryptedField) {
    auto match = fromjson(
        "{ssn: {$near: {$maxDistance: 10, $geometry: {type: 'Point', coordinates: [0, 0]}}}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, GeoWithinNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$within: {$box: [{x: 4, y:4}, [6,6]]}}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaObjectMatchNotAllowed) {
    auto match = fromjson("{notSsn: {$_internalSchemaObjectMatch: {nested: 1}}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLEMatchExpressionTest, InternalSchemaAllowedPropertiesNotAllowed) {
    auto match = fromjson(R"({
        $_internalSchemaAllowedProperties: {
            properties: ['ssn'],
            namePlaceholder: 'i', 
            patternProperties: [], 
            otherwise: {i: 0}
        }
    })");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLEMatchExpressionTest, TextExpressionNotAllowed) {
    auto match = fromjson("{$text: {$search: 'banana bread'}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLEMatchExpressionTest, WhereExpressionNotAllowed) {
    auto match = fromjson("{$where: 'this.a == this.b'}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLEMatchExpressionTest, ComparisonToNullNotAllowed) {
    auto match = fromjson("{ssn: null}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51095);

    match = fromjson("{ssn: {$ne: null}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51095);
}

TEST_F(FLEMatchExpressionTest, NullElementWithinInExpressionNotAllowed) {
    auto match = fromjson("{ssn: {$in: [null]}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51120);
}

TEST_F(FLEMatchExpressionTest, RegexElementWithinInExpressionNotAllowed) {
    auto match = fromjson("{ssn: {$in: [/^1/, 'not a regex']}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51015);
}

TEST_F(FLEMatchExpressionTest, RegexWithinInExpressionAllowedOnPrefixOfEncryptedField) {
    auto match = fromjson("{user: {$in: [/^a/, {ssn: 'encrypted'}]}}");
    auto encryptedObj = buildEncryptElem("encrypted"_sd, kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$in" << BSON_ARRAY(BSON("ssn" << encryptedObj.firstElement())
                                                << BSONRegEx("^a"))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

}  // namespace
}  // namespace mongo
