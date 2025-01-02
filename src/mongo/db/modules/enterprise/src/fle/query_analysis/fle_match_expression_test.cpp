/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "encryption_schema_tree.h"
#include "fle2_test_fixture.h"
#include "fle_match_expression.h"
#include "fle_test_fixture.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/json.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

const auto kInvalidExpressionCode = 51092;

using FLE1MatchExpressionTest = FLETestFixture;

TEST_F(FLE1MatchExpressionTest, MarksElementInEqualityAsEncrypted) {
    auto match = fromjson("{ssn: '5'}");
    auto encryptedObj = buildEncryptElem(match["ssn"], kDefaultMetadata);
    auto translatedMatch = BSON("ssn" << BSON("$eq" << encryptedObj.firstElement()));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksNestedElementInEqAsEncrypted) {
    auto match = fromjson("{'user.ssn': '5'}");
    auto encryptedObj = buildEncryptElem(match["user.ssn"], kDefaultMetadata);
    auto translatedMatch = BSON("user.ssn" << BSON("$eq" << encryptedObj.firstElement()));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementInRHSObjectOfEqExpression) {
    auto match = fromjson("{user: {$eq: {ssn: '5', notSsn: 1}}}");
    auto encryptedObj = buildEncryptElem("5", kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$eq" << BSON("ssn" << encryptedObj.firstElement() << "notSsn" << 1)));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementInNotExpression) {
    auto match = fromjson("{ssn: {$not: {$eq: '5'}}}");

    auto encryptedObj = buildEncryptElem("5", kDefaultMetadata);
    auto translatedMatch =
        BSON("ssn" << BSON("$not" << BSON("$eq" << encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementInNeExpression) {
    auto match = fromjson("{ssn: {$ne: '5'}}");

    auto encryptedObj = buildEncryptElem("5", kDefaultMetadata);
    auto translatedMatch =
        BSON("ssn" << BSON("$not" << BSON("$eq" << encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementInNorExpression) {
    auto match = fromjson("{$nor: [{ssn: {$eq: '5'}}]}");

    auto encryptedObj = buildEncryptElem("5", kDefaultMetadata);
    auto translatedMatch =
        BSON("$nor" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << encryptedObj.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementUnderTreeExpression) {
    auto match = fromjson("{$or: [{ssn: '5'}, {ssn: '6'}]}");

    auto encryptedObjIndex0 = buildEncryptElem("5", kDefaultMetadata);
    auto encryptedObjIndex1 = buildEncryptElem("6", kDefaultMetadata);
    auto translatedMatch = BSON(
        "$or" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << encryptedObjIndex0.firstElement()))
                            << BSON("ssn" << BSON("$eq" << encryptedObjIndex1.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksEncryptedFieldWithNonEncryptedSibling) {
    auto match = fromjson("{ssn: \"not a number\", unrelated: 5}");
    auto encryptedObj = buildEncryptElem(match["ssn"], kDefaultMetadata);
    auto translatedMatch =
        BSON("$and" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << encryptedObj.firstElement()))
                                  << BSON("unrelated" << BSON("$eq" << 5))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementOfInExpression) {
    auto match = fromjson("{ssn: {$in: ['encrypt this']}}");
    auto encryptedObj = buildEncryptElem("encrypt this"_sd, kDefaultMetadata);
    auto translatedMatch = BSON("ssn" << BSON("$in" << BSON_ARRAY(encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementOfInExpressionWithDottedPath) {
    auto match = fromjson("{'user.ssn': {$in: ['encrypt this']}}");
    auto encryptedObj = buildEncryptElem("encrypt this"_sd, kDefaultMetadata);
    auto translatedMatch =
        BSON("user.ssn" << BSON("$in" << BSON_ARRAY(encryptedObj.firstElement())));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MarksElementOfRHSObjectWithinInExpression) {
    auto match = fromjson("{user: {$in: ['do not encrypt', {ssn: 'encrypt this'}]}}");
    auto encryptedObj = buildEncryptElem("encrypt this"_sd, kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$in" << BSON_ARRAY("do not encrypt"
                                                << BSON("ssn" << encryptedObj.firstElement()))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, ElementWithEncryptedPrefixCorrectlyFails) {
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

TEST_F(FLE1MatchExpressionTest, DoesNotMarkNonEncryptedFieldsInEquality) {
    auto match = fromjson("{unrelated: {$eq: 5}}");
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), match);
}

TEST_F(FLE1MatchExpressionTest, ExistsAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$exists: true}}");
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultSsnSchema, match), match);
}

TEST_F(FLE1MatchExpressionTest, TypeNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$type: [5]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaTypeNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaType: [3]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaBinDataSubTypeNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaBinDataSubType: 6}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaUniqueItemsNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaUniqueItems: true}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaAllElemMatchFromIndexNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaAllElemMatchFromIndex: [2, {}]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaFmodNotAllowedOnEncryptedField) {
    auto match =
        fromjson("{ssn: {$_internalSchemaFmod: [NumberDecimal('2.3'), NumberDecimal('1.1')]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaMatchArrayIndexNotAllowedOnEncryptedField) {
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

TEST_F(FLE1MatchExpressionTest, InternalSchemaMaxItemsNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMaxItems: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaMaxLengthNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMaxLength: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaMinItemsNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMinItems: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaMinLengthNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaMinLength: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalExprEqNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalExprEq: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaEqNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$_internalSchemaEq: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, BitExpressionsNotAllowed) {
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

TEST_F(FLE1MatchExpressionTest, NonEqualityComparisonsNotAllowedOnEncryptedFields) {
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

TEST_F(FLE1MatchExpressionTest, NonEqualityComparisonsToObjectsWithEncryptedFieldsNotAllowed) {
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$gt: {ssn: '5'}}}")),
        AssertionException,
        51119);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$gte: {ssn: '5'}}}")),
        AssertionException,
        51119);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$lt: {ssn: '5'}}}")),
        AssertionException,
        51119);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{user: {$lte: {ssn: '5'}}}")),
        AssertionException,
        51119);
}

TEST_F(FLE1MatchExpressionTest, EqualityToRegexNotAllowed) {
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: /^1/}")),
                       AssertionException,
                       kInvalidExpressionCode);

    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: {$regex: '/^1/'}}")),
        AssertionException,
        kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, ModExpressionNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$mod: [5, 2]}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, ElemMatchObjectNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$elemMatch: {user: 'Ted'}}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 6890100);
}

TEST_F(FLE1MatchExpressionTest, ElemMatchValueNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$elemMatch: {$eq: 'Ted'}}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 6890100);
}

TEST_F(FLE1MatchExpressionTest, SizeExpressionNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$size: 5}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, GeoNearNotAllowedOnEncryptedField) {
    auto match = fromjson(
        "{ssn: {$near: {$maxDistance: 10, $geometry: {type: 'Point', coordinates: [0, 0]}}}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, GeoWithinNotAllowedOnEncryptedField) {
    auto match = fromjson("{ssn: {$within: {$box: [{x: 4, y:4}, [6,6]]}}}");
    ASSERT_THROWS_CODE(serializeMatchForEncryption(kDefaultSsnSchema, match),
                       AssertionException,
                       kInvalidExpressionCode);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaObjectMatchNotAllowed) {
    auto match = fromjson("{notSsn: {$_internalSchemaObjectMatch: {nested: 1}}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLE1MatchExpressionTest, InternalSchemaAllowedPropertiesNotAllowed) {
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

TEST_F(FLE1MatchExpressionTest, TextExpressionNotAllowed) {
    auto match = fromjson("{$text: {$search: 'banana bread'}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLE1MatchExpressionTest, WhereExpressionNotAllowed) {
    auto match = fromjson("{$where: 'this.a == this.b'}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51094);
}

TEST_F(FLE1MatchExpressionTest, ComparisonToNullNotAllowed) {
    auto match = fromjson("{ssn: null}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51095);

    match = fromjson("{ssn: {$ne: null}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51095);
}

TEST_F(FLE1MatchExpressionTest, NullElementWithinInExpressionNotAllowed) {
    auto match = fromjson("{ssn: {$in: [null]}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51120);
}

TEST_F(FLE1MatchExpressionTest, RegexElementWithinInExpressionNotAllowed) {
    auto match = fromjson("{ssn: {$in: [/^1/, 'not a regex']}}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(kDefaultSsnSchema, match), AssertionException, 51015);
}

TEST_F(FLE1MatchExpressionTest, RegexWithinInExpressionAllowedOnPrefixOfEncryptedField) {
    auto match = fromjson("{user: {$in: [/^a/, {ssn: 'encrypted'}]}}");
    auto encryptedObj = buildEncryptElem("encrypted"_sd, kDefaultMetadata);
    auto translatedMatch =
        BSON("user" << BSON("$in" << BSON_ARRAY(BSON("ssn" << encryptedObj.firstElement())
                                                << BSONRegEx("^a"))));
    ASSERT_BSONOBJ_EQ(serializeMatchForEncryption(kDefaultNestedSchema, match), translatedMatch);
}

TEST_F(FLE1MatchExpressionTest, MatchExpressionWithRandomizedAlgorithmFails) {
    auto randomSSNSchema = fromjson(R"({
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
    auto match = fromjson("{ssn: 5}");
    ASSERT_THROWS_CODE(
        serializeMatchForEncryption(randomSSNSchema, match), AssertionException, 51158);
}


using FLE2MatchExpressionRangeTest = FLE2TestFixture;

///
/// Open-ended range queries.
///

TEST_F(FLE2MatchExpressionRangeTest, TopLevelGte) {
    auto match = fromjson("{age: {$gte: 23}}");

    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);

    auto expected = BSON("age" << BSON("$gte" << marking.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, TopLevelGt) {
    auto match = fromjson("{age: {$gt: 23}}");
    auto marking =
        buildRangePlaceholder("age"_sd, 23, false, kMaxDouble, true, 0, Fle2RangeOperator::kGt);

    auto expected = BSON("age" << BSON("$gt" << marking.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, TopLevelLt) {
    auto match = fromjson("{age: {$lt: 23}}");
    auto marking =
        buildRangePlaceholder("age"_sd, kMinDouble, true, 23, false, 0, Fle2RangeOperator::kLt);

    auto expected = BSON("age" << BSON("$lt" << marking.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, TopLevelLte) {
    auto match = fromjson("{age: {$lte: 23}}");
    auto marking =
        buildRangePlaceholder("age"_sd, kMinDouble, true, 23, true, 0, Fle2RangeOperator::kLte);

    auto expected = BSON("age" << BSON("$lte" << marking.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    auto actualPlaceholder = parseFLE2Placeholder(actual["age"]["$lte"]);
    auto expectedPlaceholder = parseFLE2Placeholder(expected["age"]["$lte"]);
    ASSERT_BSONOBJ_EQ(actualPlaceholder.toBSON(), expectedPlaceholder.toBSON());

    ASSERT_BSONOBJ_EQ(actual, expected);
}

// Verify that logical operators are traversed as expected.

TEST_F(FLE2MatchExpressionRangeTest, GteUnderAnd) {
    auto match = fromjson("{$and: [{age: {$gte: 23}}]}");
    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);

    auto expected =
        BSON("$and" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement()))));
    auto actual = markMatchExpression(kAgeFields, match);

    auto actualPlaceholder = parseFLE2Placeholder(actual["$and"]["0"]["age"]["$gte"]);
    auto expectedPlaceholder = parseFLE2Placeholder(expected["$and"]["0"]["age"]["$gte"]);
    ASSERT_BSONOBJ_EQ(actualPlaceholder.toBSON(), expectedPlaceholder.toBSON());

    ASSERT_BSONOBJ_EQ(actual, expected);
}
TEST_F(FLE2MatchExpressionRangeTest, GteUnderOr) {
    auto match = fromjson("{$or: [{age: {$gte: 23}}]}");

    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto expected =
        BSON("$or" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement()))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}
TEST_F(FLE2MatchExpressionRangeTest, GteUnderNot) {
    auto match = fromjson("{age: {$not: {$gte: 23}}}");

    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto expected = BSON("age" << BSON("$not" << BSON("$gte" << marking.firstElement())));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}
TEST_F(FLE2MatchExpressionRangeTest, GteUnderNor) {
    auto match = fromjson("{$nor: [{age: {$gte: 23}}]}");

    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto expected =
        BSON("$nor" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement()))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

// Verify that multiple levels of logical operators are traversed.

TEST_F(FLE2MatchExpressionRangeTest, GteUnderNestedAnd) {
    auto match = fromjson("{$and: [{$and: [{age: {$gte: 23}}]}]}");
    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);

    auto expected =
        BSON("$and" << BSON_ARRAY(BSON(
                 "$and" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement()))))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, GteUnderNestedOr) {
    auto match = fromjson("{$or: [{$or: [{age: {$gte: 23}}]}]}");

    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto expected =
        BSON("$or" << BSON_ARRAY(
                 BSON("$or" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement()))))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, GteUnderNestedNor) {
    auto match = fromjson("{$nor: [{$nor: [{age: {$gte: 23}}]}]}");

    auto marking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto expected =
        BSON("$nor" << BSON_ARRAY(BSON(
                 "$nor" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement()))))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

// Verify that both query analysis passes work together as expected.
TEST_F(FLE2MatchExpressionRangeTest, OpenRangeWithEqualityQuery) {
    auto match = fromjson("{age: {$gte: 23}, ssn: \"ABC123\"}");
    auto rangeMarking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto equalityMarking = buildEqualityPlaceholder(kAllFields, "ssn"_sd, "ABC123");

    auto expected =
        BSON("$and" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << equalityMarking.firstElement()))
                                  << BSON("age" << BSON("$gte" << rangeMarking.firstElement()))));
    auto actual = markMatchExpression(kAllFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

///
/// Tests for closed ranges. These tests exercise codepaths that make use of the
/// IndexBoundsBuilder from the classic query planner.
///

TEST_F(FLE2MatchExpressionRangeTest, ExplicitTopLevelClosedRange) {
    auto match = fromjson("{$and: [{age: {$gte: 23}}, {age: {$lte: 35}}]}");
    auto marking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("age" << BSON("$gte" << marking.firstElement() << "$lte" << stub.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    auto actualPlaceholder = parseFLE2Placeholder(actual["$and"]["0"]["age"]["$gte"]);
    auto expectedPlaceholder = parseFLE2Placeholder(expected["age"]["$gte"]);
    ASSERT_BSONOBJ_EQ(actualPlaceholder.toBSON(), expectedPlaceholder.toBSON());

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

TEST_F(FLE2MatchExpressionRangeTest, RangesCannotBeCombined) {
    auto match = fromjson("{$and: [{age: {$gte: 25}}, {age: {$lte: 23}}]}");
    auto actual = markMatchExpression(kAgeFields, match);
    auto correctResult = BSON("$alwaysFalse" << 1);
    ASSERT_BSONOBJ_EQ(correctResult, actual);

    match = fromjson("{$and: [{age: {$gte: 25}}, {age: {$lte: 23}}, {age: {$lte: 25}}]}");
    actual = markMatchExpression(kAgeFields, match);
    ASSERT_BSONOBJ_EQ(correctResult, actual);
}

// Verify that everything works as expected with implicit $and. This test parses identically to
// the one above, but from here on out, the shorthand will be used for convenience's sake.
TEST_F(FLE2MatchExpressionRangeTest, TopLevelClosedRange) {
    auto match = fromjson("{age: {$gte: 23, $lte: 35}}");

    auto marking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("age" << BSON("$gte" << marking.firstElement() << "$lte" << stub.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

// Equality on range index works as expected and generates a point placeholder.
TEST_F(FLE2MatchExpressionRangeTest, EqWithRangeIndexCreatesPlaceholder) {
    auto match = fromjson("{age: {$eq: 23}}");

    auto marking = buildRangePlaceholder(
        "age"_sd, 23, true, 23, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("age" << BSON("$gte" << marking.firstElement() << "$lte" << stub.firstElement()));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

///
/// Tests with multiple fields. Since query analysis can add/remove nodes from the query, these
/// tests verify that other predicates are preserved inside a $and.
///

TEST_F(FLE2MatchExpressionRangeTest, TopLevelClosedRangeWithUnencryptedField) {
    auto match = fromjson("{age: {$gte: 23, $lte: 35}, ssn: \"ABC123\"}");
    auto marking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("$and" << BSON_ARRAY(BSON("ssn" << BSON("$eq"
                                                     << "ABC123"))
                                  << BSON("age" << BSON("$gte" << marking.firstElement() << "$lte"
                                                               << stub.firstElement()))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

TEST_F(FLE2MatchExpressionRangeTest, TopLevelClosedRangeWithTwoRangePredicates) {
    auto match = fromjson("{age: {$gte: 23, $lte: 35}, salary: {$gte: 75000, $lte: 100000}}");
    auto ageMarking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto ageStub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto salaryMarking = buildRangePlaceholder("salary"_sd,
                                               75000,
                                               true,
                                               100000,
                                               true,
                                               1,
                                               Fle2RangeOperator::kGte,
                                               Fle2RangeOperator::kLte,
                                               getSalaryConfig(),
                                               kSalaryUUID());
    auto salaryStub = buildRangeStub("salary"_sd,
                                     1,
                                     Fle2RangeOperator::kGte,
                                     Fle2RangeOperator::kLte,
                                     getSalaryConfig(),
                                     kSalaryUUID());
    auto expected = normalizeMatchExpression(
        BSON("$and" << BSON_ARRAY(
                 BSON("age" << BSON("$gte" << ageMarking.firstElement() << "$lte"
                                           << ageStub.firstElement()))
                 << BSON("salary" << BSON("$gte" << salaryMarking.firstElement() << "$lte"
                                                 << salaryStub.firstElement())))));
    auto actual = markMatchExpression(kAgeAndSalaryFields, match);

    auto actualPlaceholder =
        parseFLE2Placeholder(actual["$and"]["1"]["$and"]["0"]["salary"]["$gte"]);
    auto expectedPlaceholder =
        parseFLE2Placeholder(expected["$and"]["1"]["$and"]["0"]["salary"]["$gte"]);
    ASSERT_BSONOBJ_EQ(actualPlaceholder.toBSON(), expectedPlaceholder.toBSON());

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, ClosedRangeWithEncryptedEqualityPredicate) {
    auto match = fromjson("{age: {$gte: 23, $lte: 35}, ssn: \"ABC123\"}");
    auto rangeMarking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto equalityMarking = buildEqualityPlaceholder(kAllFields, "ssn"_sd, "ABC123");
    auto expected =
        BSON("$and" << BSON_ARRAY(BSON("ssn" << BSON("$eq" << equalityMarking.firstElement()))
                                  << BSON("age" << BSON("$gte" << rangeMarking.firstElement()
                                                               << "$lte" << stub.firstElement()))));
    auto actual = markMatchExpression(kAllFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

///
/// Tests to ensure that closed ranges are properly processed under logical operators.
///

TEST_F(FLE2MatchExpressionRangeTest, DisjunctionOpenRangeWithEqualityQuery) {
    auto match = fromjson("{$or: [{age: {$gte: 23}}, {ssn: \"ABC123\"}]}");
    auto rangeMarking =
        buildRangePlaceholder("age"_sd, 23, true, kMaxDouble, true, 0, Fle2RangeOperator::kGte);
    auto equalityMarking = buildEqualityPlaceholder(kAllFields, "ssn"_sd, "ABC123");
    auto expected =
        BSON("$or" << BSON_ARRAY(BSON("age" << BSON("$gte" << rangeMarking.firstElement()))
                                 << BSON("ssn" << BSON("$eq" << equalityMarking.firstElement()))));
    auto actual = markMatchExpression(kAllFields, match);

    ASSERT_BSONOBJ_EQ(actual, expected);
}

TEST_F(FLE2MatchExpressionRangeTest, ClosedRangeUnderNestedAnd) {
    auto match = fromjson("{$and: [{age: {$gte: 23, $lte: 35}}]}");
    auto marking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("$and" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement() << "$lte"
                                                            << stub.firstElement()))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

TEST_F(FLE2MatchExpressionRangeTest, ClosedRangeUnderOr) {
    auto match = fromjson("{$or: [{age: {$gte: 23, $lte: 35}}]}");
    auto marking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("$or" << BSON_ARRAY(BSON("age" << BSON("$gte" << marking.firstElement() << "$lte"
                                                           << stub.firstElement()))));
    auto actual = markMatchExpression(kAgeFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

TEST_F(FLE2MatchExpressionRangeTest, UnencryptedPredicateInsideClosedRange) {
    auto match = fromjson(
        "{$and: [{age: {$gte: 23, $lte: 35}}, {$or: [{level: {$gte: 1, $lte: 5}}, {name: "
        "\"dev\"}]}]}");
    auto ageMarking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto stub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto expected =
        BSON("$and" << BSON_ARRAY(
                 BSON("age" << BSON("$gte" << ageMarking.firstElement() << "$lte"
                                           << stub.firstElement()))
                 << BSON("$or" << BSON_ARRAY(
                             BSON("$and" << BSON_ARRAY(BSON("level" << BSON("$gte" << 1))
                                                       << BSON("level" << BSON("$lte" << 5))))
                             << BSON("name" << BSON("$eq"
                                                    << "dev"))))));
    auto actual = markMatchExpression(kAgeAndSalaryFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

TEST_F(FLE2MatchExpressionRangeTest, ClosedRangeInsideOtherClosedRange) {
    auto match = fromjson(
        "{$and: [{age: {$gte: 23, $lte: 35}}, {$or: [{salary: {$gte: 75000, $lte: 100000}}, "
        "{name: "
        "\"dev\"}]}]}");
    auto ageMarking = buildRangePlaceholder(
        "age"_sd, 23, true, 35, true, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto ageStub = buildRangeStub("age"_sd, 0, Fle2RangeOperator::kGte, Fle2RangeOperator::kLte);
    auto salaryMarking = buildRangePlaceholder("salary"_sd,
                                               75000,
                                               true,
                                               100000,
                                               true,
                                               1,
                                               Fle2RangeOperator::kGte,
                                               Fle2RangeOperator::kLte,
                                               getSalaryConfig(),
                                               kSalaryUUID());
    auto salaryStub = buildRangeStub("salary"_sd,
                                     1,
                                     Fle2RangeOperator::kGte,
                                     Fle2RangeOperator::kLte,
                                     getSalaryConfig(),
                                     kSalaryUUID());
    auto expected =
        BSON("$and" << BSON_ARRAY(
                 BSON("age" << BSON("$gte" << ageMarking.firstElement() << "$lte"
                                           << ageStub.firstElement()))
                 << BSON("$or" << BSON_ARRAY(
                             BSON("salary" << BSON("$gte" << salaryMarking.firstElement() << "$lte"
                                                          << salaryStub.firstElement()))
                             << BSON("name" << BSON("$eq"
                                                    << "dev"))))));
    auto actual = markMatchExpression(kAgeAndSalaryFields, match);

    ASSERT_BSONOBJ_EQ(actual, normalizeMatchExpression(expected));
}

TEST_F(FLE2MatchExpressionRangeTest, RangeQueryWithoutRangeIndex) {
    ASSERT_THROWS_CODE(markMatchExpression(kSsnFields, fromjson("{ssn: {$gte: 23}}")),
                       AssertionException,
                       6721001);
    ASSERT_THROWS_CODE(markMatchExpression(kSsnFields, fromjson("{ssn: {$gte: 23, $eq: 123}}")),
                       AssertionException,
                       6720400);
}

TEST_F(FLE2MatchExpressionRangeTest, TopLevelUnderMinBoundFails) {
    std::vector<StringData> ops{"$gte", "$gt", "$lte", "$lt"};
    for (auto& op : ops) {
        auto match = BSON("age" << (BSON(op << -100)));
        ASSERT_THROWS_CODE(markMatchExpression(kAgeFields, match), AssertionException, 6747900);
    }
}

TEST_F(FLE2MatchExpressionRangeTest, TopLevelOverMaxBoundFails) {
    std::vector<StringData> ops{"$gte", "$gt", "$lte", "$lt"};
    for (auto& op : ops) {
        auto match = BSON("age" << (BSON(op << 1000)));
        ASSERT_THROWS_CODE(markMatchExpression(kAgeFields, match), AssertionException, 6747900);
    }
}
TEST_F(FLE2MatchExpressionRangeTest, ClosedPredicateUnderMinBoundFails) {
    std::vector<StringData> lbs{"$gte", "$gt"};
    std::vector<StringData> ubs{"$lte", "$lt"};
    for (auto& lb : lbs) {
        for (auto& ub : ubs) {
            auto match = BSON("age" << (BSON(ub << 35 << lb << -100)));
            ASSERT_THROWS_CODE(markMatchExpression(kAgeFields, match), AssertionException, 6747901);
        }
    }
}

TEST_F(FLE2MatchExpressionRangeTest, ClosedPredicateOverMaxBoundFails) {
    std::vector<StringData> lbs{"$gte", "$gt"};
    std::vector<StringData> ubs{"$lte", "$lt"};
    for (auto& lb : lbs) {
        for (auto& ub : ubs) {
            auto match = BSON("age" << (BSON(ub << 1000 << lb << 23)));
            ASSERT_THROWS_CODE(markMatchExpression(kAgeFields, match), AssertionException, 6747902);
        }
    }
}

}  // namespace
}  // namespace mongo
