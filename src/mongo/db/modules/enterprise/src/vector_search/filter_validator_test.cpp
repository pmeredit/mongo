/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "vector_search/document_source_vector_search.h"
#include "vector_search/filter_validator.h"

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/unittest/death_test.h"
#include "search/mongot_options.h"

namespace mongo {
namespace {

using FilterValidatorTest = AggregationContextFixture;

TEST_F(FilterValidatorTest, AllowGT) {
    auto filterObj = fromjson(R"({x: {$gt: 0.0}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowGTE) {
    auto filterObj = fromjson(R"({x: {$gte: 1.02}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowLT) {
    auto filterObj = fromjson(R"({x: {$lt: NumberLong(12345678)}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowLTE) {
    auto filterObj = fromjson(R"({x: {$lte: NumberInt(1234)}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowEquality) {
    auto filterObj = fromjson(R"({x: {$eq: NaN}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowScientificNotation) {
    auto filterObj = fromjson(R"({x: {$eq: 1e100}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowBooleanTrue) {
    auto filterObj = fromjson(R"({x: {$eq: true}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowBooleanFalse) {
    auto filterObj = fromjson(R"({x: {$ne: false}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, DisallowNEMatchingInvalidTypeDate) {
    auto filterObj = fromjson(R"({x: {$ne: Date(54)}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, AllowString) {
    auto filterObj = fromjson(R"({x: {$eq: "xyz"}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowIn) {
    auto filterObj = fromjson(R"({x: {$in: [0, 1, 2]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowNin) {
    auto filterObj = fromjson(R"({x: {$nin: [0, 1, 2]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, DisallowNinMatchingInvalidTypeDate) {
    auto filterObj = fromjson(R"({x: {$nin: [Date(54), Date(55)]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, AllowAnd) {
    auto filterObj = fromjson(R"({$and: [{x: {$gt: 0}}, {y: {$eq: false}}]})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowOr) {
    auto filterObj = fromjson(R"({$or: [{x: {$gt: 0}}, {y: {$eq: false}}]})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowNestedAndOr) {
    auto filterObj = fromjson(R"({$and: [{$or: [{x: {$gt: 0}}]}, {$or: [{y: {$eq: false}}]}]})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, AllowNestedOrAnd) {
    auto filterObj = fromjson(R"({$or: [{$and: [{x: {$gt: 0}}]}, {$and: [{y: {$eq: false}}]}]})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    validateVectorSearchFilter(exprWithStatus.getValue().get());
}

TEST_F(FilterValidatorTest, DisallowInvalidNestedAnd) {
    auto filterObj = fromjson(R"({$and: [{x: {$gt: 0}}, {$and: [{y: {$eq: Date(54)}}]}]})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowInvalidNestedOr) {
    auto filterObj = fromjson(R"({$or: [{x: {$gt: 0}}, {$and: [{y: {$exists: true}}]}]})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowAlwaysFalse) {
    auto filterObj = fromjson(R"({$alwaysFalse: 1})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowAlwaysTrue) {
    auto filterObj = fromjson(R"({$alwaysTrue: 1})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowBitsAllClear) {
    auto filterObj = fromjson(R"({x: {$bitsAllClear: [1, 5]}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowBitsAllSet) {
    auto filterObj = fromjson(R"({x: {$bitsAllSet: [1, 5]}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowBitsAnyClear) {
    auto filterObj = fromjson(R"({x: {$bitsAnyClear: [1, 5]}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowAnySet) {
    auto filterObj = fromjson(R"({x: {$bitsAnySet: [1, 5]}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowElemMatchObject) {
    auto filterObj = fromjson(R"({x: {$elemMatch: {y: "xyz", z: {$gte: 8}}}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowElemMatchArray) {
    auto filterObj = fromjson(R"({x: {$elemMatch: {$gte: 80, $lt: 85}}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowExists) {
    auto filterObj = fromjson(R"({x: {$exists: true}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowExpr) {
    auto filterObj = fromjson(R"({$expr: {$lt: ["$x", 1]}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowGeoNear) {
    auto filterObj = fromjson(
        R"({x: {$near: {$geometry: {type: "Point", coordinates: [0, 0]}, $maxDistance: 100}}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowGeoWithin) {
    auto filterObj = fromjson(
        R"({x: {$geoWithin: {$geometry: {type: 'Polygon', coordinates: [[[0, 0], [1, 1], [2,2], [0,0]]]}}}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowMod) {
    auto filterObj = fromjson(R"({x: {$mod: [4, 0]}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowNor) {
    auto filterObj = fromjson(R"({$nor: [{x: 0}, {y: true}]})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowRegex) {
    auto filterObj = fromjson(R"({x: {$regex: "2"}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowArraySize) {
    auto filterObj = fromjson(R"({x: {$size: 2}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowTextSearch) {
    auto filterObj = fromjson(R"({$text: {$search: "xyz"}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowType) {
    auto filterObj = fromjson(R"({x: {$type: 2}})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowWhere) {
    auto filterObj = fromjson(R"({$where: "function(){return true;}"})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

TEST_F(FilterValidatorTest, DisallowNumberDecimal) {
    auto filterObj = fromjson(R"({x: {$gte: NumberDecimal('1.02')}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowOperandTypeObject) {
    auto filterObj = fromjson(R"({x: {$eq: {y: 1}}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE_AND_WHAT(validateVectorSearchFilter(exprWithStatus.getValue().get()),
                                AssertionException,
                                7828301,
                                "Operand type is not supported for $vectorSearch: object");
}

TEST_F(FilterValidatorTest, DisallowOperandTypeObjectId) {
    auto filterObj = fromjson(R"({x: {$eq: ObjectId('000000000000000000000000')}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowOperandTypeDate) {
    auto filterObj = fromjson(R"({x: {$eq: Date(54)}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowOperandTypeArray) {
    auto filterObj = fromjson(R"({x: {$eq: ["a"]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowOperandTypeNull) {
    auto filterObj = fromjson(R"({x: {$eq: null}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowMatchingArrayInArray) {
    auto filterObj = fromjson(R"({x: {$in: [["a"]]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowMatchingDateInArray) {
    auto filterObj = fromjson(R"({x: {$in: [Date(54)]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828301);
}

TEST_F(FilterValidatorTest, DisallowMatchingRegexInArray) {
    auto filterObj = fromjson(R"({x: {$in: [/^a/]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    auto expr = std::move(exprWithStatus.getValue());
    ASSERT_THROWS_CODE(validateVectorSearchFilter(expr.get()), AssertionException, 7828303);
}

TEST_F(FilterValidatorTest, DisallowMatchingNullInArray) {
    auto filterObj = fromjson(R"({x: {$in: [null]}})");
    auto exprWithStatus = MatchExpressionParser::parse(filterObj, getExpCtx());
    auto expr = std::move(exprWithStatus.getValue());
    ASSERT_THROWS_CODE(validateVectorSearchFilter(expr.get()), AssertionException, 7828302);
}

TEST_F(FilterValidatorTest, RaisesAssertionErrorAtTopLevel) {
    auto filterObj = fromjson(R"({$nor: [{x: null}]})");
    auto exprWithStatus =
        MatchExpressionParser::parse(filterObj,
                                     getExpCtx(),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures);
    ASSERT_THROWS_CODE(
        validateVectorSearchFilter(exprWithStatus.getValue().get()), AssertionException, 7828300);
}

}  // namespace
}  // namespace mongo
