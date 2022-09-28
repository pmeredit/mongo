/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string>

#include "aggregate_expression_intender_range.h"
#include "fle2_test_fixture.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

using namespace std::string_literals;
using namespace aggregate_expression_intender;

using RangedAggregateExpressionIntender = FLE2TestFixture;


TEST_F(RangedAggregateExpressionIntender, SingleLeafExpressions) {
    // Constant.
    auto identityExprBSON = BSON("$const"
                                 << "hello");
    auto serializedExpr = markAggExpressionForRange(identityExprBSON, false, Intention::NotMarked);

    // Encrypted path.
    auto ageExprBSON = BSON("$gt" << BSON_ARRAY("$age" << 5));
    serializedExpr = markAggExpressionForRange(ageExprBSON, false, Intention::Marked);
    auto correctResult =
        buildAndSerializeEncryptedBetween("age"_sd, 5, false, kMaxDouble, true, getAgeConfig());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Nested encrypted path.
    ageExprBSON = BSON("$gt" << BSON_ARRAY("$nested.age" << 5));
    serializedExpr = markAggExpressionForRange(ageExprBSON, false, Intention::Marked);
    correctResult = buildAndSerializeEncryptedBetween(
        "nested.age"_sd, 5, false, kMaxDouble, true, getAgeConfig());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Encrypted $neq with range index.
    auto ageNotExprBSON = BSON("$ne" << BSON_ARRAY("$age" << 5));
    serializedExpr = markAggExpressionForRange(ageNotExprBSON, false, Intention::Marked);
    correctResult = Value(BSON("$not" << BSON_ARRAY(buildAndSerializeEncryptedBetween(
                                   "age"_sd, 5, true, 5, true, getAgeConfig()))));
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Encrypted $eq with range index.
    ageExprBSON = BSON("$eq" << BSON_ARRAY("$age" << 5));
    serializedExpr = markAggExpressionForRange(ageExprBSON, false, Intention::Marked);
    correctResult = buildAndSerializeEncryptedBetween("age"_sd, 5, true, 5, true, getAgeConfig());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Non-encrypted path.
    auto plainExprBSON = BSON("$gt" << BSON_ARRAY("$randomPlainField" << 5));
    serializedExpr = markAggExpressionForRange(plainExprBSON, false, Intention::NotMarked);
}

TEST_F(RangedAggregateExpressionIntender, TwoBelowAndExpression) {
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                                 << BSON("$lt" << BSON_ARRAY("$salary" << 50000))));
    auto serializedExpr = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 2, false, kMaxDouble, true, getAgeConfig());
    auto secondEncryptedBetween = buildEncryptedBetween(
        std::string("salary"), kMinDouble, true, 50000, false, getSalaryConfig());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween),
                                                            std::move(secondEncryptedBetween)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    auto correctResult = andExprCorrect->serialize(false);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, BelowEachCondChild) {
    auto condExprBson = BSON("$cond" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 50))
                                                   << BSON("$lte" << BSON_ARRAY("$salary" << 5000))
                                                   << BSON("$ne" << BSON_ARRAY("$age" << 25))));
    auto serializedExpr = markAggExpressionForRange(condExprBson, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 50, false, kMaxDouble, true, getAgeConfig());
    auto secondEncryptedBetween = buildEncryptedBetween(
        std::string("salary"), kMinDouble, true, 5000, true, getSalaryConfig());
    auto thirdEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 25, true, 25, true, getAgeConfig());
    std::vector<boost::intrusive_ptr<Expression>> notArgVec = {std::move(thirdEncryptedBetween)};
    auto equalityNotExpr = make_intrusive<ExpressionNot>(getExpCtxRaw(), std::move(notArgVec));
    auto condExprCorrect = ExpressionCond::create(getExpCtxRaw(),
                                                  std::move(firstEncryptedBetween),
                                                  std::move(secondEncryptedBetween),
                                                  std::move(equalityNotExpr));
    auto correctResult = condExprCorrect->serialize(false);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, RootLevelEvaluations) {
    // Using an encrypted field is not allowed in most contexts.
    auto atanExpr = BSON("$atan2" << BSON_ARRAY(BSON("$const" << 1) << "$age"));
    ASSERT_THROWS_CODE(
        markAggExpressionForRange(atanExpr, false, Intention::Marked), AssertionException, 6331102);
}

TEST_F(RangedAggregateExpressionIntender, VariablesPermitted) {
    auto varExpr = ExpressionFieldPath::createVarFromString(
        getExpCtxRaw(), "NOW", getExpCtx()->variablesParseState);
    auto correctResult = varExpr->serialize(false);
    auto serializedExpr = markAggExpressionForRange(varExpr, false, Intention::NotMarked);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, LetAndReducePreserveParentSubtree) {
    // Test Subtree preservation by marking through a $let 'in'
    auto eqLetExpr =
        BSON("$eq" << BSON_ARRAY(
                 "$unencrypted" << BSON(
                     "$let" << BSON("vars" << BSON("hello" << 5) << "in"
                                           << BSON("$gt" << BSON_ARRAY("$age" << 25))))));
    auto serializedExpr = markAggExpressionForRange(eqLetExpr, false, Intention::Marked);
    auto encryptedBetween =
        buildEncryptedBetween(std::string("age"), 25, false, kMaxDouble, true, getAgeConfig());
    auto eqLetExprCorrect =
        BSON("$eq" << BSON_ARRAY(
                 "$unencrypted" << BSON(
                     "$let" << BSON("vars" << BSON("hello" << BSON("$const" << 5)) << "in"
                                           << encryptedBetween->serialize(false)))));
    auto correctResult = Value(eqLetExprCorrect);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);
    // Test Subtree preservation by marking through a $reduce 'in'
    auto reduceExpr =
        BSON("$eq" << BSON_ARRAY(
                 "$unencrypted" << BSON(
                     "$reduce" << BSON("input" << BSON_ARRAY(3 << 5) << "initialValue"
                                               << BSON("$const" << 5) << "in"
                                               << BSON("$gt" << BSON_ARRAY("$age" << 25))))));
    serializedExpr = markAggExpressionForRange(reduceExpr, false, Intention::Marked);
    auto reduceExprCorrect = BSON(
        "$eq" << BSON_ARRAY(
            "$unencrypted" << BSON(
                "$reduce" << BSON("input" << BSON_ARRAY(BSON("$const" << 3) << BSON("$const" << 5))
                                          << "initialValue" << BSON("$const" << 5) << "in"
                                          << encryptedBetween->serialize(false)))));
    correctResult = Value(reduceExprCorrect);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, LetForbidsBindingToEncryptedValue) {
    auto eqLetExpr =
        BSON("$eq" << BSON_ARRAY(
                 "$unencrypted" << BSON(
                     "$let" << BSON("vars" << BSON("hello"
                                                   << "$age")
                                           << "in" << BSON("$gt" << BSON_ARRAY("$age" << 25))))));
    ASSERT_THROWS_CODE(markAggExpressionForRange(eqLetExpr, false, Intention::NotMarked),
                       AssertionException,
                       6331102);
}

TEST_F(RangedAggregateExpressionIntender, CmpForbidsTopLevelEncryptedValue) {
    auto cmpExprBson = BSON("$cmp" << BSON_ARRAY("$age" << 25));
    ASSERT_THROWS_CODE(markAggExpressionForRange(cmpExprBson, false, Intention::NotMarked),
                       AssertionException,
                       6331102);

    cmpExprBson = BSON("$cmp" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 25)) << false));
    auto serializedExpr = markAggExpressionForRange(cmpExprBson, false, Intention::Marked);
    auto serializedBetween =
        buildAndSerializeEncryptedBetween("age"_sd, 25, false, kMaxDouble, true, getAgeConfig());
    auto correctResult = BSON("$cmp" << BSON_ARRAY(serializedBetween << BSON("$const" << false)));
    ASSERT_EQ(Value::compare(Value(correctResult), serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, NestedComparisonExpressions) {
    auto ageExprBSON =
        BSON("$gt" << BSON_ARRAY(BSON("$lt" << BSON_ARRAY("$age" << BSON("$const" << 55)))
                                 << BSON("$const" << false)));
    auto serializedExpr = markAggExpressionForRange(ageExprBSON, false, Intention::Marked);
    auto serializedBetween =
        buildAndSerializeEncryptedBetween("age"_sd, kMinDouble, true, 55, false, getAgeConfig());
    auto correctResult = BSON("$gt" << BSON_ARRAY(serializedBetween << BSON("$const" << false)));
    ASSERT_EQ(Value::compare(Value(correctResult), serializedExpr, nullptr), 0);

    auto doubleEqExprBSON =
        BSON("$eq" << BSON_ARRAY(BSON("$eq" << BSON_ARRAY("$age" << 55))
                                 << BSON("$eq" << BSON_ARRAY("$unencrypted" << 20))));
    serializedExpr = markAggExpressionForRange(doubleEqExprBSON, false, Intention::Marked);
    serializedBetween =
        buildAndSerializeEncryptedBetween("age"_sd, 55, true, 55, true, getAgeConfig());
    correctResult =
        BSON("$eq" << BSON_ARRAY(serializedBetween << BSON(
                                     "$eq" << BSON_ARRAY("$unencrypted" << BSON("$const" << 20)))));
    ASSERT_EQ(Value::compare(Value(correctResult), serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, CompareFailsWithNonConstant) {
    auto cmpExprBSON = BSON("$gt" << BSON_ARRAY("$age"
                                                << "$unencrypted"));
    ASSERT_THROWS_CODE(markAggExpressionForRange(cmpExprBSON, false, Intention::NotMarked),
                       AssertionException,
                       6334105);
}

}  // namespace
}  // namespace mongo
