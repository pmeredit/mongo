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

/**
 * The range intender can reorder children under a $and. As $and is commutative, prefer
 * checking that all elements are present rather than the order.
 */
bool unorderedConjunctionComparison(ExpressionAnd* correct, ExpressionAnd* testResult) {
    auto correctValue = correct->serialize(false);
    auto testValue = testResult->serialize(false);
    ASSERT(correctValue["$and"].isArray() && testValue["$and"].isArray());
    auto correctArray = correctValue["$and"].getArray();
    auto testArray = testValue["$and"].getArray();
    ASSERT_EQ(correctArray.size(), testArray.size());
    for (const auto& testVal : testArray) {
        bool found = false;
        for (const auto& correctVal : correctArray) {
            if (Value::compare(correctVal, testVal, nullptr) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
}


TEST_F(RangedAggregateExpressionIntender, SingleLeafExpressions) {
    // Constant.
    auto identityExprBSON = BSON("$const"
                                 << "hello");
    auto serializedExpr =
        markAggExpressionForRangeAndSerialize(identityExprBSON, false, Intention::NotMarked);

    // Encrypted path.
    auto ageExprBSON = BSON("$gt" << BSON_ARRAY("$age" << 5));
    serializedExpr = markAggExpressionForRangeAndSerialize(ageExprBSON, false, Intention::Marked);
    auto correctResult =
        buildAndSerializeEncryptedBetween("age"_sd, 5, false, kMaxDouble, true, getAgeConfig());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Nested encrypted path.
    ageExprBSON = BSON("$gt" << BSON_ARRAY("$nested.age" << 5));
    serializedExpr = markAggExpressionForRangeAndSerialize(ageExprBSON, false, Intention::Marked);
    correctResult = buildAndSerializeEncryptedBetween(
        "nested.age"_sd, 5, false, kMaxDouble, true, getAgeConfig());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Encrypted $neq with range index.
    auto ageNotExprBSON = BSON("$ne" << BSON_ARRAY("$age" << 5));
    serializedExpr =
        markAggExpressionForRangeAndSerialize(ageNotExprBSON, false, Intention::Marked);
    correctResult = Value(BSON("$not" << BSON_ARRAY(buildAndSerializeEncryptedBetween(
                                   "age"_sd, 5, true, 5, true, getAgeConfig()))));
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Encrypted $eq with range index.
    ageExprBSON = BSON("$eq" << BSON_ARRAY("$age" << 5));
    serializedExpr = markAggExpressionForRangeAndSerialize(ageExprBSON, false, Intention::Marked);
    correctResult = buildAndSerializeEncryptedBetween("age"_sd, 5, true, 5, true, getAgeConfig());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Non-encrypted path.
    auto plainExprBSON = BSON("$gt" << BSON_ARRAY("$randomPlainField" << 5));
    serializedExpr =
        markAggExpressionForRangeAndSerialize(plainExprBSON, false, Intention::NotMarked);
}

TEST_F(RangedAggregateExpressionIntender, TwoBelowAndExpression) {
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                                 << BSON("$lt" << BSON_ARRAY("$salary" << 50000))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 2, false, kMaxDouble, true, getAgeConfig());
    auto secondEncryptedBetween = buildEncryptedBetween(
        std::string("salary"), kMinDouble, true, 50000, false, getSalaryConfig());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween),
                                                            std::move(secondEncryptedBetween)};
    boost::intrusive_ptr<ExpressionAnd> andExprCorrect =
        make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, BelowEachCondChild) {
    auto condExprBson = BSON("$cond" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 50))
                                                   << BSON("$lte" << BSON_ARRAY("$salary" << 5000))
                                                   << BSON("$ne" << BSON_ARRAY("$age" << 25))));
    auto serializedExpr =
        markAggExpressionForRangeAndSerialize(condExprBson, false, Intention::Marked);
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
    ASSERT_THROWS_CODE(markAggExpressionForRangeAndSerialize(atanExpr, false, Intention::Marked),
                       AssertionException,
                       6331102);
}

TEST_F(RangedAggregateExpressionIntender, VariablesPermitted) {
    auto varExpr = ExpressionFieldPath::createVarFromString(
        getExpCtxRaw(), "NOW", getExpCtx()->variablesParseState);
    auto correctResult = varExpr->serialize(false);
    auto serializedExpr =
        markAggExpressionForRangeAndSerialize(varExpr, false, Intention::NotMarked);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, LetAndReducePreserveParentSubtree) {
    // Test Subtree preservation by marking through a $let 'in'
    auto eqLetExpr =
        BSON("$eq" << BSON_ARRAY(
                 "$unencrypted" << BSON(
                     "$let" << BSON("vars" << BSON("hello" << 5) << "in"
                                           << BSON("$gt" << BSON_ARRAY("$age" << 25))))));
    auto serializedExpr =
        markAggExpressionForRangeAndSerialize(eqLetExpr, false, Intention::Marked);
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
    serializedExpr = markAggExpressionForRangeAndSerialize(reduceExpr, false, Intention::Marked);
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
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(eqLetExpr, false, Intention::NotMarked),
        AssertionException,
        6331102);
}

TEST_F(RangedAggregateExpressionIntender, CmpForbidsTopLevelEncryptedValue) {
    auto cmpExprBson = BSON("$cmp" << BSON_ARRAY("$age" << 25));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(cmpExprBson, false, Intention::NotMarked),
        AssertionException,
        6331102);

    cmpExprBson = BSON("$cmp" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 25)) << false));
    auto serializedExpr =
        markAggExpressionForRangeAndSerialize(cmpExprBson, false, Intention::Marked);
    auto serializedBetween =
        buildAndSerializeEncryptedBetween("age"_sd, 25, false, kMaxDouble, true, getAgeConfig());
    auto correctResult = BSON("$cmp" << BSON_ARRAY(serializedBetween << BSON("$const" << false)));
    ASSERT_EQ(Value::compare(Value(correctResult), serializedExpr, nullptr), 0);
}

TEST_F(RangedAggregateExpressionIntender, NestedComparisonExpressions) {
    auto ageExprBSON =
        BSON("$gt" << BSON_ARRAY(BSON("$lt" << BSON_ARRAY("$age" << BSON("$const" << 55)))
                                 << BSON("$const" << false)));
    auto serializedExpr =
        markAggExpressionForRangeAndSerialize(ageExprBSON, false, Intention::Marked);
    auto serializedBetween =
        buildAndSerializeEncryptedBetween("age"_sd, kMinDouble, true, 55, false, getAgeConfig());
    auto correctResult = BSON("$gt" << BSON_ARRAY(serializedBetween << BSON("$const" << false)));
    ASSERT_EQ(Value::compare(Value(correctResult), serializedExpr, nullptr), 0);

    auto doubleEqExprBSON =
        BSON("$eq" << BSON_ARRAY(BSON("$eq" << BSON_ARRAY("$age" << 55))
                                 << BSON("$eq" << BSON_ARRAY("$unencrypted" << 20))));
    serializedExpr =
        markAggExpressionForRangeAndSerialize(doubleEqExprBSON, false, Intention::Marked);
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
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(cmpExprBSON, false, Intention::NotMarked),
        AssertionException,
        6334105);
}

TEST_F(RangedAggregateExpressionIntender, SimpleClosedInterval) {
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                                 << BSON("$lt" << BSON_ARRAY("$age" << 10))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 2, false, 10, false, getAgeConfig());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, MultiFieldClosedInterval) {
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                                 << BSON("$lt" << BSON_ARRAY("$age" << 10))
                                                 << BSON("$gte" << BSON_ARRAY("$salary" << 100))
                                                 << BSON("$lt" << BSON_ARRAY("$salary" << 1000))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 2, false, 10, false, getAgeConfig());
    auto secondEncryptedBetween =
        buildEncryptedBetween(std::string("salary"), 100, true, 1000, false, getSalaryConfig());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween),
                                                            std::move(secondEncryptedBetween)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, NonOverlappingExpressions) {
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 15))
                                                 << BSON("$lt" << BSON_ARRAY("$age" << 10))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 15, false, kMaxDouble, true, getAgeConfig());
    auto secondEncryptedBetween =
        buildEncryptedBetween(std::string("age"), kMinDouble, true, 10, false, getAgeConfig());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween),
                                                            std::move(secondEncryptedBetween)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, NonEncryptedFieldsUnchanged) {
    auto andExprBSON =
        BSON("$and" << BSON_ARRAY(BSON("$lt" << BSON_ARRAY("$age" << 15))
                                  << BSON("$gte" << BSON_ARRAY("$age" << 1))
                                  << BSON("$gt" << BSON_ARRAY("$unencrypted" << 25))
                                  << BSON("$lt" << BSON_ARRAY("$unencrypted" << 35))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 1, true, 15, false, getAgeConfig());
    auto firstUnencrypted = ExpressionCompare::create(
        getExpCtxRaw(),
        ExpressionCompare::CmpOp::GT,
        ExpressionFieldPath::createPathFromString(
            getExpCtxRaw(), "unencrypted", getExpCtxRaw()->variablesParseState),
        ExpressionConstant::create(getExpCtxRaw(), Value(25)));
    auto secondUnencrypted = ExpressionCompare::create(
        getExpCtxRaw(),
        ExpressionCompare::CmpOp::LT,
        ExpressionFieldPath::createPathFromString(
            getExpCtxRaw(), "unencrypted", getExpCtxRaw()->variablesParseState),
        ExpressionConstant::create(getExpCtxRaw(), Value(35)));
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween),
                                                            std::move(firstUnencrypted),
                                                            std::move(secondUnencrypted)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, ThreeClausesOnOneField) {
    // All can be combined.
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                                 << BSON("$gt" << BSON_ARRAY("$age" << 5))
                                                 << BSON("$lt" << BSON_ARRAY("$age" << 10))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    auto firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 5, false, 10, false, getAgeConfig());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedBetween)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));

    // One cannot be combined.
    andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                            << BSON("$gt" << BSON_ARRAY("$age" << 50))
                                            << BSON("$lt" << BSON_ARRAY("$age" << 10))));
    andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::Marked);
    firstEncryptedBetween =
        buildEncryptedBetween(std::string("age"), kMinDouble, true, 10, false, getAgeConfig());
    auto secondEncryptedBetween =
        buildEncryptedBetween(std::string("age"), 50, false, kMaxDouble, true, getAgeConfig());
    argVec = {std::move(firstEncryptedBetween), std::move(secondEncryptedBetween)};
    andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

}  // namespace
}  // namespace mongo
