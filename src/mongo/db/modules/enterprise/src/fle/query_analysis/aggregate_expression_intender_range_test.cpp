/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "mongo/unittest/bson_test_util.h"
#include <string>

#include "aggregate_expression_intender_range.h"
#include "fle2_test_fixture.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

using namespace std::string_literals;
using namespace aggregate_expression_intender;

using RangedAggregateExpressionIntender = FLE2TestFixture;

/**
 * The range intender can reorder children under an $and or $or. As they are commutative, prefer
 * checking that all elements are present rather than the order.
 */
bool unorderedArrayComparison(std::vector<Value> correctArray, std::vector<Value> testArray) {
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

bool unorderedConjunctionComparison(ExpressionAnd* correct, ExpressionAnd* testResult) {
    auto correctValue = correct->serialize();
    auto testValue = testResult->serialize();
    ASSERT(correctValue["$and"].isArray() && testValue["$and"].isArray());
    auto correctArray = correctValue["$and"].getArray();
    auto testArray = testValue["$and"].getArray();
    return unorderedArrayComparison(correctArray, testArray);
}

bool unorderedDisjunctionComparison(ExpressionOr* correct, ExpressionOr* testResult) {
    auto correctValue = correct->serialize();
    auto testValue = testResult->serialize();
    ASSERT(correctValue["$or"].isArray() && testValue["$or"].isArray());
    auto correctArray = correctValue["$or"].getArray();
    auto testArray = testValue["$or"].getArray();
    return unorderedArrayComparison(correctArray, testArray);
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
        buildAndSerializeTwoSidedRange("age"_sd, 5, false, kMaxDouble, true, getAgeConfig(), 0);
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Nested encrypted path.
    ageExprBSON = BSON("$gt" << BSON_ARRAY("$nested.age" << 5));
    serializedExpr = markAggExpressionForRangeAndSerialize(ageExprBSON, false, Intention::Marked);
    correctResult = buildAndSerializeTwoSidedRange(
        "nested.age"_sd, 5, false, kMaxDouble, true, getAgeConfig(), 0, kSalaryUUID());
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Encrypted $neq with range index.
    auto ageNotExprBSON = BSON("$ne" << BSON_ARRAY("$age" << 5));
    serializedExpr =
        markAggExpressionForRangeAndSerialize(ageNotExprBSON, false, Intention::Marked);
    correctResult = Value(BSON("$not" << BSON_ARRAY(buildAndSerializeTwoSidedRange(
                                   "age"_sd, 5, true, 5, true, getAgeConfig(), 0))));
    ASSERT_EQ(Value::compare(correctResult, serializedExpr, nullptr), 0);

    // Encrypted $eq with range index.
    ageExprBSON = BSON("$eq" << BSON_ARRAY("$age" << 5));
    serializedExpr = markAggExpressionForRangeAndSerialize(ageExprBSON, false, Intention::Marked);
    correctResult = buildAndSerializeTwoSidedRange("age"_sd, 5, true, 5, true, getAgeConfig(), 0);
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
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 2, false, kMaxDouble, true, getAgeConfig(), 0);
    auto secondEncryptedRange = buildEncryptedRange(std::string("salary"),
                                                    kMinDouble,
                                                    true,
                                                    50000,
                                                    false,
                                                    getSalaryConfig(),
                                                    1,
                                                    kSalaryUUIDAgg());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedRange),
                                                            std::move(secondEncryptedRange)};
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
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 50, false, kMaxDouble, true, getAgeConfig(), 0);
    auto secondEncryptedRange = buildEncryptedRange(std::string("salary"),
                                                    kMinDouble,
                                                    true,
                                                    5000,
                                                    true,
                                                    getSalaryConfig(),
                                                    1,
                                                    kSalaryUUIDAgg());
    auto thirdEncryptedRange =
        buildEncryptedRange(std::string("age"), 25, true, 25, true, getAgeConfig(), 2);
    std::vector<boost::intrusive_ptr<Expression>> notArgVec = {std::move(thirdEncryptedRange)};
    auto equalityNotExpr = make_intrusive<ExpressionNot>(getExpCtxRaw(), std::move(notArgVec));
    auto condExprCorrect = ExpressionCond::create(getExpCtxRaw(),
                                                  std::move(firstEncryptedRange),
                                                  std::move(secondEncryptedRange),
                                                  std::move(equalityNotExpr));
    auto correctResult = condExprCorrect->serialize();
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
    auto correctResult = varExpr->serialize();
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
    auto encryptedRange =
        buildEncryptedRange(std::string("age"), 25, false, kMaxDouble, true, getAgeConfig(), 0);
    auto eqLetExprCorrect = BSON(
        "$eq" << BSON_ARRAY(
            "$unencrypted" << BSON("$let" << BSON("vars" << BSON("hello" << BSON("$const" << 5))
                                                         << "in" << encryptedRange->serialize()))));
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
                                          << encryptedRange->serialize()))));
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
        buildAndSerializeTwoSidedRange("age"_sd, 25, false, kMaxDouble, true, getAgeConfig(), 0);
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
        buildAndSerializeTwoSidedRange("age"_sd, kMinDouble, true, 55, false, getAgeConfig(), 0);
    auto correctResult = BSON("$gt" << BSON_ARRAY(serializedBetween << BSON("$const" << false)));
    ASSERT_EQ(Value::compare(Value(correctResult), serializedExpr, nullptr), 0);

    auto doubleEqExprBSON =
        BSON("$eq" << BSON_ARRAY(BSON("$eq" << BSON_ARRAY("$age" << 55))
                                 << BSON("$eq" << BSON_ARRAY("$unencrypted" << 20))));
    serializedExpr =
        markAggExpressionForRangeAndSerialize(doubleEqExprBSON, false, Intention::Marked);
    serializedBetween =
        buildAndSerializeTwoSidedRange("age"_sd, 55, true, 55, true, getAgeConfig(), 0);
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
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 2, false, 10, false, getAgeConfig(), 0);
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedRange)};
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
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 2, false, 10, false, getAgeConfig(), 0);
    auto secondEncryptedRange = buildEncryptedRange(
        std::string("salary"), 100, true, 1000, false, getSalaryConfig(), 1, kSalaryUUIDAgg());
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedRange),
                                                            std::move(secondEncryptedRange)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, NonOverlappingExpressions) {
    auto andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 15))
                                                 << BSON("$lt" << BSON_ARRAY("$age" << 10))));
    auto andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::NotMarked);
    auto firstSubExpr = make_intrusive<ExpressionConstant>(getExpCtxRaw(), Value(false));
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstSubExpr)};
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
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 1, true, 15, false, getAgeConfig(), 0);
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
    std::vector<boost::intrusive_ptr<Expression>> argVec = {
        std::move(firstEncryptedRange), std::move(firstUnencrypted), std::move(secondUnencrypted)};
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
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 5, false, 10, false, getAgeConfig(), 0);
    std::vector<boost::intrusive_ptr<Expression>> argVec = {std::move(firstEncryptedRange)};
    auto andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));

    // One cannot be combined.
    andExprBSON = BSON("$and" << BSON_ARRAY(BSON("$gt" << BSON_ARRAY("$age" << 2))
                                            << BSON("$gt" << BSON_ARRAY("$age" << 50))
                                            << BSON("$lt" << BSON_ARRAY("$age" << 10))));
    andExprTest = markAggExpressionForRange(andExprBSON, false, Intention::NotMarked);
    auto firstSubExpr = make_intrusive<ExpressionConstant>(getExpCtxRaw(), Value(false));
    argVec = {std::move(firstSubExpr)};
    andExprCorrect = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedConjunctionComparison(andExprCorrect.get(),
                                          dynamic_cast<ExpressionAnd*>(andExprTest.get())));
}

TEST_F(RangedAggregateExpressionIntender, InRewritesCorrectlyWithFieldPathAndConstant) {
    auto inExprBSON = BSON(
        "$in" << BSON_ARRAY("$age" << BSON_ARRAY(BSON("$const" << 1)
                                                 << BSON("$const" << 5) << BSON("$const" << 10))));
    auto inExprTest = markAggExpressionForRange(inExprBSON, false, Intention::Marked);
    auto firstEncryptedRange =
        buildEncryptedRange(std::string("age"), 1, true, 1, true, getAgeConfig(), 0);
    auto secondEncryptedRange =
        buildEncryptedRange(std::string("age"), 5, true, 5, true, getAgeConfig(), 1);
    auto thirdEncryptedRange =
        buildEncryptedRange(std::string("age"), 10, true, 10, true, getAgeConfig(), 2);
    std::vector<boost::intrusive_ptr<Expression>> argVec = {
        firstEncryptedRange, secondEncryptedRange, thirdEncryptedRange};
    auto inExprCorrect = make_intrusive<ExpressionOr>(getExpCtxRaw(), std::move(argVec));
    ASSERT(unorderedDisjunctionComparison(inExprCorrect.get(),
                                          dynamic_cast<ExpressionOr*>(inExprTest.get())));
}

// TODO SERVER-71093 Support $in below encrypted prefix.
TEST_F(RangedAggregateExpressionIntender, InFailsToRewriteEncryptedPrefix) {
    auto inExprBSON =
        BSON("$in" << BSON_ARRAY(
                 "$nested" << BSON_ARRAY(
                     BSON("age" << BSON("$const" << 1) << "other" << BSON("$const" << 3))
                     << BSON("age" << BSON("$const" << 5) << "other" << BSON("$const" << 7))
                     << BSON("age" << BSON("$const" << 10)))));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(inExprBSON, false, Intention::NotMarked),
        AssertionException,
        7036804);
}
// TEST_F(RangedAggregateExpressionIntender, InRewritesEncryptedPrefixCorrectly) {
//     auto inExprBSON =
//         BSON("$in" << BSON_ARRAY(
//                  "$nested" << BSON_ARRAY(
//                      BSON("age" << BSON("$const" << 1) << "other" << BSON("$const" << 3))
//                      << BSON("age" << BSON("$const" << 5) << "other" << BSON("$const" << 7))
//                      << BSON("age" << BSON("$const" << 10)))));
//     auto inExprTest = markAggExpressionForRange(inExprBSON, false, Intention::Marked);
//     auto firstEncryptedRange = buildEncryptedRange(
//         std::string("nested.age"), 1, true, 1, true, getAgeConfig(), kSalaryUUID());

//     auto firstUnencrypted = ExpressionCompare::create(
//         getExpCtxRaw(),
//         ExpressionCompare::CmpOp::EQ,
//         ExpressionFieldPath::createPathFromString(
//             getExpCtxRaw(), "nested.other", getExpCtxRaw()->variablesParseState),
//         ExpressionConstant::create(getExpCtxRaw(), Value(3)));
//     auto secondEncryptedRange = buildEncryptedRange(
//         std::string("nested.age"), 5, true, 5, true, getAgeConfig(), kSalaryUUID());
//     auto secondUnencrypted = ExpressionCompare::create(
//         getExpCtxRaw(),
//         ExpressionCompare::CmpOp::EQ,
//         ExpressionFieldPath::createPathFromString(
//             getExpCtxRaw(), "nested.other", getExpCtxRaw()->variablesParseState),
//         ExpressionConstant::create(getExpCtxRaw(), Value(7)));
//     auto thirdEncryptedRange = buildEncryptedRange(
//         std::string("nested.age"), 10, true, 10, true, getAgeConfig(), kSalaryUUID());
//     std::vector<boost::intrusive_ptr<Expression>> firstArgVec = {firstUnencrypted,
//                                                                  firstEncryptedRange};
//     auto firstAndExpr = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(firstArgVec));
//     std::vector<boost::intrusive_ptr<Expression>> secondArgVec = {secondUnencrypted,
//                                                                   secondEncryptedRange};
//     auto secondAndExpr = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(secondArgVec));
//     std::vector<boost::intrusive_ptr<Expression>> thirdArgVec = {thirdEncryptedRange};
//     auto thirdAndExpr = make_intrusive<ExpressionAnd>(getExpCtxRaw(), std::move(thirdArgVec));
//     std::vector<boost::intrusive_ptr<Expression>> argVec = {
//         firstAndExpr, secondAndExpr, thirdAndExpr};
//     auto inExprCorrect = make_intrusive<ExpressionOr>(getExpCtxRaw(), std::move(argVec));
//     ASSERT(unorderedDisjunctionComparison(inExprCorrect.get(),
//                                           dynamic_cast<ExpressionOr*>(inExprTest.get())));
// }

TEST_F(RangedAggregateExpressionIntender, InFailsToRewriteEncryptedComparedToInvalidTypes) {
    auto inExprBSON =
        BSON("$in" << BSON_ARRAY("$age" << BSON_ARRAY(BSON_ARRAY(1) << BSON("$const" << 1))));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(inExprBSON, false, Intention::NotMarked),
        AssertionException,
        6334105);
    inExprBSON = BSON("$in" << BSON_ARRAY("$age" << BSON_ARRAY("$foo" << BSON("$const" << 1))));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(inExprBSON, false, Intention::NotMarked),
        AssertionException,
        6334105);
    inExprBSON =
        BSON("$in" << BSON_ARRAY("$nested.age" << BSON_ARRAY("$foo" << BSON("$const" << 1))));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(inExprBSON, false, Intention::NotMarked),
        AssertionException,
        6334105);
    inExprBSON = BSON(
        "$in" << BSON_ARRAY("$nested.age" << BSON_ARRAY(BSON_ARRAY(1) << BSON("$const" << 1))));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(inExprBSON, false, Intention::NotMarked),
        AssertionException,
        6334105);
    inExprBSON =
        BSON("$in" << BSON_ARRAY(
                 "$age" << BSON_ARRAY(BSON_ARRAY(1) << BSON("$gt" << BSON_ARRAY("$age"
                                                                                << "150000")))));
    ASSERT_THROWS_CODE(
        markAggExpressionForRangeAndSerialize(inExprBSON, false, Intention::NotMarked),
        AssertionException,
        6334105);
}

}  // namespace
}  // namespace mongo
