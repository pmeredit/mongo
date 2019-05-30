/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include "aggregate_expression_intender.h"
#include "encryption_schema_tree.h"
#include "fle_test_fixture.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/unittest/unittest.h"

namespace mongo::aggregate_expression_intender {

namespace {

class ExpressionAnalysisTest : public FLETestFixture {
public:
    std::unique_ptr<EncryptionSchemaTreeNode> getSchemaFromExpression(BSONObj exprObj,
                                                                      BSONObj schema) {
        auto expr = BSON("expr" << exprObj);
        auto expCtx = new ExpressionContextForTest();
        auto parsedExpr =
            Expression::parseOperand(expCtx, expr["expr"], expCtx->variablesParseState);
        auto parsedSchema = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
        return aggregate_expression_intender::getOutputSchema(*parsedSchema.get(),
                                                              parsedExpr.get());
    }
};

TEST_F(ExpressionAnalysisTest, ExpressionFieldPathCorrectlyReturnsEncryptedState) {
    auto exprObj = fromjson("{notEncrypted: '$b', encrypted: '$ssn'}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["notEncrypted"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get());
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->containsEncryptedNode());

    parsedExpr =
        Expression::parseOperand(expCtx, exprObj["encrypted"], expCtx->variablesParseState);
    outSchema = aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get());
    ASSERT(outSchema->getEncryptionMetadata() == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, ExpressionFieldPathPrefixOfEncryptedReturnsCorrectSchema) {
    auto exprObj = fromjson("{encryptedPrefix: '$user'}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["encryptedPrefix"], expCtx->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kDefaultNestedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get());
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_TRUE(outSchema->containsEncryptedNode());

    // The original schema has the path 'user.ssn' as encrypted, so the output schema from the
    // rename should have 'ssn' encrypted.
    ASSERT(outSchema->getEncryptionMetadataForPath(FieldRef("ssn")) == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, ExpressionFieldPathWhosePrefixIsEncryptedShouldFail) {
    auto exprObj = fromjson("{invalid: '$ssn.non_existent'}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["invalid"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        51102);
}

TEST_F(ExpressionAnalysisTest, ExpressionConstantCorrectlyReturnsNotEncrypted) {
    auto exprObj = fromjson("{constant: 5}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["constant"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get());
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->containsEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, VariablesNotSupported) {
    auto exprObj = fromjson("{a: '$$NOW'}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr = Expression::parseOperand(expCtx, exprObj["a"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31127);

    exprObj = fromjson("{a: {$filter: {input: '$ssn', as: 'num', cond: {$eq: ['$$num', 0]}}}}");
    parsedExpr = Expression::parseOperand(expCtx, exprObj["a"], expCtx->variablesParseState);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31127);

    exprObj = fromjson(
        "{a: {$reduce: {input: ['a', 'b', 'c'], initialValue: '', in: {$concat: ['$$value', "
        "'$ssn']}}}}");
    parsedExpr = Expression::parseOperand(expCtx, exprObj["a"], expCtx->variablesParseState);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31127);
}

TEST_F(ExpressionAnalysisTest, ExpressionCondNotSupported) {
    auto exprObj = fromjson("{expr: {$cond: {if: true, then: '$ssn', else: null}}}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["expr"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31128);
}

TEST_F(ExpressionAnalysisTest, ExpressionSwitchNotSupported) {
    auto exprObj =
        fromjson("{expr: {$switch: {branches: [{case: 1, then: '$ssn'}, {case: 2, then: null}]}}}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["expr"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31128);
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectNotSupported) {
    auto exprObj = fromjson("{expr: {newSsn: '$ssn'}}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["expr"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31128);
}

TEST_F(ExpressionAnalysisTest, ExpressionLetNotSupported) {
    auto exprObj = fromjson("{expr: {$let: {vars: {ssn: 1}, in: {$eq: ['$ssn', 1]}}}}");
    auto expCtx = new ExpressionContextForTest();
    auto parsedExpr =
        Expression::parseOperand(expCtx, exprObj["expr"], expCtx->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get()),
        AssertionException,
        31128);
}

TEST_F(ExpressionAnalysisTest, EvaluatedExpressionsCorrectlyReturnNotEncrypted) {
    auto evaluateExpressions = {
        fromjson("{$abs: -1}"),
        fromjson("{$add: ['$ssn', 5]}"),
        fromjson("{$allElementsTrue: ['$ssn']}"),
        fromjson("{$and: [true, {$eq: ['$ssn', 1]}]}"),
        fromjson("{$anyElementTrue: ['$ssn']}"),
        fromjson("{$arrayElemAt: ['$ssn', 0]}"),
        fromjson("{$arrayToObject: '$ssn'}"),
        fromjson("{$avg: ['$ssn', 42]}"),
        fromjson("{$ceil: '$ssn'}"),
        fromjson("{$cmp: [0, 1]}"),
        fromjson("{$concat: ['str', '$ssn']}"),
        fromjson("{$concatArrays: [[], '$ssn']}"),
        fromjson("{$convert: {input: '$ssn', to: 'double'}}"),
        fromjson("{$dateFromParts: {year: '$ssn'}}"),
        fromjson("{$dateToParts: {date: '$ssn'}}"),
        fromjson("{$dateFromString: {dateString: '$ssn'}}"),
        fromjson("{$dateToString: {date: '$ssn'}}"),
        fromjson("{$dayOfMonth: '$ssn'}"),
        fromjson("{$dayOfWeek: '$ssn'}"),
        fromjson("{$divide: ['$ssn', 42.0]}"),
        fromjson("{$eq: ['$ssn', '8675309']}"),
        fromjson("{$exp: '$ssn'}"),
        fromjson("{$floor: '$ssn'}"),
        fromjson("{$gt: [5, '$ssn']}"),
        fromjson("{$gte: ['$a', '$ssn']}"),
        fromjson("{$hour: '$ssn'}"),
        fromjson("{$ifNull: ['$user', '$ssn']}"),
        fromjson("{$in: ['$ssn', [1, 2, 3]]}"),
        fromjson("{$indexOfArray: ['$ssn', 42.0]}"),
        fromjson("{$indexOfBytes: ['$ssn', '1234']}"),
        fromjson("{$indexOfCP: ['$ssn', '1234']}"),
        fromjson("{$isArray: '$ssn'}"),
        fromjson("{$isoDayOfWeek: '$ssn'}"),
        fromjson("{$isoWeek: '$ssn'}"),
        fromjson("{$isoWeekYear: '$ssn'}"),
        fromjson("{$literal: '$ssn'}"),
        fromjson("{$ln: '$ssn'}"),
        fromjson("{$log: ['$ssn', 10]}"),
        fromjson("{$log10: '$ssn'}"),
        fromjson("{$lt: ['$ssn', 42]}"),
        fromjson("{$lte: ['$ssn', 42]}"),
        fromjson("{$ltrim: {input: '$ssn', chars: '-'}}"),
        fromjson("{$map: {input: '$ssn', as: 'elem', in: {$eq: ['$elem', 42]}}}"),
        fromjson("{$max: ['$ssn', 42]}"),
        fromjson("{$mergeObjects: ['$ssn', null]}"),
        fromjson("{$meta: 'textScore'}"),
        fromjson("{$min: ['$ssn', 42]}"),
        fromjson("{$millisecond: '$ssn'}"),
        fromjson("{$minute: '$ssn'}"),
        fromjson("{$mod: ['$ssn', 42]}"),
        fromjson("{$month: '$ssn'}"),
        fromjson("{$multiply: ['$ssn', 42]}"),
        fromjson("{$ne: ['$ssn', 42]}"),
        fromjson("{$not: ['$ssn']}"),
        fromjson("{$objectToArray: '$ssn'}"),
        fromjson("{$or: [false, '$ssn']}"),
        fromjson("{$pow: ['$ssn', 0]}"),
        fromjson("{$range: ['$ssn', 42, 1]}"),
        fromjson("{$reverseArray: '$ssn'}"),
        fromjson("{$rtrim: {input: '$ssn', chars: '-'}}"),
        fromjson("{$second: '$ssn'}"),
        fromjson("{$setDifference: [[1, 2, 3], '$ssn']}"),
        fromjson("{$setEquals: [[1, 2, 3], '$ssn']}"),
        fromjson("{$setIntersection: [[1, 2, 3], '$ssn']}"),
        fromjson("{$setIsSubset: [[1, 2, 3], '$ssn']}"),
        fromjson("{$setUnion: [[1, 2, 3], '$ssn']}"),
        fromjson("{$size: '$ssn'}"),
        fromjson("{$slice: [[1, 2, 3], 1, 1]}"),
        fromjson("{$split: ['$ssn', '-']}"),
        fromjson("{$sqrt: '$ssn'}"),
        fromjson("{$strcasecmp: ['$ssn', '42']}"),
        fromjson("{$strLenBytes: '$ssn'}"),
        fromjson("{$strLenCP: '$ssn'}"),
        fromjson("{$substrBytes: ['abcde', 1, 2]}"),
        fromjson("{$substrCP: ['abcde', 1, 2]}"),
        fromjson("{$subtract: [42, '$ssn']}"),
        fromjson("{$sum: ['$ssn', 42]}"),
        fromjson("{$toBool: '$ssn'}"),
        fromjson("{$toDate: '$ssn'}"),
        fromjson("{$toDecimal: '$ssn'}"),
        fromjson("{$toDouble: '$ssn'}"),
        fromjson("{$toInt: '$ssn'}"),
        fromjson("{$toLong: '$ssn'}"),
        fromjson("{$toObjectId: '$ssn'}"),
        fromjson("{$toString: '$ssn'}"),
        fromjson("{$toLower: '$ssn'}"),
        fromjson("{$toUpper: '$ssn'}"),
        fromjson("{$trim: {input: '$ssn', chars: '-'}}"),
        fromjson("{$trunc: '$ssn'}"),
        fromjson("{$type: '$ssn'}"),
        fromjson("{$week: '$ssn'}"),
        fromjson("{$year: '$ssn'}"),
        fromjson("{$zip: {inputs: [['a'], ['b'], ['c']]}}"),
    };
    for (auto&& expr : evaluateExpressions) {
        auto outSchema = getSchemaFromExpression(expr, kDefaultSsnSchema);
        ASSERT_FALSE(outSchema->getEncryptionMetadata());
        ASSERT_FALSE(outSchema->containsEncryptedNode());
    }
}

}  // namespace
}  // namespace mongo::aggregate_expression_intender
