/**
 *    Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "aggregate_expression_intender.h"
#include "encryption_schema_tree.h"
#include "fle_test_fixture.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/unittest/unittest.h"

namespace mongo::aggregate_expression_intender {

namespace {

class ExpressionAnalysisTest : public FLETestFixture {
public:
    std::unique_ptr<EncryptionSchemaTreeNode> getSchemaFromExpression(BSONObj exprObj,
                                                                      BSONObj schema) {
        auto expr = BSON("expr" << exprObj);
        auto parsedExpr = Expression::parseOperand(
            getExpCtxRaw(), expr["expr"], getExpCtx()->variablesParseState);
        auto parsedSchema = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);
        return aggregate_expression_intender::getOutputSchema(
            *parsedSchema.get(), parsedExpr.get(), false);
    }
};

TEST_F(ExpressionAnalysisTest, ExpressionFieldPathCorrectlyReturnsEncryptedState) {
    auto exprObj = fromjson("{notEncrypted: '$b', encrypted: '$ssn'}");
    auto parsedExpr = Expression::parseOperand(
        getExpCtxRaw(), exprObj["notEncrypted"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());

    parsedExpr = Expression::parseOperand(
        getExpCtxRaw(), exprObj["encrypted"], getExpCtx()->variablesParseState);
    outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT(outSchema->getEncryptionMetadata() == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, ExpressionFieldPathPrefixOfEncryptedCorrectlyFails) {
    auto exprObj = fromjson("{encryptedPrefix: '$user'}");
    auto parsedExpr = Expression::parseOperand(
        getExpCtxRaw(), exprObj["encryptedPrefix"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kDefaultNestedSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false),
        AssertionException,
        31129);
}

TEST_F(ExpressionAnalysisTest, ExpressionFieldPathWhosePrefixIsEncryptedShouldFail) {
    auto exprObj = fromjson("{invalid: '$ssn.non_existent'}");
    auto parsedExpr = Expression::parseOperand(
        getExpCtxRaw(), exprObj["invalid"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false),
        AssertionException,
        51102);
}

TEST_F(ExpressionAnalysisTest, ExpressionConstantCorrectlyReturnsNotEncrypted) {
    auto exprObj = fromjson("{constant: 5}");
    auto parsedExpr = Expression::parseOperand(
        getExpCtxRaw(), exprObj["constant"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, VariablesSupportedInOutput) {
    auto exprObj = fromjson("{a: '$$NOW'}");
    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["a"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());

    exprObj =
        fromjson("{a: {$filter: {input: ['a', 'b', 'c'], as: 'num', cond: {$eq: ['$$num', 0]}}}}");
    parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["a"], getExpCtx()->variablesParseState);
    outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());

    exprObj = fromjson(
        "{a: {$reduce: {input: ['a', 'b', 'c'], initialValue: '', in: {$concat: ['$$value', "
        "'$$this']}}}}");
    parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["a"], getExpCtx()->variablesParseState);
    outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());

    exprObj = fromjson("{expr: {$let: {vars: {not_ssn: 1}, in: {$eq: ['$not_ssn', 1]}}}}");
    parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["a"], getExpCtx()->variablesParseState);
    outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, CondReturnsCorrectSchemaIfBothBranchesNotEncrypted) {
    auto exprObj = fromjson("{expr: {$cond: {if: true, then: '$notEncrypted', else: 42}}}");
    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, CondReturnsCorrectSchemaIfBothBranchesEncrypted) {
    auto ssnAndUserSchema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [{'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "string"
                    }
                },
                user: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [{'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "string"
                    }
                }
            }
        })");
    auto exprObj = fromjson("{expr: {$cond: {if: true, then: '$ssn', else: '$user'}}}");
    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(ssnAndUserSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT(outSchema->getEncryptionMetadata() == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, CondReturnsUnknownIfBranchesHaveConflictingEncryptionProperties) {
    auto exprObj = fromjson("{expr: {$cond: {if: true, then: '$ssn', else: 42}}}");
    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, CondWithEvaluatedSubtreeReturnsNotEncrypted) {
    auto exprObj =
        fromjson("{expr: {$cond: {if: true, then: '$notEncrypted', else: {$add: [42, 5]}}}}");
    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());

    exprObj = fromjson("{expr: {$cond: {if: true, then: '$ssn', else: {$add: ['$b', 5]}}}}");
    parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, CondFailsIfBranchReferencesPrefixOfEncryptedField) {
    auto userSchema = fromjson(R"({
            type: "object",
            properties: {
                user: {
                    type: "object",
                    properties: {
                        ssn: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                                bsonType: "string"
                            }
                        }
                    }
                },
                insecureUser: {
                    type: "object",
                    properties: {
                        ssn: {type: "string"}
                    }
                }
            }
        })");
    auto exprObj = fromjson("{expr: {$cond: {if: true, then: '$user', else: '$insecureUser'}}}");
    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(userSchema, EncryptionSchemaType::kLocal);
    ASSERT_THROWS_CODE(
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false),
        AssertionException,
        31129);
}

TEST_F(ExpressionAnalysisTest, CondReturnsUnknownIfBranchesHaveMismatchedObjectSchemas) {
    auto exprObj =
        fromjson("{$cond: {if: true, then: {newSsn: '$ssn'}, else: {newSsn: '$notEncrypted'}}}");
    auto outSchema = getSchemaFromExpression(exprObj, kDefaultSsnSchema);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, NestedCondCorrectlyReturnsOutputSchema) {
    auto exprObj = fromjson(R"({
        expr: {
            $cond: {
                if: true, 
                then: {
                    $cond: {
                        if: false, 
                        then: '$a',
                        else: '$b'
                    }
                },
                else: '$c'
            }
        }
    })");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT(outSchema->getEncryptionMetadata() == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, NestedCondReturnsUnknownOnMixedEncryption) {
    auto exprObj = fromjson(R"({
        expr: {
            $cond: {
                if: true, 
                then: {
                    $cond: {
                        if: false, 
                        then: '$a',
                        else: '$b'
                    }
                },
                else: 42
            }
        }
    })");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, CondInEvaluationContextDoesNotFailIfMixedEncryption) {
    auto exprObj = fromjson(R"({
        expr: {
            $eq: [
                {$cond: {
                    if: true, 
                    then: {
                        $cond: {
                            if: false, 
                            then: '$a',
                            else: '$notSafe'
                        }
                    },
                    else: 42
                }},
                42
            ]
        }
    })");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, SwitchReturnsNotEncryptedIfAllBranchesReturnNotEncrypted) {
    auto exprObj = fromjson(R"({expr: {
        $switch: {
            branches: [
                {case: {$eq: ['$ssn', 1]}, then: '$notEncrypted'}, 
                {case: {$eq: ['$ssn', 2]}, then: '$alsoNotEncrypted'}
            ],
            default: 42
        }
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, SwitchReturnsEncryptedIfAllBranchesReturnEncrypted) {
    auto exprObj = fromjson(R"({expr: {
        $switch: {
            branches: [
                {case: {$eq: ['$a', 1]}, then: '$b'}, 
                {case: {$eq: ['$a', 2]}, then: '$c'}
            ],
            default: '$d'
        }
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT(outSchema->getEncryptionMetadata() == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, SwitchReturnsUnknownIfConflictingEncryptionInBranches) {
    auto exprObj = fromjson(R"({expr: {
        $switch: {
            branches: [
                {case: {$eq: ['$a', 1]}, then: '$notSafe'}, 
                {case: {$eq: ['$a', 2]}, then: '$c'},
                {case: {$eq: ['$a', 3]}, then: '42'}
            ],
            default: '$d'
        }
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, SwitchThrowsIfConflictingObjectSchemasInBranches) {
    auto exprObj = fromjson(R"({
        $switch: {
            branches: [
                {case: {$eq: ['$a', 1]}, then: {newSsn: '$ssn'}}, 
                {case: {$eq: ['$a', 2]}, then: {newSsn: '1234'}}
            ],
            default: {newSsn: '1234'}
        }
    })");
    auto outSchema = getSchemaFromExpression(exprObj, kDefaultSsnSchema);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, SwitchReturnsUnknownIfDefaultConflictsWithBranches) {
    auto exprObj = fromjson(R"({expr: {
        $switch: {
            branches: [
                {case: {$eq: ['$a', 1]}, then: '$b'}, 
                {case: {$eq: ['$a', 2]}, then: '$c'}
            ],
            default: {$add: [40, 2]}
        }
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, IfNullReturnsEncryptedIfBothBranchesAreEncrypted) {
    auto exprObj = fromjson(R"({expr: {
        $ifNull: [
            '$ssn',
            '$alsoEncrypted'
        ]
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT(outSchema->getEncryptionMetadata() == kDefaultMetadata);
}

TEST_F(ExpressionAnalysisTest, IfNullReturnsNotEncryptedIfBothBranchedAreNotEncrypted) {
    auto exprObj = fromjson(R"({expr: {
        $ifNull: [
            '$a',
            '$notSsn'
        ]
    }})");

    auto parsedExpr = Expression::parseOperand(
        getExpCtx().get(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, IfNullReturnsUnknownIfConflictingEncryptionProperties) {
    auto exprObj = fromjson(R"({expr: {
        $ifNull: [
            '$ssn',
            '$notSsn'
        ]
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, IfNullReturnsUnknownIfConflictingObjectSchemas) {
    auto exprObj = fromjson(R"({expr: {
        $ifNull: [
            {nestedSsn: '1234'},
            {nestedSsn: '$ssn'}
        ]
    }})");

    auto parsedExpr =
        Expression::parseOperand(getExpCtxRaw(), exprObj["expr"], getExpCtx()->variablesParseState);
    auto schema = EncryptionSchemaTreeNode::parse(kDefaultSsnSchema, EncryptionSchemaType::kLocal);
    auto outSchema =
        aggregate_expression_intender::getOutputSchema(*schema.get(), parsedExpr.get(), false);
    ASSERT_THROWS_CODE(outSchema->getEncryptionMetadata(), AssertionException, 31133);
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectWithSingleTopLevelEncryptedField) {
    auto exprObj = fromjson("{newSsn: '$ssn'}");
    auto outSchema = getSchemaFromExpression(exprObj, kDefaultSsnSchema);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_TRUE(outSchema->mayContainEncryptedNode());
    ASSERT(outSchema->getEncryptionMetadataForPath(FieldRef("newSsn")) == kDefaultMetadata);
    ASSERT_FALSE(outSchema->getEncryptionMetadataForPath(FieldRef("nonExistent")));
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectWithMultipleTopLevelEncryptedFields) {
    auto exprObj = fromjson("{newSsn: '$ssn', newUser: '$user'}");
    auto outSchema = getSchemaFromExpression(exprObj, kAllEncryptedSchema);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_TRUE(outSchema->mayContainEncryptedNode());
    ASSERT(outSchema->getEncryptionMetadataForPath(FieldRef("newSsn")) == kDefaultMetadata);
    ASSERT(outSchema->getEncryptionMetadataForPath(FieldRef("newUser")) == kDefaultMetadata);
    ASSERT_FALSE(outSchema->getEncryptionMetadataForPath(FieldRef("nonExistent")));
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectWithNoEncryptedFields) {
    auto exprObj = fromjson("{answer: 42, to: 'life'}");
    auto outSchema = getSchemaFromExpression(exprObj, kDefaultSsnSchema);
    ASSERT_FALSE(outSchema->getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectWithEvaluatedExpressionsReturnsNoEncryption) {
    auto exprObj = fromjson("{answer: {$add: [40, 2]}, to: {$concat: ['li', 'fe']}}");
    auto outSchema = getSchemaFromExpression(exprObj, kDefaultSsnSchema);
    ASSERT_FALSE(outSchema->getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(outSchema->mayContainEncryptedNode());
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectWithReferencesToEncryptedPrefixFails) {
    auto exprObj = fromjson("{newUser: '$user'}");
    ASSERT_THROWS_CODE(
        getSchemaFromExpression(exprObj, kDefaultNestedSchema), AssertionException, 31129);
}

TEST_F(ExpressionAnalysisTest, ExpressionObjectInceptionCorrectlyReturnsOutSchema) {
    auto exprObj = fromjson("{newUser: {name: '$user', ssn: '$ssn'}}");
    auto outSchema = getSchemaFromExpression(exprObj, kAllEncryptedSchema);
    ASSERT_FALSE(outSchema->getEncryptionMetadata());
    ASSERT_TRUE(outSchema->mayContainEncryptedNode());
    ASSERT(outSchema->getEncryptionMetadataForPath(FieldRef("newUser.name")) == kDefaultMetadata);
    ASSERT(outSchema->getEncryptionMetadataForPath(FieldRef("newUser.ssn")) == kDefaultMetadata);
    ASSERT_FALSE(outSchema->getEncryptionMetadataForPath(FieldRef("nonExistent")));
}

TEST_F(ExpressionAnalysisTest, EvaluatedExpressionsCorrectlyReturnNotEncrypted) {
    setTestCommandsEnabled(true);
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
        fromjson("{$dateAdd: {startDate : '$ssn', unit : 'day', amount : 1}}"),
        fromjson(
            "{$dateDiff: {startDate: '$ssn', endDate: '$ssn', unit: '$ssn', timezone: '$ssn'}}"),
        fromjson("{$dateFromParts: {year: '$ssn'}}"),
        fromjson("{$dateToParts: {date: '$ssn'}}"),
        fromjson("{$dateFromString: {dateString: '$ssn'}}"),
        fromjson("{$dateSubtract: {startDate : '$ssn', unit : 'day', amount : 1}}"),
        fromjson("{$dateToString: {date: '$ssn'}}"),
        fromjson("{$dateTrunc: {date: '$ssn', unit: '$ssn', binSize: '$ssn', timezone: '$ssn', "
                 "startOfWeek: '$ssn'}}"),
        fromjson("{$dayOfMonth: '$ssn'}"),
        fromjson("{$dayOfWeek: '$ssn'}"),
        fromjson("{$divide: ['$ssn', 42.0]}"),
        fromjson("{$eq: ['$ssn', '8675309']}"),
        fromjson("{$exp: '$ssn'}"),
        fromjson("{$floor: '$ssn'}"),
        fromjson("{$function: {body: 'function(){}', args: ['$ssn'], lang: 'js'}}"),
        fromjson("{$gt: [5, '$ssn']}"),
        fromjson("{$gte: ['$a', '$ssn']}"),
        fromjson("{$hour: '$ssn'}"),
        fromjson("{$in: ['$ssn', [1, 2, 3]]}"),
        fromjson("{$indexOfArray: ['$ssn', 42.0]}"),
        fromjson("{$indexOfBytes: ['$ssn', '1234']}"),
        fromjson("{$indexOfCP: ['$ssn', '1234']}"),
        BSON("$_internalJsEmit" << BSON("this"
                                        << "{}"
                                        << "eval" << BSONCode("function(){}"))),
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
        ASSERT_FALSE(outSchema->mayContainEncryptedNode());
    }
}

}  // namespace
}  // namespace mongo::aggregate_expression_intender
