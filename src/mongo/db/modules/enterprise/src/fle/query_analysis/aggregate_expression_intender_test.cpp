/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string>

#include "aggregate_expression_intender_entry.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using namespace std::string_literals;

class AggregateExpressionIntenderTest : public AggregationContextFixture {
protected:
    auto addObjectWrapper(StringData expression) {
        return "{\"\" : "s + expression + "}";
    }

    auto removeObjectWrapper(std::string expression) {
        return expression.substr(7, expression.size() - 9);
    }

    static constexpr auto schemaString =
        R"({
            type: "object",
            properties: {
                safeString: {
                    encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                            bsonType: "string"
                    }
                },
                otherSafeString: {
                    encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                            bsonType: "string"
                    }
                },
                yetAnotherSafeString: {
                    encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                            bsonType: "string"
                    }
                },
                differentSafeString: {
                    encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [{$binary: "fkJwjwbZSiS/AtxiedXLNQ==", $type: "04"}],
                            bsonType: "string"
                    }
                },
                safeInt: {
                    encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                            bsonType: "int"
                    }
                },
                randomString: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "string"
                    }
                },
                nestedRandom: {
                    type: "object",
                    properties: {
                        randomString: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                                bsonType: "string"
                            }
                        }
                    }
                }
            }
        })";

    /**
     * Parses 'schema' into an encryption schema tree.
     */
    auto makeSchema() {
        return EncryptionSchemaTreeNode::parse(fromjson(schemaString),
                                               EncryptionSchemaType::kLocal);
    }

    auto stateIntentionBson(StringData expression) {
        BSONObjBuilder bob;
        auto expressionPtr =
            Expression::parseOperand(getExpCtxRaw(),
                                     mongo::fromjson(addObjectWrapper(expression))[""],
                                     getExpCtx()->variablesParseState);
        auto intention = aggregate_expression_intender::mark(
            getExpCtxRaw(), *makeSchema(), expressionPtr, false);
        ASSERT(intention == aggregate_expression_intender::Intention::Marked ||
               intention == aggregate_expression_intender::Intention::NotMarked);
        expressionPtr->serialize().addToBsonObj(&bob, "");
        return bob.obj();
    }
    auto stateIntention(StringData expression) {
        return removeObjectWrapper(
            tojson(stateIntentionBson(expression), JsonStringFormat::LegacyStrict));
    }
};

TEST_F(AggregateExpressionIntenderTest, SingleLeafExpressions) {
    // Constant.
    ASSERT_IDENTITY("{ \"$const\" : \"hello\" }", stateIntention);
    // Field Reference unencrypted.
    ASSERT_IDENTITY("\"$unsafeString\"", stateIntention);
    // Field Reference encrypted.
    ASSERT_IDENTITY("\"$safeString\"", stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, RootLevelEvaluations) {
    // Directly Evaluated unencrypted.
    ASSERT_IDENTITY("{ \"$add\" : [ { \"$const\" : 1 }, { \"$const\" : 2 } ] }", stateIntention);
    // Directly Evaluated encrypted (not allowed).
    ASSERT_THROWS_CODE(stateIntention("{ \"$atan2\" : [ { \"$const\" : 1 }, \"$safeInt\" ] }"),
                       AssertionException,
                       31110);
}

TEST_F(AggregateExpressionIntenderTest, RootComparisonsWithLeafChildren) {
    // Equality comparison unencrypted.
    ASSERT_IDENTITY("{ \"$eq\" : [ { \"$const\" : \"hello\" }, \"$unsafeString\" ] }",
                    stateIntention);
    // Equality comparison encrypted.
    ASSERT_IDENTITY("{ \"$ne\" : [ \"$safeString\", \"$otherSafeString\" ] }", stateIntention);
    // Equality comparison encryption mismatch.
    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ \"$safeString\", \"$differentSafeString\" ] }"),
        AssertionException,
        31100);
    // Equality comparison needing mark.
    ASSERT_EQ(stateIntention("{ \"$eq\" : [ { \"$const\" : \"hello\" }, \"$safeString\" ] }"),
              "{ \"$eq\" : [ { \"$const\" : { \"$binary\" : "
              "\"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", \"$type\" "
              ": \"06\" } }, \"$safeString\" ] }");
}

TEST_F(AggregateExpressionIntenderTest, ForwardedCond) {
    // Forwarded $cond permits anything.
    ASSERT_IDENTITY(
        "{ \"$cond\" : [ { \"$const\" : true }, \"$safeString\", { \"$const\" : \"foo\" } ] }",
        stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, ComparedCond) {
    // Compared encrypted $cond permits encrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$cond\" : [ { \"$const\" : true }, \"$safeString\", "
        "\"$otherSafeString\" ] }, \"$yetAnotherSafeString\" ] }",
        stateIntention);
    // Compared encrypted $cond marks literals.
    ASSERT_EQ(
        stateIntention(
            "{ \"$eq\" : [ { \"$cond\" : [ { \"$const\" : true }, "
            "{ \"$const\" : \"hello\" }, { \"$const\" : \"goodbye\" } ] }, \"$safeString\" ] }"),
        "{ \"$eq\" : [ { \"$cond\" : [ { \"$const\" : true }, { \"$const\" : { \"$binary\" : "
        "\"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", \"$type\" "
        ": \"06\" } }, { \"$const\" : { \"$binary\" : "
        "\"ADQAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAgAAABnb29kYnllAAA=\", "
        "\"$type\" : \"06\" } } ] }, \"$safeString\" ] }");
    // Compared encrypted $cond forbids unlike encrypted.
    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ { \"$cond\" : [ { \"$const\" : true }, \"$safeString\", "
                       "\"$differentSafeString\" ] }, \"$otherSafeString\" ] }"),
        AssertionException,
        31100);
    // Compared encrypted $cond forbids unencrypted.
    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ { \"$cond\" : [ { \"$const\" : true }, \"$safeString\", "
                       "\"$unsafeString\" ] }, \"$otherSafeString\" ] }"),
        AssertionException,
        31099);
    // Compared unencrypted $cond permits unencrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$cond\" : [ { \"$const\" : true }, "
        "{ \"$const\" : \"hello\" }, { \"$const\" : \"goodbye\" } ] }, \"$unsafeString\" ] }",
        stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, EvaluatedCond) {
    // Evaluated $cond permits unencrypted.
    ASSERT_IDENTITY(
        "{ \"$concat\" : [ { \"$cond\" : [ { \"$const\" : true }, "
        "{ \"$const\" : \"hello \" }, \"$unsafeString\" ] }, { \"$const\" : \"world\" } ] }",
        stateIntention);
    // Evaluated $cond forbids encrypted.
    ASSERT_THROWS_CODE(
        stateIntention(
            "{ \"$concat\" : [ { \"$cond\" : [ { \"$const\" : true }, "
            "{ \"$const\" : \"hello \" }, \"$safeString\" ] }, { \"$const\" : \"world\" } ] }"),
        AssertionException,
        31110);
}

TEST_F(AggregateExpressionIntenderTest, ForwardedSwitch) {
    // Forwarded $switch permits anything.
    ASSERT_IDENTITY(
        "{ \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" : \"true\" }, "
        "\"then\" : \"$safeString\" }, { \"case\" : { \"$const\" : \"false\" }, "
        "\"then\" : { \"$const\" : \"foo\" } } ], \"default\" : \"$unsafeString\" } }",
        stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, ComparedSwitch) {
    // Compared encrypted $switch permits encrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" : \"true\" }, "
        "\"then\" : \"$safeString\" }, { \"case\" : { \"$const\" : \"false\" }, "
        "\"then\" : \"$otherSafeString\" } ] } }, \"$yetAnotherSafeString\" ] }",
        stateIntention);
    // Compared encrypted $switch marks literals.
    ASSERT_EQ(
        stateIntention(
            "{ \"$eq\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : "
            "{ \"$const\" : \"true\" }, \"then\" : { \"$const\" : \"hello\"} }, "
            "{ \"case\" : { \"$const\" : \"false\" }, \"then\" : { \"$const\" : \"goodbye\" } } ]"
            " } }, \"$safeString\" ] }"),
        "{ \"$eq\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" : \"true\" }, "
        "\"then\" : { \"$const\" : { \"$binary\" : "
        "\"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", "
        "\"$type\" : \"06\" } } }, { \"case\" : { \"$const\" : \"false\" }, \"then\" : "
        "{ \"$const\" : { \"$binary\" : "
        "\"ADQAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAgAAABnb29kYnllAAA=\", \"$type\" : "
        "\"06\" } } } ] } }, \"$safeString\" ] }");
    // Compared encrypted $switch forbids unlike encrypted.
    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" "
                       ": \"true\" }, "
                       "\"then\" : \"$safeString\" }, { \"case\" : { \"$const\" : \"false\" }, "
                       "\"then\" : \"$otherSafeString\" } ] } }, \"$differentSafeString\" ] }"),
        AssertionException,
        31100);
    // Compared encrypted $switch forbids unencrypted.
    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" "
                       ": \"true\" }, "
                       "\"then\" : \"$safeString\" }, { \"case\" : { \"$const\" : \"false\" }, "
                       "\"then\" : \"$unsafeString\" } ] } }, \"$otherSafeString\" ] }"),
        AssertionException,
        31099);
    // Compared unencrypted $switch permits unencrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" : \"true\" }, "
        "\"then\" : { \"$const\" : \"foo\" } }, { \"case\" : { \"$const\" : \"false\" }, "
        "\"then\" : \"$unsafeString\" } ] } }, { \"$const\" : \"bar\" } ] }",
        stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, EvaluatedSwitch) {
    // Evaluated $switch permits unencrypted.
    ASSERT_IDENTITY(
        "{ \"$concat\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" : \"true\" "
        "}, \"then\" : { \"$const\" : \"hello\" } }, { \"case\" : { \"$const\" : \"false\" }, "
        "\"then\" : \"$unsafeString\" } ] } }, { \"$const\" : \"world\" } ] }",
        stateIntention);
    // Evaluated $switch forbids encrypted.
    ASSERT_THROWS_CODE(
        stateIntention(
            "{ \"$concat\" : [ { \"$switch\" : { \"branches\" : [ { \"case\" : { \"$const\" : "
            "\"true\" }, \"then\" : { \"$const\" : \"hello\" } }, { \"case\" : { \"$const\" : "
            "\"false\" }, \"then\" : \"$safeString\" } ] } }, { \"$const\" : \"world\" } ] }"),
        AssertionException,
        31110);
}

TEST_F(AggregateExpressionIntenderTest, VariablesPermitted) {
    // Variable allowed in Forwarded.
    ASSERT_IDENTITY("\"$$NOW\"", stateIntention);
    // Variable allowed in Compared.
    ASSERT_IDENTITY("{ \"$cond\" : [ { \"$const\" : true }, \"$$REMOVE\", { \"$const\" : 33 } ] }",
                    stateIntention);
    // Variable allowed in Evaluated.
    ASSERT_IDENTITY(
        "{ \"$let\" : { \"vars\" : { \"three\" : { \"$const\" : 3 } }, \"in\" : "
        "{ \"$subtract\" : [ \"$unsafeInt\", \"$$three\" ] } } }",
        stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, LetAndReducePreserveParentSubtree) {
    // Test Subtree preservation by marking through a $let 'in'
    ASSERT_EQ(stateIntention("{ \"$eq\" : [ { \"$let\" : { \"vars\" : {}, \"in\" : { \"$const\" : "
                             "\"hello\" } } }, \"$safeString\" ] }"),
              "{ \"$eq\" : [ { \"$let\" : { \"vars\" : {}, \"in\" : { \"$const\" : { \"$binary\" : "
              "\"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", \"$type\" "
              ": \"06\" } } } }, \"$safeString\" ] }");
    // Test Subtree preservation by marking through a $reduce 'in'
    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            "": {
                "$eq": [
                    {
                        "$reduce": {
                            "input": [],
                            "initialValue": {
                                "$const": {}
                            },
                            "in": {
                                "$const": {"$binary":{"base64":"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA","subType":"6"}}
                            }
                        }
                    },
                    "$safeString"
                ]
            }
        })",
        stateIntentionBson(
            R"({$eq: [{$reduce: {input: [], initialValue: {}, in: {$const: "hello"}}}, "$safeString"]})"));
}

TEST_F(AggregateExpressionIntenderTest, LetForbidsBindingToEncryptedValue) {
    ASSERT_THROWS_CODE(
        stateIntention(
            "{ \"$let\" : { \"vars\" : { \"dontTouchIt\" : \"$safeInt\" }, \"in\": \"opps\" } }"),
        AssertionException,
        31110);
}

TEST_F(AggregateExpressionIntenderTest, ComparisonsWithInExpression) {
    // Everything unencrypted.
    ASSERT_IDENTITY(
        "{ \"$in\" : [ { \"$const\" : \"hello\" }, [ { \"$const\" : \"hello\" }, \"$unsafeString\" "
        "] ] }",
        stateIntention);
    ASSERT_IDENTITY(
        "{ \"$in\" : [ { \"$const\" : \"hello\" }, [ { \"$const\" : \"hello\" }, "
        "\"$unsafeString\", "
        "{ \"$add\" : [ { \"$const\" : 1 }, { \"$const\" : 2 } ] } ] ] }",
        stateIntention);

    // Everything encrypted.
    ASSERT_IDENTITY(
        "{ \"$in\" : [ \"$safeString\", [ \"$otherSafeString\", \"$otherSafeString\" ] ] }",
        stateIntention);

    // Mix of encrypted and unencrypted
    ASSERT_THROWS_CODE(
        stateIntention(
            "{ \"$in\" : [ \"$safeString\", [ \"$unsafeString\", \"$otherSafeString\" ] ] }"),
        AssertionException,
        31099);

    // If the second argument will resolve to an array at runtime then we must fail if that
    // expression is encrypted. We won't be able to look into an encrypted array at runtime.
    ASSERT_THROWS_CODE(stateIntention("{ \"$in\" : [ \"$safeString\", \"$otherSafeString\" ] }"),
                       AssertionException,
                       31110);

    // Equality comparison needing mark on the left-hand side.
    ASSERT_EQ(stateIntention("{ \"$in\" : [ { \"$const\" : \"hello\" }, [ \"$safeString\" ] ] }"),
              "{ \"$in\" : [ { \"$const\" : { \"$binary\" : "
              "\"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", \"$type\" "
              ": \"06\" } }, [ \"$safeString\" ] ] }");

    // Equality comparison needing mark within the array literal.
    ASSERT_EQ(stateIntention("{ \"$in\" : [ \"$safeString\", [ \"$otherSafeString\", { \"$const\" "
                             ": \"hello\" } ] ] }"),
              "{ \"$in\" : [ \"$safeString\", [ \"$otherSafeString\", { \"$const\" : { \"$binary\" "
              ": \"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", "
              "\"$type\" : \"06\" } } ] ] }");
}

TEST_F(AggregateExpressionIntenderTest,
       EqualityComparisonsAreNotAllowedInComparedContextWithEncryption) {
    // Everything unencrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$const\" : true }, { \"$eq\" : [ { \"$const\" : \"hello\" }, { "
        "\"$const\" : \"hello\" } ] } ] }",
        stateIntention);
    ASSERT_IDENTITY(
        "{ \"$ne\" : [ { \"$const\" : true }, { \"$ne\" : [ { \"$const\" : \"hello\" }, { "
        "\"$const\" : \"hello\" } ] } ] }",
        stateIntention);

    // Comparing an unencrypted value to the result of comparing two encrypted values should be
    // allowed
    // since "the result of comparing two encrypted values" will always be unencrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$const\" : false }, { \"$eq\" : [ \"$safeString\", \"$otherSafeString\" "
        "] } ] }",
        stateIntention);
    ASSERT_IDENTITY(
        "{ \"$ne\" : [ { \"$const\" : false }, { \"$ne\" : [ \"$safeString\", \"$otherSafeString\" "
        "] } ] }",
        stateIntention);

    // Comparing an encrypted value to the result of an equality comparison should not be allowed.
    ASSERT_THROWS_CODE(stateIntention("{ \"$eq\" : [ \"$safeString\", { \"$eq\" : [ "
                                      "\"$safeString\", \"$otherSafeString\" ] } ] }"),
                       AssertionException,
                       31117);
    ASSERT_THROWS_CODE(stateIntention("{ \"$ne\" : [ \"$safeString\", { \"$ne\" : [ "
                                      "\"$safeString\", \"$otherSafeString\" ] } ] }"),
                       AssertionException,
                       31117);
}

TEST_F(AggregateExpressionIntenderTest, InIsNotAllowedInComparedContextWithEncyption) {
    // Everything unencrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$const\" : true }, { \"$in\" : [ { \"$const\" : \"hello\" }, [ { "
        "\"$const\" : \"hello\" }, \"$unsafeString\" "
        "] ] } ] }",
        stateIntention);

    // Comparing an unencrypted value to the result of comparing two encrypted values should be
    // allowed since "the result of comparing two encrypted values" will always be unencrypted.
    ASSERT_IDENTITY(
        "{ \"$eq\" : [ { \"$const\" : false }, { \"$in\" : [ \"$safeString\", [ "
        "\"$otherSafeString\", \"$otherSafeString\" ] ] } ] }",
        stateIntention);

    // Comparing an encrypted value to the result of an $in comparison should not be allowed.
    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ \"$safeString\", { \"$in\" : [ \"$safeString\", [ "
                       "\"$otherSafeString\", \"$yetAnotherSafeString\" ] ] } ] }"),
        AssertionException,
        31117);

    ASSERT_THROWS_CODE(stateIntention("{ \"$eq\" : [ \"$safeString\", { \"$in\" : [ "
                                      "\"$safeString\", \"$nonArrayLiteral\" ] } ] }"),
                       AssertionException,
                       31117);
}

TEST_F(AggregateExpressionIntenderTest, ArrayLiteralShouldFailIfMisplacedWithinIn) {
    ASSERT_THROWS_CODE(stateIntention("{ \"$in\" : [ [ \"$safeString\" ] , [ \"hello\" ] ] }"),
                       AssertionException,
                       31110);
    // Note the double brackets around the $in array argument.
    ASSERT_THROWS_CODE(stateIntention("{ \"$in\" : [ \"$safeString\", [ [ \"hello\" ] ] ] }"),
                       AssertionException,
                       31117);
}

TEST_F(AggregateExpressionIntenderTest, ArrayLiteralShouldFailWithinEqualityComparison) {
    ASSERT_THROWS_CODE(stateIntention("{ \"$eq\" : [ \"$safeString\", [ \"hello\" ] ] }"),
                       AssertionException,
                       31117);

    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ [ \"$safeString\",  \"$otherSafeString\" ], \"hello\" ] }"),
        AssertionException,
        31110);

    ASSERT_THROWS_CODE(
        stateIntention("{ \"$eq\" : [ \"hello\", { \"$cond\" : [ { \"$eq\": [ \"$unsafeString1\", "
                       "\"$unsafeString2\" ] }, [ \"$safeString\",  \"$otherSafeString\" ], "
                       "\"hello\" ] } ] }"),
        AssertionException,
        31110);
}

TEST_F(AggregateExpressionIntenderTest, MarkReportsExistenceOfEncryptedPlaceholder) {
    auto query = R"({
        "$eq": [
            "$safeString",
            "abc123"
        ]
    })";
    BSONObjBuilder bob;
    auto expressionPtr = Expression::parseOperand(getExpCtxRaw(),
                                                  mongo::fromjson(addObjectWrapper(query))[""],
                                                  getExpCtx()->variablesParseState);
    ASSERT_TRUE(
        aggregate_expression_intender::mark(getExpCtxRaw(), *makeSchema(), expressionPtr, false) ==
        aggregate_expression_intender::Intention::Marked);
}

TEST_F(AggregateExpressionIntenderTest, MarkReportsNoEncryptedPlaceholders) {
    auto query = R"({
        "$eq": [
            "$safeString",
            "$otherSafeString"
        ]
    })";
    BSONObjBuilder bob;
    auto expressionPtr = Expression::parseOperand(getExpCtxRaw(),
                                                  mongo::fromjson(addObjectWrapper(query))[""],
                                                  getExpCtx()->variablesParseState);
    ASSERT_TRUE(
        aggregate_expression_intender::mark(getExpCtxRaw(), *makeSchema(), expressionPtr, false) ==
        aggregate_expression_intender::Intention::NotMarked);
}

TEST_F(AggregateExpressionIntenderTest, ComparisonFailsOnRandomToDeterministic) {
    ASSERT_THROWS_CODE(
        stateIntention(R"({"$eq": ["$randomString", "$safeString"]})"), AssertionException, 31158);
}

TEST_F(AggregateExpressionIntenderTest, ComparisonFailsOnRandomToLiteral) {
    ASSERT_THROWS_CODE(
        stateIntention(R"({"$eq": ["$randomString", "helloworld"]})"), AssertionException, 31158);
}

TEST_F(AggregateExpressionIntenderTest, ComparisonFailsOnRandomToRandom) {
    ASSERT_THROWS_CODE(stateIntention(R"({"$eq": ["$randomString", "$randomString"]})"),
                       AssertionException,
                       31158);
}

TEST_F(AggregateExpressionIntenderTest, ComparisonFailsWithPrefixOfRandom) {
    ASSERT_THROWS_CODE(
        stateIntention(R"({"$eq": ["$nestedRandom", "literal"]})"), AssertionException, 31131);
}

TEST_F(AggregateExpressionIntenderTest, IsNumberFailsOnEncryptedField) {
    ASSERT_THROWS_CODE(stateIntention(R"({"$isNumber": "$safeInt"})"), AssertionException, 31110);
}

TEST_F(AggregateExpressionIntenderTest, ArrayLiteralShouldSucceedWhenEverythingIsUnencrypted) {
    ASSERT_IDENTITY(
        "{ \"$cond\" : [ { \"$eq\" : [ \"$unsafeString\", { \"$const\" : \"hello\" } ] }, "
        "[ { \"$const\" : 3 } ], [ { \"$const\" : 4 } ] ] }",
        stateIntention);

    ASSERT_IDENTITY(
        "{ \"$cond\" : [ { \"$eq\" : [ \"$unsafeString\", { \"$const\" : \"hello\" } ] }, "
        "[ { \"$const\" : 3 } ], [ { \"$add\" : [ { \"$const\" : 4 }, { \"$const\" : 5 } ] } ] ] }",
        stateIntention);

    ASSERT_IDENTITY(
        "{ \"$eq\" : [ \"$unsafeString\", { \"$arrayElemAt\" : ["
        " [ { \"$const\" : \"hello\" } ], { \"$const\" : 0 } ] } ] }",
        stateIntention);

    ASSERT_IDENTITY(
        "{ \"$cond\" : [ { \"$eq\" : [ \"$unsafeString\", { \"$const\" : \"hello\" } ] }, "
        "[ { \"$const\" : 3 } ], { \"$cond\" : ["
        " { \"$eq\" : [ \"$otherUnsafeString\", { \"$const\" : \"world\" } ] },"
        " [ { \"$const\" : 4 } ],"
        " [ { \"$const\" : 5 } ] ] } ] }",
        stateIntention);
}

TEST_F(AggregateExpressionIntenderTest, FunctionFailsOnEncryptedField) {
    ASSERT_THROWS_CODE(
        stateIntention(
            "{$function: {body: 'function() { return true; }', args: ['$safeInt'], lang: 'js'}}"),
        AssertionException,
        31110);
}

TEST_F(AggregateExpressionIntenderTest, FunctionFailsWithPrefixOfRandom) {
    ASSERT_THROWS_CODE(stateIntention("{$function: {body: 'function() { return true; }', args: "
                                      "['$nestedRandom'], lang: 'js'}}"),
                       AssertionException,
                       31131);
}

TEST_F(AggregateExpressionIntenderTest, FunctionFailsWithCurrentDocument) {
    ASSERT_THROWS_CODE(
        stateIntention(
            "{$function: {body: 'function() { return true; }', args: ['$$CURRENT'], lang: 'js'}}"),
        AssertionException,
        31121);
}

}  // namespace
}  // namespace mongo
