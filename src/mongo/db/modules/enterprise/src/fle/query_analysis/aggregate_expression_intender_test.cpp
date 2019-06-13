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

#include <string>

#include "aggregate_expression_intender.h"
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

    auto stateIntention(StringData expression) {
        BSONObjBuilder bob;
        auto expressionPtr =
            Expression::parseOperand(getExpCtx(),
                                     mongo::fromjson(addObjectWrapper(expression))[""],
                                     getExpCtx()->variablesParseState);
        auto intention = aggregate_expression_intender::mark(
            *getExpCtx(), *makeSchema(), expressionPtr.get(), false);
        ASSERT(intention == aggregate_expression_intender::Intention::Marked ||
               intention == aggregate_expression_intender::Intention::NotMarked);
        expressionPtr->serialize(false).addToBsonObj(&bob, "");
        return removeObjectWrapper(tojson(bob.obj()));
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
    ASSERT_EQ(stateIntention("{ \"$eq\" : [ { \"$reduce\" : { \"input\" : [], \"initialValue\" : "
                             "{}, \"in\" : { \"$const\" : \"hello\" } } }, \"$safeString\" ] }"),
              "{ \"$eq\" : [ { \"$reduce\" : { \"input\" : [], \"initialValue\" : {}, \"in\" : { "
              "\"$const\" : { \"$binary\" : "
              "\"ADIAAAAQYQABAAAABWtpABAAAAAEASNFZ4mrze/ty6mHZUMhAQJ2AAYAAABoZWxsbwAA\", \"$type\" "
              ": \"06\" } } } }, \"$safeString\" ] }");
}

TEST_F(AggregateExpressionIntenderTest, LetForbidsBindingToEncryptedValue) {
    ASSERT_THROWS_CODE(
        stateIntention(
            "{ \"$let\" : { \"vars\" : { \"dontTouchIt\" : \"$safeInt\" }, \"in\": \"opps\" } }"),
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
    auto expressionPtr = Expression::parseOperand(getExpCtx(),
                                                  mongo::fromjson(addObjectWrapper(query))[""],
                                                  getExpCtx()->variablesParseState);
    ASSERT_TRUE(aggregate_expression_intender::mark(
                    *getExpCtx(), *makeSchema(), expressionPtr.get(), false) ==
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
    auto expressionPtr = Expression::parseOperand(getExpCtx(),
                                                  mongo::fromjson(addObjectWrapper(query))[""],
                                                  getExpCtx()->variablesParseState);
    ASSERT_TRUE(aggregate_expression_intender::mark(
                    *getExpCtx(), *makeSchema(), expressionPtr.get(), false) ==
                aggregate_expression_intender::Intention::NotMarked);
}
}  // namespace
}  // namespace mongo
