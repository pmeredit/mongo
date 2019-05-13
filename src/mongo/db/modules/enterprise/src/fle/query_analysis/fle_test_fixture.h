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

#pragma once

#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

namespace mongo {

class FLETestFixture : public AggregationContextFixture {
protected:
    void setUp() {
        kDefaultSsnSchema = fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [{'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                        bsonType: "string"
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
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                                bsonType: "string"
                            }
                        }
                    }
                }
            }
        })");
    }

    /**
     * Wraps 'value' in a BSONElement and returns a BSONObj representing the EncryptionPlaceholder.
     * Performs validity checks against 'value' as though this placeholder is in a "comparison
     * context" with no collation.
     *
     * The first element in the BSONObj will have a value of BinData sub-type 6 for the placeholder.
     */
    template <class T>
    BSONObj buildEncryptElem(T value, const ResolvedEncryptionInfo& metadata) {
        auto tempObj = BSON("v" << value);
        return buildEncryptPlaceholder(
            tempObj.firstElement(), metadata, EncryptionPlaceholderContext::kComparison, nullptr);
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

    std::vector<std::uint8_t> kInitializationVector{0x6D, 0x6F, 0x6E, 0x67, 0x6F};

    // Default metadata, see initialization above for actual values.
    ResolvedEncryptionInfo kDefaultMetadata{
        EncryptSchemaKeyId{std::vector<UUID>{
            uassertStatusOK(UUID::parse("01234567-89ab-cdef-edcb-a98765432101"))}},
        FleAlgorithmEnum::kDeterministic,
        MatcherTypeSet{BSONType::String}};
};

}  // namespace mongo
