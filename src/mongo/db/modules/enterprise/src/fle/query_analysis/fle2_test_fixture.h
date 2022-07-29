/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "encryption_schema_tree.h"
#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "query_analysis.h"

namespace mongo {
class FLE2TestFixture : public AggregationContextFixture {
protected:
    void setUp() {
        kSsnFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "ssn",
                        "bsonType": "string",
                        "queries": {"queryType": "equality"}
                    }
                ]
            }
        )");
        kAgeFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "sparsity": 1}
                    }
                ]
            }
        )");
    }
    // Encrypted SSN field with equality index.
    BSONObj kSsnFields;
    // Encrypted age field with range index.
    BSONObj kAgeFields;
    // Key UUID to be used in encryption placeholders.
    UUID kDefaultUUID() {
        return uassertStatusOK(UUID::parse("01234567-89ab-cdef-edcb-a98765432101"));
    };

    BSONObj markMatchExpression(const BSONObj& fields, const BSONObj& matchExpression) {
        auto cmd = BSON("find"
                        << "coll"
                        << "filter" << matchExpression);
        auto params = query_analysis::QueryAnalysisParams(fields, cmd);

        auto schemaTree = EncryptionSchemaTreeNode::parse(params);
        auto parsedMatch = uassertStatusOK(
            MatchExpressionParser::parse(matchExpression,
                                         getExpCtx(),
                                         ExtensionsCallbackNoop(),
                                         MatchExpressionParser::kAllowAllSpecialFeatures));
        FLEMatchExpression fleMatchExpression{std::move(parsedMatch), *schemaTree};
        return fleMatchExpression.getMatchExpression()->serialize();
    }

    BSONObj buildRangePlaceholder(StringData fieldname, BSONElement min, BSONElement max) {
        auto expr = buildEncryptedBetweenWithPlaceholder(fieldname, kDefaultUUID(), 4, min, max);
        return BSON(fieldname << expr->rhs());
    }
};
}  // namespace mongo
