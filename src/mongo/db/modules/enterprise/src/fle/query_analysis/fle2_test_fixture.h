/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "encryption_schema_tree.h"
#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_crypto.h"
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
        kNestedAge = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "user.age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "sparsity": 1}
                    }
                ]
            }
        )");
        kAgeAndSalaryFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "sparsity": 1}
                    },
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "salary",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 1000000000, "sparsity": 1}
                    }
                ]
            }
        )");
        kAllFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "sparsity": 1}
                    },
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "salary",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 1000000000, "sparsity": 1}
                    },
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "ssn",
                        "bsonType": "string",
                        "queries": {"queryType": "equality"}
                    }
                ]
            }
        )");

        limitsBackingBSON = BSON("min" << -std::numeric_limits<double>::infinity() << "max"
                                       << std::numeric_limits<double>::infinity());
        kMinDouble = limitsBackingBSON["min"];
        kMaxDouble = limitsBackingBSON["max"];
    }
    // Encrypted SSN field with equality index.
    BSONObj kSsnFields;
    // Encrypted age field with range index.
    BSONObj kAgeFields;
    BSONObj kAgeAndSalaryFields;
    BSONObj kAllFields;
    BSONObj kNestedAge;

    BSONObj limitsBackingBSON;
    BSONElement kMaxDouble;
    BSONElement kMinDouble;
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

    template <class N, class X>
    BSONObj buildRangePlaceholder(
        StringData fieldname, N min, bool minIncluded, X max, bool maxIncluded) {
        auto tempObj = BSON("min" << min << "max" << max);
        auto expr = buildEncryptedBetweenWithPlaceholder(fieldname,
                                                         kDefaultUUID(),
                                                         4,
                                                         1,
                                                         {tempObj["min"], minIncluded},
                                                         {tempObj["max"], maxIncluded});
        return BSON(fieldname << expr->rhs());
    }

    template <class T>
    BSONObj buildEqualityPlaceholder(const BSONObj& fields, StringData field, T value) {
        auto cmd = BSON("find"
                        << "coll"
                        << "filter" << BSONObj());
        auto params = query_analysis::QueryAnalysisParams(fields, cmd);

        auto schemaTree = EncryptionSchemaTreeNode::parse(params);
        auto metadata = schemaTree->getEncryptionMetadataForPath(FieldRef(field));
        auto tempObj = BSON("" << value);
        return buildEncryptPlaceholder(tempObj.firstElement(),
                                       metadata.value(),
                                       query_analysis::EncryptionPlaceholderContext::kComparison,
                                       nullptr,
                                       boost::none,
                                       boost::none);
    }

    std::unique_ptr<EncryptionSchemaTreeNode> buildSchema(const BSONObj& fields) {
        auto cmd = BSON("find"
                        << "coll"
                        << "filter" << BSONObj());
        auto params = query_analysis::QueryAnalysisParams(fields, cmd);

        return EncryptionSchemaTreeNode::parse(params);
    }

    FLE2EncryptionPlaceholder parseRangePlaceholder(BSONElement elt) {
        auto cdr = binDataToCDR(elt);
        auto [encryptedType, subCdr] = fromEncryptedConstDataRange(cdr);
        tassert(6720203,
                "BinData payload not a placeholder.",
                encryptedType == EncryptedBinDataType::kFLE2Placeholder);
        return parseFromCDR<FLE2EncryptionPlaceholder>(subCdr);
    }

    FLE2RangeSpec getEncryptedRange(const FLE2EncryptionPlaceholder& placeholder) {
        auto rangeObj = placeholder.getValue().getElement().Obj();
        return FLE2RangeSpec::parse(IDLParserContext("range"), rangeObj);
    }
};
}  // namespace mongo
