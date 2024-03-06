/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "aggregate_expression_intender_entry.h"
#include "aggregate_expression_intender_range.h"
#include "encryption_schema_tree.h"
#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_crypto.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/util/assert_util.h"
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
                        "keyId": {'$binary': "BSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
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
                        "keyId": {'$binary': "BSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "nested.age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "sparsity": 1}
                    },
                    {
                        "keyId": {'$binary': "CSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "salary",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 1000000000, "sparsity": 1}
                    },
                    {
                        "keyId": {'$binary': "DSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "ssn",
                        "bsonType": "string",
                        "queries": {"queryType": "equality"}
                    }
                ]
            }
        )");

        limitsBackingBSON =
            BSON("minDouble" << -std::numeric_limits<double>::infinity() << "maxDouble"
                             << std::numeric_limits<double>::infinity());
        kMinDouble = limitsBackingBSON["minDouble"];
        kMaxDouble = limitsBackingBSON["maxDouble"];
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

    UUID kSalaryUUID() {
        return uassertStatusOK(UUID::parse("05234567-89ab-cdef-edcb-a98765432101"));
    }

    UUID kSalaryUUIDAgg() {
        return uassertStatusOK(UUID::parse("09234567-89ab-cdef-edcb-a98765432101"));
    }

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

    BSONObj normalizeMatchExpression(BSONObj q) {
        auto parsedMatch = uassertStatusOK(
            MatchExpressionParser::parse(q,
                                         getExpCtx(),
                                         ExtensionsCallbackNoop(),
                                         MatchExpressionParser::kAllowAllSpecialFeatures));
        return parsedMatch->serialize();
    }

    QueryTypeConfig getAgeConfig() {
        auto config = QueryTypeConfig(QueryTypeEnum::Range);
        config.setContention(4);
        config.setSparsity(1);
        config.setMin(Value(0));
        config.setMax(Value(200));
        return config;
    }

    QueryTypeConfig getSalaryConfig() {
        auto config = QueryTypeConfig(QueryTypeEnum::Range);
        config.setContention(4);
        config.setSparsity(1);
        config.setMin(Value(0));
        config.setMax(Value(1000000000));
        return config;
    }

    BSONObj wrapObj(BSONObj innerObj) {
        return BSON("" << innerObj);
    }


    auto markAggExpressionForRange(boost::intrusive_ptr<Expression> expressionPtr,
                                   bool expressionIsCompared,
                                   aggregate_expression_intender::Intention expectedIntention) {

        // The command for the params is not relevant for this test.
        auto params = query_analysis::QueryAnalysisParams(kAllFields, BSONObj());
        auto schemaTree = EncryptionSchemaTreeNode::parse(params);
        auto intention = aggregate_expression_intender::markRange(getExpCtxRaw(),
                                                                  *schemaTree,
                                                                  expressionPtr,
                                                                  expressionIsCompared,
                                                                  FLE2FieldRefExpr::allowed);
        ASSERT(intention == expectedIntention);
        return expressionPtr;
    }

    auto markAggExpressionForRange(const BSONObj& unparsedExpr,
                                   bool expressionIsCompared,
                                   aggregate_expression_intender::Intention expectedIntention) {
        auto expressionPtr =
            Expression::parseObject(getExpCtxRaw(), unparsedExpr, getExpCtx()->variablesParseState);
        return markAggExpressionForRange(
            std::move(expressionPtr), expressionIsCompared, expectedIntention);
    }

    Value markAggExpressionForRangeAndSerialize(
        boost::intrusive_ptr<Expression> expressionPtr,
        bool expressionIsCompared,
        aggregate_expression_intender::Intention expectedIntention) {
        return markAggExpressionForRange(expressionPtr, expressionIsCompared, expectedIntention)
            ->serialize(false);
    }

    Value markAggExpressionForRangeAndSerialize(
        const BSONObj& unparsedExpr,
        bool expressionIsCompared,
        aggregate_expression_intender::Intention expectedIntention) {

        return markAggExpressionForRange(
                   std::move(unparsedExpr), expressionIsCompared, expectedIntention)
            ->serialize(false);
    }


    template <class N, class X>
    auto buildEncryptedBetween(std::string fieldName,
                               N min,
                               bool minIncluded,
                               X max,
                               bool maxIncluded,
                               QueryTypeConfig config,
                               boost::optional<UUID> uuid = boost::none) {
        auto tempObj = BSON("min" << min << "max" << max);
        return buildExpressionEncryptedBetweenWithPlaceholder(getExpCtxRaw(),
                                                              fieldName,
                                                              uuid.value_or(kDefaultUUID()),
                                                              config,
                                                              {tempObj["min"], minIncluded},
                                                              {tempObj["max"], maxIncluded});
    }
    template <class N, class X>
    auto buildAndSerializeEncryptedBetween(StringData fieldName,
                                           N min,
                                           bool minIncluded,
                                           X max,
                                           bool maxIncluded,
                                           QueryTypeConfig config,
                                           boost::optional<UUID> uuid = boost::none) {
        auto encryptedBetween = buildEncryptedBetween(
            std::string(fieldName), min, minIncluded, max, maxIncluded, config, uuid);
        return encryptedBetween->serialize(false);
    }

    template <class N, class X>
    BSONObj buildRangePlaceholder(StringData fieldname,
                                  N min,
                                  bool minIncluded,
                                  X max,
                                  bool maxIncluded,
                                  int32_t payloadId,
                                  Fle2RangeOperator firstOp,
                                  boost::optional<Fle2RangeOperator> secondOp = boost::none,
                                  boost::optional<QueryTypeConfig> config = boost::none,
                                  boost::optional<UUID> uuid = boost::none) {
        auto tempObj = BSON("min" << min << "max" << max);
        if (!config) {
            config = getAgeConfig();
        }
        return makeAndSerializeRangePlaceholder(fieldname,
                                                uuid.value_or(kDefaultUUID()),
                                                config.value(),
                                                {tempObj["min"], minIncluded},
                                                {tempObj["max"], maxIncluded},
                                                payloadId,
                                                firstOp,
                                                secondOp);
    }

    BSONObj buildRangeStub(StringData fieldname,
                           int32_t payloadId,
                           Fle2RangeOperator firstOp,
                           Fle2RangeOperator secondOp,
                           boost::optional<QueryTypeConfig> config = boost::none,
                           boost::optional<UUID> uuid = boost::none) {
        if (!config) {
            config = getAgeConfig();
        }
        return makeAndSerializeRangeStub(
            fieldname, uuid.value_or(kDefaultUUID()), config.value(), payloadId, firstOp, secondOp);
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

    FLE2RangeFindSpec getEncryptedRange(const FLE2EncryptionPlaceholder& placeholder) {
        auto rangeObj = placeholder.getValue().getElement().Obj();
        return FLE2RangeFindSpec::parse(IDLParserContext("range"), rangeObj);
    }
};
}  // namespace mongo
