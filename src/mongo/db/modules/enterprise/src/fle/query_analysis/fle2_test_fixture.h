/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
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
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/util/assert_util.h"
#include "query_analysis.h"

namespace mongo {

class FLE2TestFixture : public AggregationContextFixture {
protected:
    void setUp() override;

    // Encrypted SSN field with equality index.
    BSONObj kSsnFields;
    // Encrypted age field with bounded range index.
    BSONObj kAgeFields;
    BSONObj kAgeAndSalaryFields;
    BSONObj kAllFields;
    BSONObj kNestedAge;
    // Encrypted date field with default-bounded range index
    BSONObj kDateFields;

    BSONObj limitsBackingBSON;
    BSONElement kMaxDouble;
    BSONElement kMinDouble;
    const NamespaceString ns = NamespaceString::createNamespaceString_forTest("testdb.testcoll");

    // Key UUID to be used in encryption placeholders.
    UUID kDefaultUUID() {
        return uassertStatusOK(UUID::parse("01234567-89ab-cdef-edcb-a98765432101"));
    }

    UUID kSalaryUUID() {
        return uassertStatusOK(UUID::parse("05234567-89ab-cdef-edcb-a98765432101"));
    }

    UUID kSalaryUUIDAgg() {
        return uassertStatusOK(UUID::parse("09234567-89ab-cdef-edcb-a98765432101"));
    }

    query_analysis::QueryAnalysisParams createQueryAnalysisParamsFromFields(
        const BSONObj& fields, BSONObj strippedCmd) const {
        return query_analysis::QueryAnalysisParams(
            ns,
            BSON(ns.serializeWithoutTenantPrefix_UNSAFE() << fields),
            strippedCmd,
            FleVersion::kFle2,
            false);
    }

    BSONObj markMatchExpression(const BSONObj& fields, const BSONObj& matchExpression) {
        auto cmd = BSON("find"
                        << "coll"
                        << "filter" << matchExpression);

        auto params = createQueryAnalysisParamsFromFields(fields, cmd);
        auto schemaTree =
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params);
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
        config.setContention(1);
        config.setSparsity(1);
        config.setTrimFactor(0);
        config.setMin(Value(0));
        config.setMax(Value(200));
        return config;
    }

    QueryTypeConfig getSalaryConfig() {
        auto config = QueryTypeConfig(QueryTypeEnum::Range);
        config.setContention(1);
        config.setSparsity(1);
        config.setTrimFactor(0);
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
        auto params = createQueryAnalysisParamsFromFields(kAllFields, BSONObj());
        auto schemaTree =
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params);
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
            ->serialize();
    }

    Value markAggExpressionForRangeAndSerialize(
        const BSONObj& unparsedExpr,
        bool expressionIsCompared,
        aggregate_expression_intender::Intention expectedIntention) {
        return markAggExpressionForRange(
                   std::move(unparsedExpr), expressionIsCompared, expectedIntention)
            ->serialize();
    }

    template <class N, class X>
    auto buildEncryptedRange(std::string fieldName,
                             N min,
                             bool minIncluded,
                             X max,
                             bool maxIncluded,
                             QueryTypeConfig config,
                             int32_t placeholderId,
                             boost::optional<UUID> uuid = boost::none) {
        auto tempObj = BSON("min" << min << "max" << max);
        return buildEncryptedRangeWithPlaceholder(getExpCtxRaw(),
                                                  fieldName,
                                                  uuid.value_or(kDefaultUUID()),
                                                  config,
                                                  {tempObj["min"], minIncluded},
                                                  {tempObj["max"], maxIncluded},
                                                  placeholderId);
    }

    template <class N, class X>
    auto buildAndSerializeTwoSidedRange(StringData fieldName,
                                        N min,
                                        bool minIncluded,
                                        X max,
                                        bool maxIncluded,
                                        QueryTypeConfig config,
                                        int32_t placeholderId,
                                        boost::optional<UUID> uuid = boost::none) {
        auto encryptedRange = buildEncryptedRange(std::string(fieldName),
                                                  min,
                                                  minIncluded,
                                                  max,
                                                  maxIncluded,
                                                  config,
                                                  placeholderId,
                                                  uuid);
        return encryptedRange->serialize();
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
        auto params = createQueryAnalysisParamsFromFields(fields, cmd);
        auto schemaTree =
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params);
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
        auto params = createQueryAnalysisParamsFromFields(fields, cmd);
        return EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(params);
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
