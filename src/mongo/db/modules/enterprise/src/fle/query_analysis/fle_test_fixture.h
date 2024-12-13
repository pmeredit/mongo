/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

namespace mongo {

class FLETestFixture : public AggregationContextFixture {
protected:
    FLETestFixture() = default;
    explicit FLETestFixture(NamespaceString nss) : AggregationContextFixture(nss) {}
    void setUp() override;

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
        return buildEncryptPlaceholder(tempObj.firstElement(),
                                       metadata,
                                       query_analysis::EncryptionPlaceholderContext::kComparison,
                                       nullptr);
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
        auto schemaTree = EncryptionSchemaTreeNode::parse(schema, EncryptionSchemaType::kLocal);

        // By default, allow all features for testing.
        auto parsedMatch = uassertStatusOK(
            MatchExpressionParser::parse(matchExpression,
                                         getExpCtx(),
                                         ExtensionsCallbackNoop(),
                                         MatchExpressionParser::kAllowAllSpecialFeatures));
        FLEMatchExpression fleMatchExpression{std::move(parsedMatch), *schemaTree};

        return fleMatchExpression.getMatchExpression()->serialize();
    }

    // Default schema where only the path 'ssn' is encrypted.
    BSONObj kDefaultSsnSchema;

    // Schema which defines a 'user' object with a nested 'ssn' encrypted field.
    BSONObj kDefaultNestedSchema;

    // Schema where every field is an encrypted string.
    BSONObj kAllEncryptedSchema;

    std::vector<std::uint8_t> kInitializationVector{0x6D, 0x6F, 0x6E, 0x67, 0x6F};

    // Default metadata, see initialization above for actual values.
    ResolvedEncryptionInfo kDefaultMetadata{
        EncryptSchemaKeyId{std::vector<UUID>{
            uassertStatusOK(UUID::parse("01234567-89ab-cdef-edcb-a98765432101"))}},
        FleAlgorithmEnum::kDeterministic,
        MatcherTypeSet{BSONType::String}};
};

}  // namespace mongo
