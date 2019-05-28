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

#include "fle_pipeline.h"

#include "fle_test_fixture.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
const BSONObj encryptObj = BSON("encrypt" << BSON("algorithm"
                                                  << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                                                  << "keyId"
                                                  << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))
                                                  << "bsonType"
                                                  << "string"));
const BSONObj kAdditionalPropertiesSchema = BSON("type"
                                                 << "object"
                                                 << "additionalProperties"
                                                 << encryptObj);

const BSONObj kPatternPropertiesSchema = BSON("type"
                                              << "object"
                                              << "patternProperties"
                                              << BSON("foo" << encryptObj));

const BSONObj kMultiEncryptSchema = BSON(
    "type"
    << "object"
    << "properties"
    << BSON("person" << encryptObj << "user"
                     << BSON("type"
                             << "object"
                             << "properties"
                             << BSON("ssn" << encryptObj << "address" << encryptObj << "accounts"
                                           << BSON("type"
                                                   << "object"
                                                   << "properties"
                                                   << BSON("bank" << encryptObj))))));

class FLEPipelineTest : public FLETestFixture {
public:
    /**
     * Given a pipeline and an input schema, returns the schema of documents flowing out of the last
     * stage of the pipeline.
     */
    const EncryptionSchemaTreeNode& getSchemaForStage(const std::vector<BSONObj>& pipeline,
                                                      BSONObj inputSchema) {
        auto parsedPipeline = uassertStatusOK(Pipeline::parse(pipeline, getExpCtx()));
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema, EncryptionSchemaType::kLocal);
        _flePipe = std::make_unique<FLEPipeline>(std::move(parsedPipeline), *schema.get());
        return _flePipe->getOutputSchema();
    }

    /**
     * Given a pipeline and an input schema, returns a new serialized pipeline with encrypted fields
     * replaced by their appropriate intent-to-encrypt markings.
     */
    std::vector<BSONObj> translatePipeline(const std::vector<BSONObj>& pipeline,
                                           BSONObj inputSchema) {
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema, EncryptionSchemaType::kLocal);
        FLEPipeline flePipe(uassertStatusOK(Pipeline::parse(pipeline, getExpCtx())), *schema.get());
        std::vector<BSONObj> serialized;
        for (const auto& stage : flePipe.getPipeline().serialize()) {
            ASSERT(stage.getType() == BSONType::Object);
            serialized.push_back(stage.getDocument().toBson());
        }

        return serialized;
    }

private:
    std::unique_ptr<FLEPipeline> _flePipe;
};

TEST_F(FLEPipelineTest, ThrowsOnInvalidOrUnsupportedStage) {
    // Setup involved namespaces to avoid crashing on pipeline parse.
    NamespaceString fromNs("test", "other");
    getExpCtx()->setResolvedNamespaces(
        {{fromNs.coll().toString(), {fromNs, std::vector<BSONObj>{}}}});
    std::vector<BSONObj> stageSpecs = {
        fromjson(R"({$lookup: {
            from: "other", 
            localField: "ssn", 
            foreignField: "sensitive", 
            as: "res"
        }})"),
        fromjson(R"({$graphLookup: {
            from: "other",
            startWith: "$reportsTo",
            connectFromField: "reportsTo",
            connectToField: "name",
            as: "reportingHierarchy"
        }})"),
        fromjson(R"({$facet: {
            "pipeline1": [
                { $unwind: "$tags" },
                { $sortByCount: "$tags" }
            ],
            "pipeline2": [
                { $match: { ssn: 5}}
            ]
        }})"),
        fromjson("{$group: {_id: null}}"),
        fromjson("{$unwind: '$ssn'}"),
        fromjson("{$redact: '$$DESCEND'}"),
        fromjson("{$bucketAuto: {groupBy: '$_id', buckets: 2}}"),
        fromjson("{$planCacheStats: {}}"),
        fromjson("{$_internalInhibitOptimization: {}}"),
        fromjson("{$out: 'other'}"),
    };

    for (auto&& stage : stageSpecs) {
        ASSERT_THROWS_CODE(
            getSchemaForStage({stage}, kDefaultSsnSchema), AssertionException, 31011);
    }
}

TEST_F(FLEPipelineTest, ThrowsOnInvalidCollectionlessAggregations) {
    getExpCtx()->ns = NamespaceString::makeCollectionlessAggregateNSS("admin");
    ASSERT_THROWS_CODE(getSchemaForStage({fromjson("{$currentOp: {}}")}, kDefaultSsnSchema),
                       AssertionException,
                       31011);
}

TEST_F(FLEPipelineTest, LimitStageTreatedAsNoop) {
    const auto& outputSchema = getSchemaForStage({fromjson("{$limit: 1}")}, kDefaultSsnSchema);
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef("notSsn")));
}

TEST_F(FLEPipelineTest, MatchWithSingleTopLevelEncryptedField) {
    auto result = translatePipeline({fromjson("{$match: {ssn: '5'}}")}, kDefaultSsnSchema);
    auto expectedMatch = serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: '5'}"));
    ASSERT_EQ(1UL, result.size());
    ASSERT_BSONOBJ_EQ(result[0]["$match"].Obj(), expectedMatch);
}

TEST_F(FLEPipelineTest, MatchPrecededByNoopStageCorrectlyMarksTopLevelField) {
    auto result = translatePipeline({fromjson("{$limit: 1}"), fromjson("{$match: {ssn: '5'}}")},
                                    kDefaultSsnSchema);
    auto expectedMatch = serializeMatchForEncryption(kDefaultSsnSchema, fromjson("{ssn: '5'}"));
    ASSERT_EQ(2UL, result.size());
    ASSERT_BSONOBJ_EQ(result[1]["$match"].Obj(), expectedMatch);
}

TEST_F(FLEPipelineTest, MatchWithSingleDottedPathEncryptedField) {
    auto result =
        translatePipeline({fromjson("{$match: {'user.ssn': '5'}}")}, kDefaultNestedSchema);
    auto expectedMatch =
        serializeMatchForEncryption(kDefaultNestedSchema, fromjson("{'user.ssn': '5'}"));
    ASSERT_EQ(1UL, result.size());
    ASSERT_BSONOBJ_EQ(result[0]["$match"].Obj(), expectedMatch);
}

TEST_F(FLEPipelineTest, MatchWithEncryptedPrefixCorrectlyFails) {
    ASSERT_THROWS_CODE(
        translatePipeline({fromjson("{$match: {'ssn.nested': 'not allowed'}}")}, kDefaultSsnSchema),
        AssertionException,
        51102);
}

TEST_F(FLEPipelineTest, MatchWithInvalidComparisonToEncryptedFieldCorrectlyFails) {
    ASSERT_THROWS_CODE(
        translatePipeline({fromjson("{$match: {ssn: {$gt: 5}}}")}, kDefaultSsnSchema),
        AssertionException,
        51118);
}

TEST_F(FLEPipelineTest, InclusionProjectionWithNoEncryptedFieldsSucceeds) {
    BSONObj projection = BSON("$project" << BSON("_id" << 1));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(schema.containsEncryptedNode());
}

TEST_F(FLEPipelineTest, InclusionProjectionWithOneEncryptedFieldHasOneEncryptedPath) {
    BSONObj projection = BSON("$project" << BSON("person" << 1));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
}

TEST_F(FLEPipelineTest, InclusionProjectionPreservesTwoEncryptedPaths) {
    BSONObj projection = BSON("$project" << BSON("person" << 1 << "user" << BSON("address" << 1)));
    const auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
}

TEST_F(FLEPipelineTest, InclusionProjectionPreservesAllEncryptedPaths) {
    BSONObj projection =
        BSON("$project" << BSON("person" << 1 << "user" << BSON("ssn" << 1 << "address" << 1)));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
}

TEST_F(FLEPipelineTest, InclusionProjectionPreservesIncludedSubtrees) {
    BSONObj projection = BSON("$project" << BSON("user" << 1));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionWithNoEncryptedFieldsSucceeds) {
    BSONObj projection = BSON("$project" << BSON("test" << 0));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionWithOneEncryptedTopLevelFieldSucceeds) {
    BSONObj projection = BSON("$project" << BSON("person" << 0));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionWithOneEncryptedSecondLevelFieldSucceeds) {
    BSONObj projection = BSON("$project" << BSON("user" << BSON("address" << 0)));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionWithMultipleEncryptedSecondLevelFieldsSucceeds) {
    BSONObj projection = BSON("$project" << BSON("user" << BSON("address" << 0 << "ssn" << 0)));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionWithManyEncryptedFieldsSucceeds) {
    BSONObj projection =
        BSON("$project" << BSON("person" << 0 << "user" << BSON("address" << 0 << "ssn" << 0)));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionOfTopLevelFieldRemovesAllSubFields) {
    BSONObj projection = BSON("$project" << BSON("user" << 0));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
}

TEST_F(FLEPipelineTest, ExclusionOfAllFieldsSucceeds) {
    BSONObj projection =
        BSON("$project" << BSON("person" << 0 << "user"
                                         << BSON("address" << 0 << "ssn" << 0 << "accounts"
                                                           << BSON("bank" << 0))));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("person")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.address")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.ssn")));
    ASSERT_FALSE(schema.getEncryptionMetadataForPath(FieldRef("user.accounts.bank")));
    ASSERT_FALSE(schema.containsEncryptedNode());
}

TEST_F(FLEPipelineTest, InclusionOfAdditionalPropertiesSucceeds) {
    BSONObj projection = BSON("$project" << BSON("add" << 1));
    auto& schema = getSchemaForStage({projection}, kAdditionalPropertiesSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("add")));
}

TEST_F(FLEPipelineTest, InclusionOfPatternPropertiesSucceeds) {
    BSONObj projection = BSON("$project" << BSON("foo" << 1));
    auto& schema = getSchemaForStage({projection}, kPatternPropertiesSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("foo")));
}

TEST_F(FLEPipelineTest, EmptyPipelineCorrectlyPropagatesInputSchema) {
    const auto& outputSchema = getSchemaForStage({}, kDefaultSsnSchema);
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef("notSsn")));
}

TEST_F(FLEPipelineTest, InclusionIncludesIDEvenIfNotSpecified) {
    BSONObj projection = BSON("$project" << BSON("user" << 1));
    auto jsonSchema = fromjson(R"({
             type: "object",
             properties: {
                 _id: {
                     encrypt: {
                         algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                         keyId: [{'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}],
                         bsonType: "string"
                     }
                 }
            }
        })");

    auto& schema = getSchemaForStage({projection}, jsonSchema);
    ASSERT_TRUE(schema.getEncryptionMetadataForPath(FieldRef("_id")));
}

TEST_F(FLEPipelineTest, ExclusionWithPatternPropertiesKeepsNonNamedProperties) {
    BSONObj projection = BSON("$project" << BSON("foo" << 0));
    const auto& outputSchema = getSchemaForStage({projection}, kPatternPropertiesSchema);
    ASSERT_TRUE(outputSchema.containsEncryptedNode());
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef{"foo"}));
}

TEST_F(FLEPipelineTest, ExclusionWithAdditionalPropertiesKeepsNonNamedProperties) {
    BSONObj projection = BSON("$project" << BSON("user" << 0));
    const auto& outputSchema = getSchemaForStage({projection}, kAdditionalPropertiesSchema);
    ASSERT_TRUE(outputSchema.containsEncryptedNode());
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef{"user"}));
}

}  // namespace
}  // namespace mongo
