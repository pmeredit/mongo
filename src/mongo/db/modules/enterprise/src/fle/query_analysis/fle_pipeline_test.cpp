/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "fle_pipeline.h"
#include "fle_test_fixture.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
const BSONObj encryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                           << "keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))
                           << "bsonType"
                           << "string"));

const BSONObj kPatternPropertiesSchema = BSON("type"
                                              << "object"
                                              << "patternProperties" << BSON("foo" << encryptObj));

const BSONObj kMultiEncryptSchema = BSON(
    "type"
    << "object"
    << "properties"
    << BSON("person" << encryptObj << "user"
                     << BSON("type"
                             << "object"
                             << "properties"
                             << BSON("ssn"
                                     << encryptObj << "address" << encryptObj << "accounts"
                                     << BSON("type"
                                             << "object"
                                             << "properties" << BSON("bank" << encryptObj))))));

class FLEPipelineTest : public FLETestFixture {
public:
    /**
     * Given a pipeline and an input schema, returns the schema of documents flowing out of the last
     * stage of the pipeline.
     */
    const EncryptionSchemaTreeNode& getSchemaForStage(const std::vector<BSONObj>& pipeline,
                                                      BSONObj inputSchema) {
        auto parsedPipeline = Pipeline::parse(pipeline, getExpCtx());
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema, EncryptionSchemaType::kLocal);
        _flePipe = std::make_unique<FLEPipeline>(std::move(parsedPipeline), *schema.get());
        return _flePipe->getOutputSchema();
    }

    bool doesHavePlaceholders(const std::vector<BSONObj>& pipeline, BSONObj inputSchema) {
        auto parsedPipeline = Pipeline::parse(pipeline, getExpCtx());
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema, EncryptionSchemaType::kLocal);
        return FLEPipeline(std::move(parsedPipeline), *schema.get()).hasEncryptedPlaceholders;
    }

    /**
     * Given a pipeline and an input schema, returns a new serialized pipeline with encrypted fields
     * replaced by their appropriate intent-to-encrypt markings.
     */
    std::vector<BSONObj> translatePipeline(const std::vector<BSONObj>& pipeline,
                                           BSONObj inputSchema) {
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema, EncryptionSchemaType::kLocal);
        FLEPipeline flePipe(Pipeline::parse(pipeline, getExpCtx()), *schema.get());
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
    NamespaceString fromNs = NamespaceString::createNamespaceString_forTest("test", "other");
    getExpCtx()->setNamespaceString(fromNs);
    getExpCtx()->setResolvedNamespaces(
        {{fromNs.coll().toString(), {fromNs, std::vector<BSONObj>{}}}});
    std::vector<BSONObj> stageSpecs = {
        fromjson(R"({$facet: {
            "pipeline1": [
                { $unwind: "$tags" },
                { $sortByCount: "$tags" }
            ],
            "pipeline2": [
                { $match: { ssn: 5}}
            ]
        }})"),
        fromjson("{$redact: '$$DESCEND'}"),
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
    getExpCtx()->setNamespaceString(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin));
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
    ASSERT_FALSE(schema.mayContainEncryptedNode());
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
    ASSERT_FALSE(schema.mayContainEncryptedNode());
}

TEST_F(FLEPipelineTest, InclusionOfAdditionalPropertiesSucceeds) {
    BSONObj projection = BSON("$project" << BSON("add" << 1));
    auto& schema = getSchemaForStage({projection}, kAllEncryptedSchema);
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
    ASSERT_TRUE(outputSchema.mayContainEncryptedNode());
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef{"foo"}));
}

TEST_F(FLEPipelineTest, ExclusionWithAdditionalPropertiesKeepsNonNamedProperties) {
    BSONObj projection = BSON("$project" << BSON("user" << 0));
    const auto& outputSchema = getSchemaForStage({projection}, kAllEncryptedSchema);
    ASSERT_TRUE(outputSchema.mayContainEncryptedNode());
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef{"user"}));
}

TEST_F(FLEPipelineTest, InclusionRenameMovesEncryptionMetadata) {
    BSONObj projection = BSON("$project" << BSON("user"
                                                 << "$ssn"));
    const auto& outputSchema = getSchemaForStage({projection}, kDefaultSsnSchema);

    ASSERT(outputSchema.getEncryptionMetadataForPath(FieldRef{"user"}) == kDefaultMetadata);
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef{"ssn"}));
}

TEST_F(FLEPipelineTest, InclusionRenameOverwritesEncryptionMetadata) {
    BSONObj projection = BSON("$project" << BSON("user"
                                                 << "$ssn"));
    const BSONObj secondEncryptObj =
        BSON("encrypt" << BSON("algorithm"
                               << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                               << "keyId" << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))
                               << "bsonType"
                               << "string"));
    const auto inputSchema = BSON("type"
                                  << "object"
                                  << "properties"
                                  << BSON("user" << encryptObj << "ssn" << secondEncryptObj));
    const auto& outputSchema = getSchemaForStage({projection}, inputSchema);
    auto inputTree = EncryptionSchemaTreeNode::parse(inputSchema, EncryptionSchemaType::kLocal);
    ASSERT(outputSchema.getEncryptionMetadataForPath(FieldRef{"user"}) ==
           inputTree->getEncryptionMetadataForPath(FieldRef("ssn")));
}

TEST_F(FLEPipelineTest, RenameFromPrefixOfEncryptedFieldWorks) {
    BSONObj projection = BSON("$project" << BSON("newName"
                                                 << "$user"));
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kMultiEncryptSchema, EncryptionSchemaType::kLocal);
    const auto& outputSchema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT(*outputSchema.getNode(FieldRef("newName")) == *inputSchema->getNode(FieldRef("user")));
}

TEST_F(FLEPipelineTest, RenameFromPatternPropertiesKeepsEncryptionMetadata) {
    BSONObj projection = BSON("$project" << BSON("newName"
                                                 << "$foo"));
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kPatternPropertiesSchema, EncryptionSchemaType::kLocal);
    const auto& outputSchema = getSchemaForStage({projection}, kPatternPropertiesSchema);
    ASSERT(*outputSchema.getNode(FieldRef("newName")) == *inputSchema->getNode(FieldRef("foo")));
    ASSERT(outputSchema.getNode(FieldRef("newName"))->getEncryptionMetadata());
}

TEST_F(FLEPipelineTest, RenameFromAdditionalPropertiesKeepsEncryptionMetadata) {
    BSONObj projection = BSON("$project" << BSON("newName"
                                                 << "$foo"));
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    const auto& outputSchema = getSchemaForStage({projection}, kAllEncryptedSchema);
    ASSERT(*outputSchema.getNode(FieldRef("newName")) == *inputSchema->getNode(FieldRef("foo")));
    ASSERT(outputSchema.getNode(FieldRef("newName"))->getEncryptionMetadata());
}

TEST_F(FLEPipelineTest, InclusionEvaluatedExpressionReturnsNotEncrypted) {
    BSONObj projection =
        BSON("$project" << BSON("newField" << BSON("$add" << BSON_ARRAY("$oldOne"
                                                                        << "$oldTwo"))));
    const auto& outputSchema = getSchemaForStage({projection}, kDefaultSsnSchema);
    ASSERT_FALSE(outputSchema.mayContainEncryptedNode());
}

TEST_F(FLEPipelineTest, InclusionWithExpressionProperlyBuildsSchema) {
    BSONObj projection = BSON("$project" << BSON("person" << 1 << "newField"
                                                          << "$user.ssn"
                                                          << "unEncrypted" << 1));
    const auto& outputSchema = getSchemaForStage({projection}, kMultiEncryptSchema);
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kMultiEncryptSchema, EncryptionSchemaType::kLocal);
    ASSERT(outputSchema.getEncryptionMetadataForPath(FieldRef{"person"}) ==
           *inputSchema->getEncryptionMetadataForPath(FieldRef{"person"}));
    ASSERT(outputSchema.getEncryptionMetadataForPath(FieldRef{"newField"}) ==
           *inputSchema->getEncryptionMetadataForPath(FieldRef{"user.ssn"}));
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef{"unEncrypted"}));
}

TEST_F(FLEPipelineTest,
       InclusionWithEncryptedExpressionAtNestedPathReturnsEncryptionSchemaStateMixedNode) {
    BSONObj projection = BSON("$project" << BSON("newField.secondary"
                                                 << "$user.ssn"));
    ASSERT_THROWS_CODE(getSchemaForStage({projection}, kMultiEncryptSchema)
                           .getEncryptionMetadataForPath(FieldRef("newField")),
                       AssertionException,
                       31133);

    projection = BSON("$project" << BSON("a.very.deep.path"
                                         << "$user.ssn"));
    ASSERT_THROWS_CODE(getSchemaForStage({projection}, kMultiEncryptSchema)
                           .getEncryptionMetadataForPath(FieldRef("a")),
                       AssertionException,
                       31133);
}

TEST_F(FLEPipelineTest, InclusionWithUnEncryptedExpressionAtNestedPathReturnsNotEncrypted) {
    BSONObj projection = BSON("$project" << BSON("person" << 1 << "newField.secondary"
                                                          << "$user.notEncrypted"
                                                          << "unEncrypted" << 1));
    const auto& outputSchema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef{"newField.secondary"}));
}

TEST_F(FLEPipelineTest, AddFieldsRenameMovesEncryptionMetadataWorks) {
    BSONObj projection = BSON("$addFields" << BSON("person"
                                                   << "$ssn"));
    auto& schema = getSchemaForStage({projection}, kDefaultSsnSchema);
    ASSERT(schema.getEncryptionMetadataForPath(FieldRef("person")) == kDefaultMetadata);
}

TEST_F(FLEPipelineTest, AddFieldsRenameEncryptedToDottedPathCreatesEncryptionSchemaStateMixedNode) {
    BSONObj projection = BSON("$addFields" << BSON("person.subObj"
                                                   << "$ssn"));
    auto& schema = getSchemaForStage({projection}, kDefaultSsnSchema);
    ASSERT_THROWS_CODE(
        schema.getEncryptionMetadataForPath(FieldRef("person")), AssertionException, 31133);
}

TEST_F(FLEPipelineTest,
       AddFieldsComputedEncryptedToDottedPathCreatesEncryptionSchemaStateMixedNode) {
    BSONObj projection = BSON("$addFields" << BSON("person.subObj"
                                                   << "$user.ssn"));
    auto& schema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_THROWS_CODE(
        schema.getEncryptionMetadataForPath(FieldRef("person")), AssertionException, 31133);
}

TEST_F(FLEPipelineTest, AddFieldsFromPrefixOfEncryptedField) {
    BSONObj projection = BSON("$addFields" << BSON("newName"
                                                   << "$user"));
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kMultiEncryptSchema, EncryptionSchemaType::kLocal);
    const auto& outputSchema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT(*outputSchema.getNode(FieldRef("newName")) == *inputSchema->getNode(FieldRef("user")));
}

TEST_F(FLEPipelineTest, AddFieldsFromPatternPropertiesKeepsEncryptionMetadata) {
    BSONObj projection = BSON("$addFields" << BSON("newName"
                                                   << "$foo"));
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kPatternPropertiesSchema, EncryptionSchemaType::kLocal);
    const auto& outputSchema = getSchemaForStage({projection}, kPatternPropertiesSchema);
    ASSERT(*outputSchema.getNode(FieldRef("newName")) == *inputSchema->getNode(FieldRef("foo")));
    ASSERT(outputSchema.getNode(FieldRef("newName"))->getEncryptionMetadata());
}

TEST_F(FLEPipelineTest, AddFieldsFromAdditionalPropertiesKeepsEncryptionMetadata) {
    BSONObj projection = BSON("$addFields" << BSON("newName"
                                                   << "$foo"));
    const auto inputSchema =
        EncryptionSchemaTreeNode::parse(kAllEncryptedSchema, EncryptionSchemaType::kLocal);
    const auto& outputSchema = getSchemaForStage({projection}, kAllEncryptedSchema);
    ASSERT(*outputSchema.getNode(FieldRef("newName")) == *inputSchema->getNode(FieldRef("foo")));
    ASSERT(outputSchema.getNode(FieldRef("newName"))->getEncryptionMetadata());
}

TEST_F(FLEPipelineTest, AddFieldsOverwritesPrefixOfEncryptedField) {
    BSONObj projection = BSON("$addFields" << BSON("user"
                                                   << "randomString"
                                                   << "person"
                                                   << "secondString"));
    const auto& outputSchema = getSchemaForStage({projection}, kMultiEncryptSchema);
    ASSERT_FALSE(outputSchema.mayContainEncryptedNode());
}

TEST_F(FLEPipelineTest, AddFieldsCannotExtendAnEncryptedField) {
    BSONObj projection = BSON("$addFields" << BSON("ssn.notAllowed"
                                                   << "randomString"));
    ASSERT_THROWS_CODE(
        getSchemaForStage({projection}, kDefaultSsnSchema), AssertionException, 51096);
}

TEST_F(FLEPipelineTest, MarkingPlaceholdersSetsFlagToTrue) {
    BSONObj project = fromjson(R"(
        {
            "$addFields": {"newfield": {"$eq": ["$ssn", "abc123"]}}
        }
    )");

    ASSERT_TRUE(doesHavePlaceholders({project}, kDefaultSsnSchema));
}

TEST_F(FLEPipelineTest, NoEncryptedFieldSetsFlagToFalse) {
    BSONObj project = fromjson(R"(
        {
            "$addFields": {"newfield": {"$eq": ["$foo", "abc123"]}}
        }
    )");

    ASSERT_FALSE(doesHavePlaceholders({project}, kDefaultSsnSchema));
}

TEST_F(FLEPipelineTest, ReplaceRootReferringToNewFields) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot"
                                                      << "$randomField"));
    const auto& outputSchema = getSchemaForStage({replaceRoot}, kDefaultNestedSchema);
    ASSERT_FALSE(outputSchema.mayContainEncryptedNode());
}

// TODO SERVER-41337: Support expressions which reference prefixes of encrypted fields.
TEST_F(FLEPipelineTest, ReplaceRootReferringToEncryptedSubFieldFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot"
                                                      << "$user"));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kDefaultNestedSchema), AssertionException, 31129);
}

TEST_F(FLEPipelineTest, ReplaceRootReferringToEncryptedFieldFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot"
                                                      << "$user.ssn"));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kDefaultNestedSchema), AssertionException, 31159);
}

TEST_F(FLEPipelineTest, ReplaceRootReferringToEncryptedFieldMatchingPatternPropertiesFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot"
                                                      << "$matchingFieldfoo"));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kPatternPropertiesSchema), AssertionException, 31159);
}

TEST_F(FLEPipelineTest, ReplaceRootReferringToNonEncryptedFieldWithPatternProperties) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot"
                                                      << "$nonMatchingField"));
    const auto& outputSchema = getSchemaForStage({replaceRoot}, kPatternPropertiesSchema);
    ASSERT_FALSE(outputSchema.mayContainEncryptedNode());
}

TEST_F(FLEPipelineTest, ReplaceRootReferringToEncryptedFieldAdditionalPropertiesFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot"
                                                      << "$additionalField"));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kAllEncryptedSchema), AssertionException, 31159);
}

TEST_F(FLEPipelineTest, ReplaceRootWithCustomObjectReferringToLiteral) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot" << BSON("ssn"
                                                                        << "value")));
    const auto& outputSchema = getSchemaForStage({replaceRoot}, kDefaultNestedSchema);
    ASSERT_FALSE(outputSchema.mayContainEncryptedNode());
}

TEST_F(FLEPipelineTest, ReplaceRootWithCustomObjectReferringToEncryptFieldFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot" << BSON("newField"
                                                                        << "$user.ssn")));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kDefaultNestedSchema), AssertionException, 31110);
}

TEST_F(FLEPipelineTest, ReplaceRootWithCustomObjectReferringToEncryptAdditionalFieldFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot" << BSON("newField"
                                                                        << "$additionalField")));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kAllEncryptedSchema), AssertionException, 31110);
}

// TODO SERVER-41337: Support expressions which reference prefixes of encrypted fields.
TEST_F(FLEPipelineTest, ReplaceRootWithCustomObjectReferringToEncryptedSubFieldFails) {
    BSONObj replaceRoot = BSON("$replaceRoot" << BSON("newRoot" << BSON("newField"
                                                                        << "$user")));
    ASSERT_THROWS_CODE(
        getSchemaForStage({replaceRoot}, kDefaultNestedSchema), AssertionException, 31129);
}

TEST_F(FLEPipelineTest, ScoreFailsUnsupportedCommand) {
    RAIIServerParameterControllerForTest controller("featureFlagSearchHybridScoring", true);

    BSONObj scoreSpec = BSON("$score" << BSON("score"
                                              << "$myScore"));
    ASSERT_THROWS_CODE(getSchemaForStage({scoreSpec}, kDefaultSsnSchema),
                       AssertionException,
                       ErrorCodes::CommandNotSupported);
}

}  // namespace
}  // namespace mongo
