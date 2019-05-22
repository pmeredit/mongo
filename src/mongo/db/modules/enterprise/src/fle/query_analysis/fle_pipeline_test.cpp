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

class FLEPipelineTest : public FLETestFixture {
public:
    /**
     * Given a pipeline and an input schema, returns the schema of documents flowing out of the last
     * stage of the pipeline.
     */
    const EncryptionSchemaTreeNode& getSchemaForStage(const std::vector<BSONObj>& pipeline,
                                                      BSONObj inputSchema) {
        auto parsedPipeline = uassertStatusOK(Pipeline::parse(pipeline, getExpCtx()));
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema);
        _flePipe = std::make_unique<FLEPipeline>(std::move(parsedPipeline), *schema.get());
        return _flePipe->getOutputSchema();
    }

    /**
     * Given a pipeline and an input schema, returns a new serialized pipeline with encrypted fields
     * replaced by their appropriate intent-to-encrypt markings.
     */
    std::vector<BSONObj> translatePipeline(const std::vector<BSONObj>& pipeline,
                                           BSONObj inputSchema) {
        auto schema = EncryptionSchemaTreeNode::parse(inputSchema);
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

TEST_F(FLEPipelineTest, EmptyPipelineCorrectlyPropagatesInputSchema) {
    const auto& outputSchema = getSchemaForStage({}, kDefaultSsnSchema);
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef("notSsn")));
}

}  // namespace
}  // namespace mongo
