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
    const EncryptionSchemaTreeNode& getSchemaForStage(const std::vector<BSONObj>& pipeline,
                                                      BSONObj inputSchema) {
        auto parsedPipeline = uassertStatusOK(Pipeline::parse(pipeline, getExpCtx()));
        _flePipe = std::make_unique<FLEPipeline>(std::move(parsedPipeline),
                                                 EncryptionSchemaTreeNode::parse(inputSchema));
        return _flePipe->getOutputSchema();
    }

private:
    std::unique_ptr<FLEPipeline> _flePipe;
};

TEST_F(FLEPipelineTest, LimitStageTreatedAsNoop) {
    const auto& outputSchema = getSchemaForStage({fromjson("{$limit: 1}")}, kDefaultSsnSchema);
    ASSERT_TRUE(outputSchema.getEncryptionMetadataForPath(FieldRef("ssn")));
    ASSERT_FALSE(outputSchema.getEncryptionMetadataForPath(FieldRef("notSsn")));
}

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
        fromjson("{$match: {}}"),
        fromjson("{$geoNear: {near: [0.0, 0.0], distanceField: 'dist'}}"),
        fromjson("{$sample: {size: 1}}"),
        fromjson("{$_internalInhibitOptimization: {}}"),
        fromjson("{$skip: 1}"),
        fromjson("{$sort: {ssn: 1}}"),
        fromjson("{$indexStats: {}}"),
        fromjson("{$collStats: {}}"),
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

}  // namespace
}  // namespace mongo
