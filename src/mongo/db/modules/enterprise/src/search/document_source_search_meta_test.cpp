/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include <boost/intrusive_ptr.hpp>
#include <vector>

#include "document_source_internal_search_mongot_remote.h"
#include "document_source_search_meta.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_mock.h"
#include "mongo/db/pipeline/document_source_mock_collection.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_set_variable_from_subpipeline.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongot_options.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::list;
using std::vector;

using SearchMetaTest = AggregationContextFixture;

struct MockMongoInterface final : public StubMongoProcessInterface {
    bool inShardedEnvironment(OperationContext* opCtx) const override {
        return false;
    }

    std::unique_ptr<Pipeline, PipelineDeleter> attachCursorSourceToPipeline(
        Pipeline* pipeline,
        ShardTargetingPolicy shardTargetingPolicy = ShardTargetingPolicy::kAllowed,
        boost::optional<BSONObj> readConcern = boost::none) override {
        // Return the pipeline unmodified. In this test, we expect it to start with a $mongotRemote
        // stage.
        ASSERT_GT(pipeline->getSources().size(), 0);
        ASSERT(dynamic_cast<DocumentSourceInternalSearchMongotRemote*>(
            pipeline->getSources().begin()->get()));
        return std::unique_ptr<Pipeline, PipelineDeleter>(
            pipeline, PipelineDeleter{pipeline->getContext()->opCtx});
    }
};

TEST_F(SearchMetaTest, TestParsingOfSearchMeta) {
    const auto mongotQuery = fromjson("{query: 'cakes', path: 'title'}");
    auto specObj = BSON("$searchMeta" << mongotQuery);

    auto expCtx = getExpCtx();
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>();
    auto fromNs = NamespaceString("unittests.$cmd.aggregate");
    expCtx->setResolvedNamespaces(StringMap<ExpressionContext::ResolvedNamespace>{
        {fromNs.coll().toString(), {fromNs, std::vector<BSONObj>()}}});
    list<intrusive_ptr<DocumentSource>> results =
        DocumentSourceSearchMeta::createFromBson(specObj.firstElement(), expCtx);

    ASSERT_EQUALS(results.size(), 3UL);
    auto it = results.begin();
    const auto* mockCollection = dynamic_cast<DocumentSourceMockCollection*>(it->get());
    ASSERT(mockCollection);

    std::advance(it, 1);
    const auto* setVarStage = dynamic_cast<DocumentSourceSetVariableFromSubPipeline*>(it->get());
    ASSERT(setVarStage);

    {
        auto subPipe = setVarStage->getSubPipeline();
        ASSERT(subPipe);
        ASSERT_EQUALS(subPipe->size(), 3UL);
        auto subPipeIt = subPipe->begin();
        const auto* mongotRemote =
            dynamic_cast<DocumentSourceInternalSearchMongotRemote*>(subPipeIt->get());
        ASSERT(mongotRemote);

        std::advance(subPipeIt, 1);
        // DocumentSourceReplaceRoot desugars to singleDocTransStage in createFromBson().
        const auto* replaceRoot =
            dynamic_cast<DocumentSourceSingleDocumentTransformation*>(subPipeIt->get());
        ASSERT(replaceRoot);

        std::advance(subPipeIt, 1);
        const auto* limit = dynamic_cast<DocumentSourceLimit*>(subPipeIt->get());
        ASSERT(limit);
    }

    std::advance(it, 1);
    // DocumentSourceReplaceRoot desugars to singleDocTransStage in createFromBson().
    const auto* singleDocTransStage =
        dynamic_cast<DocumentSourceSingleDocumentTransformation*>(it->get());
    ASSERT(singleDocTransStage);

    // $searchMeta argument must be an object.
    specObj = BSON("$searchMeta" << 1000);
    ASSERT_THROWS_CODE(
        DocumentSourceSearchMeta::createFromBson(specObj.firstElement(), getExpCtx()),
        AssertionException,
        ErrorCodes::FailedToParse);
}

}  // namespace
}  // namespace mongo
