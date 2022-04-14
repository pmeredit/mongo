/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/search_helper.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongot_task_executor.h"

namespace mongo::mongot_cursor {

/**
 * Run the given search query against mongot and build one cursor object for each
 * cursor returned from mongot.
 */
std::vector<executor::TaskExecutorCursor> establishCursors(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    executor::TaskExecutor* taskExecutor,
    const boost::optional<int>& protocolVersion = boost::none);

/**
 * Gets the explain information by issuing an explain command to mongot and blocking
 * until the response is retrieved. The 'query' argument is the original search query
 * that we are trying to explain, not a full explain command. Throws an exception on failure.
 */
BSONObj getExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                           const BSONObj& query,
                           executor::TaskExecutor* taskExecutor);

/**
 * Fetch the search metadata merging pipeline from mongot. The return value is a pair including the
 * parsed pipeline along with the protocol version which must be included in the search query.
 */
std::pair<std::unique_ptr<Pipeline, PipelineDeleter>, int> fetchMergingPipeline(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx, const BSONObj& searchRequest);

/**
 * Create the initial search pipeline which can be used for both $search and $searchMeta. The
 * returned list is unique and mutable.
 */
template <typename TargetSearchDocumentSource>
std::list<boost::intrusive_ptr<DocumentSource>> createInitialSearchPipeline(
    BSONObj specObj, const boost::intrusive_ptr<ExpressionContext>& expCtx) {

    uassert(6600901,
            "Running search command in non-allowed context (update pipeline)",
            !expCtx->isParsingPipelineUpdate);
    auto params = DocumentSourceInternalSearchMongotRemote::parseParamsFromBson(specObj, expCtx);
    auto executor = executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext());
    if (!expCtx->mongoProcessInterface->inShardedEnvironment(expCtx->opCtx) ||
        MONGO_unlikely(DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup()) ||
        !feature_flags::gFeatureFlagSearchShardedFacets.isEnabled(
            serverGlobalParams.featureCompatibility)) {
        return {make_intrusive<TargetSearchDocumentSource>(std::move(params), expCtx, executor)};
    }

    // We need a $setVariableFromSubPipeline if we support faceted search on sharded cluster. If the
    // collection is not sharded, the search will have previously been sent to the primary shard and
    // we don't need to merge the metadata.
    // We don't actually know if the collection is sharded at this point, so assume it is in order
    // to generate the correct pipeline. The extra stage will be removed later if necessary.
    auto [mergingPipeline, protocolVersion] = fetchMergingPipeline(expCtx, specObj);
    params.mergePipeline = std::move(mergingPipeline);
    params.protocolVersion = protocolVersion;

    return {make_intrusive<TargetSearchDocumentSource>(std::move(params), expCtx, executor)};
}

/**
 * A class that contains methods that are implemented as stubs in community that need to be
 * overridden.
 */
class SearchImplementedHelperFunctions : public SearchDefaultHelperFunctions {
public:
    void assertSearchMetaAccessValid(const Pipeline::SourceContainer& pipeline,
                                     ExpressionContext* expCtx) override final;
    void injectSearchShardFiltererIfNeeded(Pipeline* pipeline) override final;
    std::unique_ptr<Pipeline, PipelineDeleter> generateMetadataPipelineForSearch(
        OperationContext* opCtx,
        boost::intrusive_ptr<ExpressionContext> expCtx,
        const AggregateCommandRequest& request,
        Pipeline* origPipeline,
        boost::optional<UUID> uuid) override final;
    boost::optional<std::string> validatePipelineForShardedCollection(
        const Pipeline& pipeline) override final;
};

}  // namespace mongo::mongot_cursor
