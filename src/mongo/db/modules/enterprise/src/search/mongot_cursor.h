/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/process_interface/stub_mongo_process_interface.h"
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
 * Consult mongot to get planning information.
 * This function populates the 'params' out-param pointer.
 */
void planShardedSearch(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                       const BSONObj& searchRequest,
                       DocumentSourceInternalSearchMongotRemote::Params* params);

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
    if ((typeid(*expCtx->mongoProcessInterface) == typeid(StubMongoProcessInterface) ||
         !expCtx->mongoProcessInterface->inShardedEnvironment(expCtx->opCtx)) ||
        MONGO_unlikely(DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup())) {
        return {make_intrusive<TargetSearchDocumentSource>(std::move(params), expCtx, executor)};
    }
    planShardedSearch(expCtx, specObj, &params);
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
};

}  // namespace mongo::mongot_cursor
