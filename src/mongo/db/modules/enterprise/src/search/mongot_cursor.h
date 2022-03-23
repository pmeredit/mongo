/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/search_helper.h"
#include "mongo/executor/task_executor_cursor.h"

namespace mongo::mongot_cursor {

/**
 * Run the given search query against mongot and build one cursor object for each
 * cursor returned from mongot.
 */
std::vector<executor::TaskExecutorCursor> establishCursors(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    executor::TaskExecutor* taskExecutor);

/**
 * Gets the explain information by issuing an explain command to mongot and blocking
 * until the response is retrieved. The 'query' argument is the original search query
 * that we are trying to explain, not a full explain command. Throws an exception on failure.
 */
BSONObj getExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                           const BSONObj& query,
                           executor::TaskExecutor* taskExecutor);

/**
 * A class that contains methods that are implemented as stubs in community that need to be
 * overridden.
 */
class SearchImplementedHelperFunctions : public SearchDefaultHelperFunctions {
public:
    void assertSearchMetaAccessValid(const Pipeline::SourceContainer& pipeline) override final;
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
