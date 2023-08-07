/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "search/mongot_cursor.h"
#include "vector_search/document_source_vector_search_gen.h"

namespace mongo::mongot_cursor {

static constexpr StringData kVectorSearchCmd = "vectorSearch"_sd;

/**
 * Run the given vector search request against mongot and build a cursor object for the cursor
 * returned from mongot.
 */
executor::TaskExecutorCursor establishVectorSearchCursor(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const VectorSearchSpec& request,
    std::shared_ptr<executor::TaskExecutor> taskExecutor);

/**
 * Wrapper function to run getExplainResponse with vectorSearch command.
 */
BSONObj getVectorSearchExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                       const VectorSearchSpec& request,
                                       executor::TaskExecutor* taskExecutor);

}  // namespace mongo::mongot_cursor
