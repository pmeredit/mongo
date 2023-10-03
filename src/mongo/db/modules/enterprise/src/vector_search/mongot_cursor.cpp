/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongot_cursor.h"

namespace mongo::mongot_cursor {

namespace {

executor::RemoteCommandRequest getRemoteCommandRequestForVectorSearchQuery(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const VectorSearchSpec& request) {
    BSONObjBuilder cmdBob;
    cmdBob.append(kVectorSearchCmd, expCtx->ns.coll());
    uassert(7828001,
            str::stream()
                << "A uuid is required for a vector search query, but was missing. Got namespace "
                << expCtx->ns.toStringForErrorMsg(),
            expCtx->uuid);
    expCtx->uuid.value().appendToBuilder(&cmdBob, kCollectionUuidField);

    cmdBob.append(VectorSearchSpec::kQueryVectorFieldName, request.getQueryVector());
    cmdBob.append(VectorSearchSpec::kPathFieldName, request.getPath());
    cmdBob.append(VectorSearchSpec::kLimitFieldName, request.getLimit().coerceToLong());

    if (request.getIndex()) {
        cmdBob.append(VectorSearchSpec::kIndexFieldName, *request.getIndex());
    }

    if (request.getNumCandidates()) {
        cmdBob.append(VectorSearchSpec::kNumCandidatesFieldName,
                      request.getNumCandidates()->coerceToLong());
    }

    if (request.getFilter()) {
        cmdBob.append(VectorSearchSpec::kFilterFieldName, *request.getFilter());
    }
    if (expCtx->explain) {
        cmdBob.append("explain",
                      BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
    }

    return getRemoteCommandRequest(expCtx->opCtx, expCtx->ns, cmdBob.obj());
}

}  // namespace

executor::TaskExecutorCursor establishVectorSearchCursor(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const VectorSearchSpec& request,
    std::shared_ptr<executor::TaskExecutor> taskExecutor) {
    // Note that we always pre-fetch the next batch here. This is because we generally expect
    // everything to fit into one batch, since we give mongot the exact upper bound initially - we
    // will only see multiple batches if this upper bound doesn't fit in 16MB. This should be a rare
    // enough case that it shouldn't overwhelm mongot to pre-fetch.
    auto cursors = establishCursors(expCtx,
                                    getRemoteCommandRequestForVectorSearchQuery(expCtx, request),
                                    taskExecutor,
                                    true /* preFetchNextBatch */);
    // Should always have one results cursor.
    tassert(7828000, "Expected exactly one cursor from mongot", cursors.size() == 1);
    return std::move(cursors.front());
}

BSONObj getVectorSearchExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                       const VectorSearchSpec& spec,
                                       executor::TaskExecutor* taskExecutor) {
    auto request = getRemoteCommandRequestForVectorSearchQuery(expCtx, spec);
    return mongot_cursor::getExplainResponse(expCtx.get(), request, taskExecutor);
}

}  // namespace mongo::mongot_cursor
