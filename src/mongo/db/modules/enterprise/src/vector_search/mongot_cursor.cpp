/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongot_cursor.h"

namespace mongo::mongot_cursor {

namespace {

executor::RemoteCommandRequest getRemoteCommandRequestForKnnQuery(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const VectorSearchSpec& request) {
    BSONObjBuilder cmdBob;
    cmdBob.append(kKnnCmd, expCtx->ns.coll());
    uassert(7828001,
            str::stream() << "A uuid is required for a knn query, but was missing. Got namespace "
                          << expCtx->ns.toStringForErrorMsg(),
            expCtx->uuid);
    expCtx->uuid.value().appendToBuilder(&cmdBob, kCollectionUuidField);

    cmdBob.append(VectorSearchSpec::kQueryVectorFieldName, request.getQueryVector());
    cmdBob.append(VectorSearchSpec::kPathFieldName, request.getPath());
    cmdBob.append(VectorSearchSpec::kCandidatesFieldName, request.getCandidates().coerceToLong());
    cmdBob.append(VectorSearchSpec::kIndexFieldName, request.getIndex());

    if (request.getFilter()) {
        cmdBob.append(VectorSearchSpec::kFilterFieldName, *request.getFilter());
    }

    return getRemoteCommandRequest(expCtx, cmdBob.obj());
}

}  // namespace

executor::TaskExecutorCursor establishKnnCursor(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const VectorSearchSpec& request,
    std::shared_ptr<executor::TaskExecutor> taskExecutor) {
    auto cursors =
        establishCursors(expCtx, getRemoteCommandRequestForKnnQuery(expCtx, request), taskExecutor);
    // Should always have one results cursor.
    tassert(7828000, "Expected exactly one cursor from mongot", cursors.size() == 1);
    return std::move(cursors.front());
}

}  // namespace mongo::mongot_cursor
