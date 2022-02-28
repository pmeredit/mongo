/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongot_cursor.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source_documents.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/search_helper.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/query/document_source_merge_cursors.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/net/hostandport.h"
#include "mongot_options.h"
#include "mongot_task_executor.h"

namespace mongo::mongot_cursor {

namespace {
BSONObj commandObject(const boost::intrusive_ptr<ExpressionContext>& expCtx, const BSONObj& query) {
    BSONObjBuilder builder;
    builder.append("search", expCtx->ns.coll());
    expCtx->uuid.get().appendToBuilder(&builder, "collectionUUID");
    builder.append("query", query);
    if (expCtx->explain) {
        builder.append("explain",
                       BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
    }
    return builder.obj();
}

executor::RemoteCommandRequest getRemoteCommandRequest(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const BSONObj& query) {
    uassert(31082,
            str::stream() << "$search not enabled! "
                          << "Enable Search by setting serverParameter mongotHost to a valid "
                          << "\"host:port\" string",
            globalMongotParams.enabled);
    auto swHostAndPort = HostAndPort::parse(globalMongotParams.host);
    // This host and port string is configured and validated at startup.
    invariant(swHostAndPort.getStatus().isOK());
    executor::RemoteCommandRequest rcr(executor::RemoteCommandRequest(swHostAndPort.getValue(),
                                                                      expCtx->ns.db().toString(),
                                                                      commandObject(expCtx, query),
                                                                      expCtx->opCtx));
    rcr.sslMode = transport::ConnectSSLMode::kDisableSSL;
    return rcr;
}

}  // namespace

BSONObj getExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                           const BSONObj& query,
                           executor::TaskExecutor* taskExecutor) {
    auto request = getRemoteCommandRequest(expCtx, query);
    auto [promise, future] = makePromiseFuture<executor::TaskExecutor::RemoteCommandCallbackArgs>();
    auto promisePtr = std::make_shared<Promise<executor::TaskExecutor::RemoteCommandCallbackArgs>>(
        std::move(promise));
    auto scheduleResult = taskExecutor->scheduleRemoteCommand(
        std::move(request), [promisePtr](const auto& args) { promisePtr->emplaceValue(args); });
    if (!scheduleResult.isOK()) {
        // Since the command failed to be scheduled, the callback above did not and will not run.
        // Thus, it is safe to fulfill the promise here without worrying about synchronizing access
        // with the executor's thread.
        promisePtr->setError(scheduleResult.getStatus());
    }
    auto response = future.getNoThrow(expCtx->opCtx);
    uassertStatusOK(response.getStatus());
    uassertStatusOK(response.getValue().response.status);
    BSONObj responseData = response.getValue().response.data;
    uassertStatusOK(getStatusFromCommandResult(responseData));
    auto explain = responseData["explain"];
    uassert(4895000,
            "Response must contain an 'explain' field that is of type 'Object'",
            explain.type() == BSONType::Object);
    return explain.embeddedObject().getOwned();
}
std::vector<executor::TaskExecutorCursor> establishCursors(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    executor::TaskExecutor* taskExecutor) {
    // UUID is required for mongot queries. If not present, no results for the query as the
    // collection has not been created yet.
    if (!expCtx->uuid) {
        return {};
    }
    // If this is an explain query, getExplainResponse() should have been called instead. No cursor
    // is necessary for explain.
    tassert(6253300,
            "Expected to have query, not explain, to establish mongot cursors",
            !expCtx->explain);

    std::vector<executor::TaskExecutorCursor> cursors;
    cursors.emplace_back(taskExecutor, getRemoteCommandRequest(expCtx, query));
    auto additionalCursors = cursors[0].releaseAdditionalCursors();
    // Preserve cursor order. Expect cursors to be labeled, so this may not be necessary.
    for (auto& thisCursor : additionalCursors) {
        cursors.push_back(std::move(thisCursor));
    }

    return cursors;
}

namespace {
// The following stages are required to run search queries but do not support dep tracking. If we
// encounter one in a pipeline, ignore NOT_SUPPORTED.
std::set<StringData> skipDepTrackingStages{DocumentSourceInternalSearchMongotRemote::kStageName,
                                           DocumentSourceInternalSearchIdLookUp::kStageName,
                                           DocumentSourceDocuments::kStageName,
                                           DocumentSourceMergeCursors::kStageName};
std::pair<bool, bool> assertSearchMetaAccessValidHelper(const Pipeline::SourceContainer& pipeline) {
    // Whether there is a $$SEARCH_META in the pipeline.
    bool searchMetaAccessed = false;
    // Whether or not $$SEARCH_META is set in various locations.
    bool searchMetaSet = false;
    bool searchMetaSetInSubPipeline = false;

    // If we see a stage that doesn't support dependency tracking, there's no way to tell if we'd
    // be doing something incorrectly.
    bool depTrackingSupported = true;

    for (const auto& source : pipeline) {
        DepsTracker dep;
        // Check if this is a stage that sets $$SEARCH_META.
        auto stageName = StringData(source->getSourceName());
        if (stageName == DocumentSourceInternalSearchMongotRemote::kStageName) {
            searchMetaSet = true;
        }

        // If this stage has a sub-pipeline, check those stages as well.
        auto subPipeline = source->getSubPipeline();
        if (subPipeline && subPipeline->size() != 0) {
            auto [subMetaSet, subMetaAccessed] = assertSearchMetaAccessValidHelper(*subPipeline);
            searchMetaAccessed = searchMetaAccessed || subMetaAccessed;
            if (subMetaSet) {
                searchMetaSetInSubPipeline = true;
                searchMetaSet = true;
            }
        }

        // Check if this stage references $$SEARCH_META. If a stage does not support dep tracking,
        // fail as soon as we hit something search related.
        if (DepsTracker::State::NOT_SUPPORTED == source->getDependencies(&dep) &&
            skipDepTrackingStages.find(stageName) == skipDepTrackingStages.end()) {
            depTrackingSupported = false;
        } else if (dep.hasVariableReferenceTo({Variables::kSearchMetaId})) {
            searchMetaAccessed = true;
        }

        uassert(6080011,
                "Could not determine whether $$SEARCH_META would be accessed in a not-allowed "
                "context",
                depTrackingSupported || (!searchMetaAccessed && !searchMetaSet));

        uassert(6080010,
                "$$SEARCH_META is not available if there is a search stage in a sub-pipeline in "
                "the same query",
                !searchMetaAccessed || !searchMetaSetInSubPipeline);
    }

    return {searchMetaSet, searchMetaAccessed};
}

void injectSearchShardFilteredIfNeeded(Pipeline* pipeline) {
    auto& sources = pipeline->getSources();
    auto internalSearchLookupIt = sources.begin();
    // Bail early if the pipeline is not $search stage
    if (internalSearchLookupIt == sources.end() ||
        mongo::DocumentSourceInternalSearchMongotRemote::kStageName !=
            (*internalSearchLookupIt)->getSourceName()) {
        return;
    }

    while (internalSearchLookupIt != sources.end()) {
        if (DocumentSourceInternalSearchIdLookUp::kStageName ==
            (*internalSearchLookupIt)->getSourceName()) {
            break;
        }
        internalSearchLookupIt++;
    }

    if (internalSearchLookupIt != sources.end()) {
        auto expCtx = pipeline->getContext();
        if (auto shardFilterer = expCtx->mongoProcessInterface->getShardFilterer(expCtx)) {
            auto doc = new DocumentSourceInternalShardFilter(expCtx, std::move(shardFilterer));
            internalSearchLookupIt++;
            sources.insert(internalSearchLookupIt, doc);
            Pipeline::stitch(&sources);
        }
    }
}

ServiceContext::ConstructorActionRegisterer searchQueryImplementation{
    "searchQueryImplementation", {"searchQueryHelperRegisterer"}, [](ServiceContext* context) {
        invariant(context);
        getSearchHelpers(context) = std::make_unique<SearchImplementedHelperFunctions>();
    }};
}  // namespace

void SearchImplementedHelperFunctions::assertSearchMetaAccessValid(
    const Pipeline::SourceContainer& pipeline) {
    assertSearchMetaAccessValidHelper(pipeline);
}

void mongo::mongot_cursor::SearchImplementedHelperFunctions::injectSearchShardFiltererIfNeeded(
    Pipeline* pipeline) {
    injectSearchShardFilteredIfNeeded(pipeline);
}

}  // namespace mongo::mongot_cursor
