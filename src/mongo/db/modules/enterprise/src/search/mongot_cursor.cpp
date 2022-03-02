/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongot_cursor.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/document_source_documents.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/search_helper.h"
#include "mongo/db/s/collection_sharding_state.h"
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
const std::string kMetadataCursorType = "meta";
const std::string kResultCursorType = "results";

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
/**
 * Creates an additional pipeline to be run during a query if the query needs to generate metadata.
 * Does not take ownership of the passed in pipeline, and returns a pipeline containing only a
 * metadata generating $search stage. Can return null if no metadata pipeline is required.
 */
std::unique_ptr<Pipeline, PipelineDeleter>
SearchImplementedHelperFunctions::generateMetadataPipelineForSearch(
    OperationContext* opCtx,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    const AggregateCommandRequest& request,
    Pipeline* origPipeline,
    boost::optional<UUID> uuid) {
    if (expCtx->explain) {
        // $search doesn't return documents or metadata from explain regardless of the verbosity.
        return nullptr;
    }
    // We only want to return multiple cursors if this request originated from mongoS, and not from
    // a user. If the collection isn't sharded there won't be any need to merge the metadata.
    auto needMetadataPipeline = request.getFromMongos() && expCtx->needsMerge &&
        ::mongo::feature_flags::gFeatureFlagSearchShardedFacets.isEnabledAndIgnoreFCV();

    // If there's no pipeline, we have no work to do.
    if (!origPipeline || origPipeline->getSources().size() == 0) {
        return nullptr;
    }

    // Set up all the necessary checks for later. If any of the following checks are false we
    // won't need a metadata pipeline but we still need to setup the original search stage (if
    // present). $search is required to be the first stage of the pipeline.
    auto potentialSearchStage = origPipeline->getSources().front();
    if (potentialSearchStage->getSourceName() !=
        DocumentSourceInternalSearchMongotRemote::kStageName) {
        return nullptr;
    }

    uassert(
        6253506, "Cannot have exchange specified in a $search pipeline", !request.getExchange());


    auto origSearchStage =
        dynamic_cast<DocumentSourceInternalSearchMongotRemote*>(potentialSearchStage.get());

    // Some tests build $search pipelines without actually setting up a mongot. In this case either
    // return a dummy stage or nothing depending on the environment. Note that in this case we don't
    // actually make any queries, the document source will return eof immediately.
    if (MONGO_unlikely(DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup())) {
        if (needMetadataPipeline) {
            boost::intrusive_ptr<DocumentSource> newStage = origSearchStage->clone();
            return Pipeline::create({newStage}, expCtx);
        }
        return nullptr;
    }

    // The search stage has not yet established its cursor on mongoT. Establish the cursor for it.
    auto cursors = mongot_cursor::establishCursors(
        expCtx, origSearchStage->getSearchQuery(), origSearchStage->getTaskExecutor());

    // mongot can return zero cursors for an empty collection, one without metadata, or two for
    // results and metadata.
    tassert(6253500, "Expected less than or exactly two cursors from mongot", cursors.size() <= 2);

    if (cursors.size() == 0) {
        origSearchStage->markCollectionEmpty();
    }

    std::unique_ptr<Pipeline, PipelineDeleter> newPipeline = nullptr;
    for (auto it = cursors.begin(); it != cursors.end(); it++) {
        auto cursorLabel = it->getType();
        if (!cursorLabel) {
            // If a cursor is unlabeled mongot does not support metadata cursors. $$SEARCH_META
            // should not be supported in this query.
            tassert(6253301,
                    "Expected cursors to be labeled if there are more than one",
                    cursors.size() == 1);
            origSearchStage->setCursor(std::move(cursors.front()));
            return nullptr;
        }
        auto cursorType = cursorLabel.get();
        if (cursorType == kResultCursorType) {
            origSearchStage->setCursor(std::move(*it));
            origPipeline->pipelineType = CursorTypeEnum::DocumentResult;
        } else if (cursorType == kMetadataCursorType) {
            // If this request is not from mongos, is not sharded, or the feature flag is not
            // enabled mongos should not have added the "intermediate" flag to the mongot query, and
            // we therefore should not have seen a metadata cursor.
            // Ignore FCV for the feature flag as this feature is available on earlier versions.
            tassert(6253303, "Didn't expect metadata cursor from mongot", needMetadataPipeline);
            // Clone the MongotRemote stage and set the metadata cursor.
            auto newStage = origSearchStage->copyForAlternateSource(std::move(*it));

            // Build a new pipeline with the metadata source as the only stage.
            newPipeline = Pipeline::create({newStage}, expCtx);
            newPipeline->pipelineType = CursorTypeEnum::SearchMetaResult;
        } else {
            tasserted(6253302, str::stream() << "Unexpected cursor type '" << cursorType << "'");
        }
    }

    // Can return null if we did not build a metadata pipeline.
    return newPipeline;
}

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

    std::vector<executor::TaskExecutorCursor> cursors;
    cursors.emplace_back(taskExecutor, getRemoteCommandRequest(expCtx, query));
    // Wait for the cursors to actually be populated. Wait on the callback handle of the original
    // cursor.
    if (auto callback = cursors[0].getCallbackHandle()) {
        taskExecutor->wait(callback.get(), expCtx->opCtx);
        cursors[0].populateCursor(expCtx->opCtx);
    }
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
