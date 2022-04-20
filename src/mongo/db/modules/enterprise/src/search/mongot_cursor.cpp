/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongot_cursor.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/document_source_documents.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_queue.h"
#include "mongo/db/pipeline/document_source_set_variable_from_subpipeline.h"
#include "mongo/db/pipeline/document_source_union_with.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/search_helper.h"
#include "mongo/db/pipeline/sharded_agg_helpers.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/query/document_source_merge_cursors.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/net/hostandport.h"
#include "mongot_options.h"
#include "mongot_task_executor.h"

namespace mongo::mongot_cursor {

namespace {
const std::string kMetadataCursorType = "meta";
const std::string kResultCursorType = "results";
// Used to allow test-only stages in a pipeline even if they don't support dependency tracking.
MONGO_FAIL_POINT_DEFINE(assumeMetaContextOK);

/**
 * Create the RemoteCommandRequest for the provided command.
 */
executor::RemoteCommandRequest getRemoteCommandRequest(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const BSONObj& cmdObj) {
    uassert(31082,
            str::stream() << "$search not enabled! "
                          << "Enable Search by setting serverParameter mongotHost to a valid "
                          << "\"host:port\" string",
            globalMongotParams.enabled);
    auto swHostAndPort = HostAndPort::parse(globalMongotParams.host);
    // This host and port string is configured and validated at startup.
    invariant(swHostAndPort.getStatus().isOK());
    executor::RemoteCommandRequest rcr(executor::RemoteCommandRequest(
        swHostAndPort.getValue(), expCtx->ns.db().toString(), cmdObj, expCtx->opCtx));
    rcr.sslMode = transport::ConnectSSLMode::kDisableSSL;
    return rcr;
}

executor::RemoteCommandRequest getRemoteCommandRequestForQuery(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    const boost::optional<int> protocolVersion = boost::none) {
    BSONObjBuilder cmdBob;
    cmdBob.append("search", expCtx->ns.coll());
    uassert(
        6584801,
        str::stream() << "A uuid is required for a search query, but was missing. Got namespace "
                      << expCtx->ns.toString(),
        expCtx->uuid);
    expCtx->uuid.get().appendToBuilder(&cmdBob, "collectionUUID");
    cmdBob.append("query", query);
    if (expCtx->explain) {
        cmdBob.append("explain",
                      BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
    }
    if (protocolVersion) {
        cmdBob.append("intermediate", *protocolVersion);
    }
    return getRemoteCommandRequest(expCtx, cmdBob.obj());
}

bool isSearchPipeline(const Pipeline* pipeline) {
    if (!pipeline || pipeline->getSources().empty()) {
        return false;
    }
    auto firstStage = pipeline->peekFront();
    if (!firstStage)
        return false;
    return (StringData(firstStage->getSourceName()) ==
            DocumentSourceInternalSearchMongotRemote::kStageName);
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
    if (expCtx->explain || !isSearchPipeline(origPipeline)) {
        // $search doesn't return documents or metadata from explain regardless of the verbosity.
        return nullptr;
    }

    auto origSearchStage =
        dynamic_cast<DocumentSourceInternalSearchMongotRemote*>(origPipeline->peekFront());
    tassert(6253727, "Expected search stage", origSearchStage);

    // We only want to return multiple cursors if we are not in mongos and we plan on getting
    // unmerged metadata documents back from mongot.
    auto shouldBuildMetadataPipeline =
        !expCtx->inMongos && origSearchStage->getIntermediateResultsProtocolVersion();

    uassert(
        6253506, "Cannot have exchange specified in a $search pipeline", !request.getExchange());


    // Some tests build $search pipelines without actually setting up a mongot. In this case either
    // return a dummy stage or nothing depending on the environment. Note that in this case we don't
    // actually make any queries, the document source will return eof immediately.
    if (MONGO_unlikely(DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup())) {
        if (shouldBuildMetadataPipeline) {
            return Pipeline::create({origSearchStage->clone()}, expCtx);
        }
        return nullptr;
    }

    // The search stage has not yet established its cursor on mongoT. Establish the cursor for it.
    auto cursors =
        mongot_cursor::establishCursors(expCtx,
                                        origSearchStage->getSearchQuery(),
                                        origSearchStage->getTaskExecutor(),
                                        origSearchStage->getIntermediateResultsProtocolVersion());

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
            // If we don't think we're in a sharded environment, mongot should not have sent
            // metadata.
            tassert(
                6253303, "Didn't expect metadata cursor from mongot", shouldBuildMetadataPipeline);
            tassert(
                6253726, "Expected to not already have created a metadata pipeline", !newPipeline);
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
    auto request = getRemoteCommandRequestForQuery(expCtx, query);
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
    executor::TaskExecutor* taskExecutor,
    const boost::optional<int>& protocolVersion) {
    // UUID is required for mongot queries. If not present, no results for the query as the
    // collection has not been created yet.
    if (!expCtx->uuid) {
        return {};
    }

    std::vector<executor::TaskExecutorCursor> cursors;
    cursors.emplace_back(taskExecutor,
                         getRemoteCommandRequestForQuery(expCtx, query, protocolVersion));
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
                                           DocumentSourceMergeCursors::kStageName};
// Returns a pair of booleans. The first is whether or not '$$SEARCH_META' is set by 'pipeline', the
// second is whether '$$SEARCH_META' is accessed by 'pipeline'. It is assumed that if there is a
// 'DocumentSourceInternalSearchMongotRemote' then '$$SEARCH_META' will be set at some point in the
// pipeline. Depending on the configuration of the cluster
// 'DocumentSourceSetVariableFromSubPipeline' could do the actual setting of the variable, but it
// can only be generated alongside a 'DocumentSourceInternalSearchMongotRemote'.
void assertSearchMetaAccessValidHelper(const Pipeline::SourceContainer& pipeline) {
    // Whether there is a $$SEARCH_META in the pipeline.
    bool searchMetaAccessed = false;
    // Whether or not there was a sub-pipeline stage previously in this pipeline.
    bool subPipeSeen = false;
    bool searchMetaSet = false;

    // If we see a stage that doesn't support dependency tracking, there's no way to tell if we'd
    // be doing something incorrectly.
    bool depTrackingSupported = true;

    for (const auto& source : pipeline) {
        DepsTracker dep;
        // Check if this is a stage that sets $$SEARCH_META.
        static constexpr StringData kSetVarName =
            DocumentSourceSetVariableFromSubPipeline::kStageName;
        auto stageName = StringData(source->getSourceName());
        if (stageName == DocumentSourceInternalSearchMongotRemote::kStageName ||
            stageName == kSetVarName) {
            searchMetaSet = true;
            if (stageName == kSetVarName) {
                tassert(6448003,
                        str::stream()
                            << "Expecting all " << kSetVarName << " stages to be setting "
                            << Variables::getBuiltinVariableName(Variables::kSearchMetaId),
                        checked_cast<DocumentSourceSetVariableFromSubPipeline*>(source.get())
                                ->variableId() == Variables::kSearchMetaId);
                // $setVariableFromSubPipeline has a "sub pipeline", but it is the exception to the
                // scoping rule, since it is defining the $$SEARCH_META variable.
                continue;
            }
        }

        // If this stage has a sub-pipeline, $$SEARCH_META is not allowed after this stage.
        auto thisStageSubPipeline = source->getSubPipeline();
        if (thisStageSubPipeline) {
            subPipeSeen = true;
            if (!thisStageSubPipeline->empty()) {
                assertSearchMetaAccessValidHelper(*thisStageSubPipeline);
            }
        }

        // Check if this stage references $$SEARCH_META. If a stage does not support dep tracking,
        // fail as soon as we hit something search related.
        if (DepsTracker::State::NOT_SUPPORTED == source->getDependencies(&dep) &&
            skipDepTrackingStages.find(stageName) == skipDepTrackingStages.end()) {
            depTrackingSupported = false;
        } else if (dep.hasVariableReferenceTo({Variables::kSearchMetaId})) {
            uassert(6347901,
                    "Can't access $$SEARCH_META after a stage with a sub-pipeline",
                    !subPipeSeen || thisStageSubPipeline);
            uassert(6347902,
                    "Can't access $$SEARCH_META without a $search stage earlier in the pipeline",
                    searchMetaSet);
            searchMetaAccessed = true;
        }

        uassert(6080011,
                "Could not determine whether $$SEARCH_META would be accessed in a not-allowed "
                "context",
                depTrackingSupported || (!searchMetaAccessed && !searchMetaSet));
    }
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

std::pair<std::unique_ptr<Pipeline, PipelineDeleter>, int> fetchMergingPipeline(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const BSONObj& searchRequest) {
    // Retrieve merging pipeline from mongot.
    auto taskExecutor = executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext());
    auto cmdObj = [&]() {
        BSONObjBuilder cmdBob;
        // Request for faceted search merging pipeline uses "planShardedSearch" instead of "search".
        cmdBob.append("planShardedSearch", expCtx->ns.coll());
        cmdBob.append("query", searchRequest);
        if (expCtx->explain) {
            cmdBob.append("explain",
                          BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
        }
        return cmdBob.obj();
    }();

    executor::RemoteCommandResponse response =
        Status(ErrorCodes::InternalError, "Internal error running search command");
    executor::TaskExecutor::CallbackHandle cbHnd =
        uassertStatusOKWithContext(taskExecutor->scheduleRemoteCommand(
                                       getRemoteCommandRequest(expCtx, cmdObj),
                                       [&response](const auto& args) { response = args.response; }),
                                   str::stream() << "Failed to execute search command " << cmdObj);
    taskExecutor->wait(cbHnd, expCtx->opCtx);
    uassertStatusOKWithContext(response.status,
                               str::stream() << "Failed to execute search command " << cmdObj);

    uassertStatusOKWithContext(getStatusFromCommandResult(response.data),
                               "mongot returned an error");
    auto mergingPipeline = mongo::Pipeline::parseFromArray(response.data["metaPipeline"], expCtx);

    // We must also communicate the protocolVersion back to the caller.
    int protocolVersion = response.data["protocolVersion"_sd].Int();
    return {std::move(mergingPipeline), protocolVersion};
}

std::list<boost::intrusive_ptr<DocumentSource>> createInitialSearchPipeline(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$search/$searchMeta value must be an object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object);

    if (!expCtx->mongoProcessInterface->inShardedEnvironment(expCtx->opCtx) ||
        MONGO_unlikely(DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup()) ||
        !feature_flags::gFeatureFlagSearchShardedFacets.isEnabled(
            serverGlobalParams.featureCompatibility)) {
        return {DocumentSourceInternalSearchMongotRemote::createFromBson(elem, expCtx)};
    }

    // We need a $setVariableFromSubPipeline if we support faceted search on sharded cluster. If the
    // collection is not sharded, the search will have previously been sent to the primary shard and
    // we don't need to merge the metadata.
    // We don't actually know if the collection is sharded at this point, so assume it is in order
    // to generate the correct pipeline. The extra stage will be removed later if necessary.
    auto [mergingPipeline, protocolVersion] = fetchMergingPipeline(expCtx, elem.embeddedObject());

    return {make_intrusive<DocumentSourceInternalSearchMongotRemote>(
        elem.embeddedObject(),
        expCtx,
        executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext()),
        protocolVersion,
        std::move(mergingPipeline))};
}

void SearchImplementedHelperFunctions::assertSearchMetaAccessValid(
    const Pipeline::SourceContainer& pipeline, ExpressionContext* expCtx) {
    if (MONGO_unlikely(assumeMetaContextOK.shouldFail())) {
        return;
    }
    // If we already validated this pipeline on mongos, no need to do it again on a shard. Check
    // mergeCursors because we could be on a shard doing the merge.
    if ((expCtx->inMongos || !expCtx->needsMerge) &&
        pipeline.front()->getSourceName() != DocumentSourceMergeCursors::kStageName) {
        assertSearchMetaAccessValidHelper(pipeline);
    }
}

boost::optional<std::string> validatePipelineForShardedCollectionHelper(
    const Pipeline::SourceContainer& pipeline) {
    // We've already checked in 'assertSearchMetaAccessValid' for all failure conditions except
    // $$SEARCH_META access in a sharded collection. Only fail in that case.
    // Only do these checks for a $search pipeline, if $$SEARCH_META is accessed in a non-$search
    // pipeline we would have failed earlier.
    for (const auto& source : pipeline) {
        auto subPipeline = source->getSubPipeline();
        if (subPipeline && !subPipeline->empty()) {
            if (auto errMsg = validatePipelineForShardedCollectionHelper(*subPipeline)) {
                return errMsg;
            }
        }

        DepsTracker dep;
        if (DepsTracker::State::NOT_SUPPORTED == source->getDependencies(&dep)) {
            // We would have failed earlier if this particular stage was an issue.
            continue;
        } else if (dep.hasVariableReferenceTo({Variables::kSearchMetaId})) {
            return std::string(
                "$$SEARCH_META cannot be used in a sharded environment unless "
                "'featureFlagSearchShardedFacets' is enabled");
        }
    }
    return boost::none;
}

boost::optional<std::string> SearchImplementedHelperFunctions::validatePipelineForShardedCollection(
    const Pipeline& pipeline) {
    if (!feature_flags::gFeatureFlagSearchShardedFacets.isEnabled(
            serverGlobalParams.featureCompatibility)) {
        return validatePipelineForShardedCollectionHelper(pipeline.getSources());
    }
    return boost::none;
}

void mongo::mongot_cursor::SearchImplementedHelperFunctions::injectSearchShardFiltererIfNeeded(
    Pipeline* pipeline) {
    injectSearchShardFilteredIfNeeded(pipeline);
}

}  // namespace mongo::mongot_cursor
