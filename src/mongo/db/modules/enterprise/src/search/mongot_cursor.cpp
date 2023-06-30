/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongot_cursor.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "document_source_search.h"
#include "document_source_search_meta.h"
#include "mongo/db/exec/shard_filterer_impl.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/search_helper.h"
#include "mongo/db/pipeline/sharded_agg_helpers.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/server_options.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/query/document_source_merge_cursors.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/net/hostandport.h"
#include "mongot_options.h"
#include "search/plan_sharded_search_gen.h"
#include "search_task_executors.h"
#include "vector_search/document_source_vector_search.h"

namespace mongo::mongot_cursor {

namespace {

executor::TaskExecutorCursor::Options getSearchCursorOptions(
    bool preFetchNextBatch, std::function<void(BSONObjBuilder& bob)> augmentGetMore) {
    executor::TaskExecutorCursor::Options opts;
    // If we are pushing down a limit to mongot, then we should avoid prefetching the next
    // batch. We optimistically assume that we will only need a single batch and attempt to
    // avoid doing unnecessary work on mongot. If $idLookup filters out enough documents
    // such that we are not able to satisfy the limit, then we will fetch the next batch
    // syncronously on the subsequent 'getNext()' call.
    opts.preFetchNextBatch = preFetchNextBatch;
    if (!opts.preFetchNextBatch) {
        // Only set this function if we will not be prefetching.
        opts.getMoreAugmentationWriter = augmentGetMore;
    }
    return opts;
}

executor::RemoteCommandRequest getRemoteCommandRequestForSearchQuery(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    const boost::optional<long long> docsRequested,
    const boost::optional<int> protocolVersion = boost::none) {
    BSONObjBuilder cmdBob;
    cmdBob.append("search", expCtx->ns.coll());
    uassert(
        6584801,
        str::stream() << "A uuid is required for a search query, but was missing. Got namespace "
                      << expCtx->ns.toStringForErrorMsg(),
        expCtx->uuid);
    expCtx->uuid.value().appendToBuilder(&cmdBob, kCollectionUuidField);
    cmdBob.append("query", query);
    if (expCtx->explain) {
        cmdBob.append("explain",
                      BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
    }
    if (protocolVersion) {
        cmdBob.append("intermediate", *protocolVersion);
    }
    // (Ignore FCV check): This feature is enabled on an earlier FCV.
    if (feature_flags::gFeatureFlagSearchBatchSizeLimit.isEnabledAndIgnoreFCVUnsafe() &&
        docsRequested.has_value()) {
        BSONObjBuilder cursorOptionsBob(cmdBob.subobjStart(kCursorOptionsField));
        cursorOptionsBob.append(kDocsRequestedField, docsRequested.get());
        cursorOptionsBob.doneFast();
    }
    return getRemoteCommandRequest(expCtx, cmdBob.obj());
}

auto makeRetryOnNetworkErrorPolicy() {
    return [retried = false](const Status& st) mutable {
        return std::exchange(retried, true) ? false : ErrorCodes::isNetworkError(st);
    };
}

/**
 * A helper function to run the initial search query and return a list CursorResponse.
 * The logic mimics the TaskExecutorCursor for initial command, we will use the cursor responses to
 * establish TaskExecutorCursor elsewhere in the SBE stage, the cursor id and first batch will be
 * parameterized as the SBE plan could be cached.
 */
std::vector<CursorResponse> executeInitialSearchQuery(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    std::shared_ptr<executor::TaskExecutor> taskExecutor,
    boost::optional<long long> docsRequested,
    const boost::optional<int>& protocolVersion) {

    std::vector<CursorResponse> result;
    auto req = getRemoteCommandRequestForSearchQuery(expCtx, query, docsRequested, protocolVersion);
    for (;;) {
        try {
            SharedPromise<BSONObj> promise;
            auto cb = uassertStatusOK(taskExecutor->scheduleRemoteCommand(
                req, [&promise](const executor::TaskExecutor::RemoteCommandCallbackArgs& args) {
                    if (args.response.isOK()) {
                        promise.emplaceValue(args.response.data);
                    } else {
                        promise.setError(args.response.status);
                    }
                }));
            auto out = promise.getFuture().getNoThrow(expCtx->opCtx);
            uassertStatusOK(out);
            auto cursorResponses = CursorResponse::parseFromBSONMany(out.getValue());

            for (auto&& cursorResponse : cursorResponses) {
                result.emplace_back(uassertStatusOK(std::move(cursorResponse)));
            }
            return result;
        } catch (const DBException& ex) {
            if (!makeRetryOnNetworkErrorPolicy()(ex.toStatus())) {
                throw;
            }
        }
    }
}
}  // namespace

executor::RemoteCommandRequest getRemoteCommandRequest(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const BSONObj& cmdObj) {
    uassert(31082,
            str::stream() << "search and mongot vector search not enabled! "
                          << "Enable Search by setting serverParameter mongotHost to a valid "
                          << "\"host:port\" string",
            globalMongotParams.enabled);
    auto swHostAndPort = HostAndPort::parse(globalMongotParams.host);
    // This host and port string is configured and validated at startup.
    invariant(swHostAndPort.getStatus().isOK());
    executor::RemoteCommandRequest rcr(executor::RemoteCommandRequest(
        swHostAndPort.getValue(), expCtx->ns.db_deprecated().toString(), cmdObj, expCtx->opCtx));
    rcr.sslMode = transport::ConnectSSLMode::kDisableSSL;
    return rcr;
}

std::vector<executor::TaskExecutorCursor> establishCursors(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const executor::RemoteCommandRequest& command,
    std::shared_ptr<executor::TaskExecutor> taskExecutor,
    bool preFetchNextBatch,
    std::function<void(BSONObjBuilder& bob)> augmentGetMore) {
    std::vector<executor::TaskExecutorCursor> cursors;
    auto initialCursor =
        makeTaskExecutorCursor(expCtx->opCtx,
                               taskExecutor,
                               command,
                               getSearchCursorOptions(preFetchNextBatch, augmentGetMore),
                               makeRetryOnNetworkErrorPolicy());

    auto additionalCursors = initialCursor.releaseAdditionalCursors();
    cursors.push_back(std::move(initialCursor));
    // Preserve cursor order. Expect cursors to be labeled, so this may not be necessary.
    for (auto& thisCursor : additionalCursors) {
        cursors.push_back(std::move(thisCursor));
    }

    return cursors;
}

bool SearchImplementedHelperFunctions::isSearchPipeline(const Pipeline* pipeline) {
    if (!pipeline || pipeline->getSources().empty()) {
        return false;
    }
    return isSearchStage(pipeline->peekFront());
}

bool SearchImplementedHelperFunctions::isSearchMetaPipeline(const Pipeline* pipeline) {
    if (!pipeline || pipeline->getSources().empty()) {
        return false;
    }
    return isSearchMetaStage(pipeline->peekFront());
}

/** Because 'DocumentSourceSearchMeta' inherits from 'DocumentSourceInternalSearchMongotRemote',
 *  to make sure a DocumentSource is a $search stage and not $searchMeta check it is either:
 *    - a 'DocumentSourceSearch'.
 *    - a 'DocumentSourceInternalSearchMongotRemote' and not a 'DocumentSourceSearchMeta'.
 * TODO: SERVER-78159 refactor after DocumentSourceInternalSearchMongotRemote and
 * DocumentSourceInternalIdLookup are merged into into DocumentSourceSearch.
 */
bool SearchImplementedHelperFunctions::isSearchStage(DocumentSource* stage) {
    return stage &&
        (dynamic_cast<mongo::DocumentSourceSearch*>(stage) ||
         (dynamic_cast<mongo::DocumentSourceInternalSearchMongotRemote*>(stage) &&
          !dynamic_cast<mongo::DocumentSourceSearchMeta*>(stage)));
}

bool SearchImplementedHelperFunctions::isSearchMetaStage(DocumentSource* stage) {
    return stage && dynamic_cast<mongo::DocumentSourceSearchMeta*>(stage);
}

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
    auto shouldBuildMetadataPipeline = !expCtx->inMongos &&
        (typeid(*expCtx->mongoProcessInterface) != typeid(StubMongoProcessInterface) &&
         expCtx->mongoProcessInterface->inShardedEnvironment(expCtx->opCtx));

    uassert(
        6253506, "Cannot have exchange specified in a $search pipeline", !request.getExchange());

    // Some tests build $search pipelines without actually setting up a mongot. In this case either
    // return a dummy stage or nothing depending on the environment. Note that in this case we don't
    // actually make any queries, the document source will return eof immediately.
    if (MONGO_unlikely(DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup())) {
        if (shouldBuildMetadataPipeline) {
            // Construct a duplicate ExpressionContext for our cloned pipeline. This is necessary
            // so that the duplicated pipeline and the cloned pipeline do not accidentally
            // share an OperationContext.
            auto newExpCtx = expCtx->copyWith(expCtx->ns, expCtx->uuid);
            return Pipeline::create({origSearchStage->clone(newExpCtx)}, newExpCtx);
        }
        return nullptr;
    }

    // This will be used to augment the getMore command sent to mongot.
    auto augmentGetMore = [origSearchStage](BSONObjBuilder& bob) {
        auto docsNeeded = origSearchStage->calcDocsNeeded();
        // (Ignore FCV check): This feature is enabled on an earlier FCV.
        if (feature_flags::gFeatureFlagSearchBatchSizeLimit.isEnabledAndIgnoreFCVUnsafe() &&
            docsNeeded.has_value()) {
            BSONObjBuilder cursorOptionsBob(bob.subobjStart(mongot_cursor::kCursorOptionsField));
            cursorOptionsBob.append(mongot_cursor::kDocsRequestedField, docsNeeded.get());
            cursorOptionsBob.doneFast();
        }
    };

    // The search stage has not yet established its cursor on mongoT. Establish the cursor for it.
    auto cursors = mongot_cursor::establishSearchCursors(
        expCtx,
        origSearchStage->getSearchQuery(),
        origSearchStage->getTaskExecutor(),
        origSearchStage->getMongotDocsRequested(),
        augmentGetMore,
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
        auto cursorType =
            CursorType_parse(IDLParserContext("ShardedAggHelperCursorType"), cursorLabel.value());
        if (cursorType == CursorTypeEnum::DocumentResult) {
            origSearchStage->setCursor(std::move(*it));
            origPipeline->pipelineType = CursorTypeEnum::DocumentResult;
        } else if (cursorType == CursorTypeEnum::SearchMetaResult) {
            // If we don't think we're in a sharded environment, mongot should not have sent
            // metadata.
            tassert(
                6253303, "Didn't expect metadata cursor from mongot", shouldBuildMetadataPipeline);
            tassert(
                6253726, "Expected to not already have created a metadata pipeline", !newPipeline);

            // Construct a duplicate ExpressionContext for our cloned pipeline. This is necessary
            // so that the duplicated pipeline and the cloned pipeline do not accidentally
            // share an OperationContext.
            auto newExpCtx = expCtx->copyWith(expCtx->ns, expCtx->uuid);

            // Clone the MongotRemote stage and set the metadata cursor.
            auto newStage = origSearchStage->copyForAlternateSource(std::move(*it), newExpCtx);

            // Build a new pipeline with the metadata source as the only stage.
            newPipeline = Pipeline::create({newStage}, newExpCtx);
            newPipeline->pipelineType = CursorTypeEnum::SearchMetaResult;
        } else {
            tasserted(6253302,
                      str::stream() << "Unexpected cursor type '" << cursorLabel.value() << "'");
        }
    }

    // Can return null if we did not build a metadata pipeline.
    return newPipeline;
}

BSONObj getExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                           const executor::RemoteCommandRequest& request,
                           executor::TaskExecutor* taskExecutor) {
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

std::vector<executor::TaskExecutorCursor> establishSearchCursors(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    std::shared_ptr<executor::TaskExecutor> taskExecutor,
    boost::optional<long long> docsRequested,
    std::function<void(BSONObjBuilder& bob)> augmentGetMore,
    const boost::optional<int>& protocolVersion) {
    // UUID is required for mongot queries. If not present, no results for the query as the
    // collection has not been created yet.
    if (!expCtx->uuid) {
        return {};
    }
    return establishCursors(
        expCtx,
        getRemoteCommandRequestForSearchQuery(expCtx, query, docsRequested, protocolVersion),
        taskExecutor,
        !docsRequested.has_value(),
        augmentGetMore);
}

BSONObj getSearchExplainResponse(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                 const BSONObj& query,
                                 executor::TaskExecutor* taskExecutor) {
    const auto request = getRemoteCommandRequestForSearchQuery(expCtx, query, boost::none);
    return getExplainResponse(expCtx, request, taskExecutor);
}

namespace {

// Returns a pair of booleans. The first is whether or not '$$SEARCH_META' is set by 'pipeline', the
// second is whether '$$SEARCH_META' is accessed by 'pipeline'. It is assumed that if there is a
// 'DocumentSourceInternalSearchMongotRemote' then '$$SEARCH_META' will be set at some point in the
// pipeline. Depending on the configuration of the cluster
// 'DocumentSourceSetVariableFromSubPipeline' could do the actual setting of the variable, but it
// can only be generated alongside a 'DocumentSourceInternalSearchMongotRemote'.
void assertSearchMetaAccessValidHelper(const Pipeline::SourceContainer& pipeline) {
    // Whether or not there was a sub-pipeline stage previously in this pipeline.
    bool subPipeSeen = false;
    bool searchMetaSet = false;

    for (const auto& source : pipeline) {
        // Check if this is a stage that sets $$SEARCH_META.
        static constexpr StringData kSetVarName =
            DocumentSourceSetVariableFromSubPipeline::kStageName;
        auto stageName = StringData(source->getSourceName());
        if (stageName == DocumentSourceInternalSearchMongotRemote::kStageName ||
            stageName == DocumentSourceSearch::kStageName || stageName == kSetVarName) {
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

        // Check if this stage references $$SEARCH_META.
        std::set<Variables::Id> refs;
        source->addVariableRefs(&refs);
        if (Variables::hasVariableReferenceTo(refs, {Variables::kSearchMetaId})) {
            uassert(6347901,
                    "Can't access $$SEARCH_META after a stage with a sub-pipeline",
                    !subPipeSeen || thisStageSubPipeline);
            uassert(6347902,
                    "Can't access $$SEARCH_META without a $search stage earlier in the pipeline",
                    searchMetaSet);
        }
    }
}

/**
 * Preparing for pipeline starting with $search on the DocumentSource-based implementation before
 * the query execution.
 * 'applyShardFilter' should be true for top level pipelines only.
 */
void prepareSearchPipeline(Pipeline* pipeline, bool applyShardFilter) {
    auto searchStage = pipeline->popFrontWithName(DocumentSourceSearch::kStageName);
    auto& sources = pipeline->getSources();
    if (searchStage) {
        auto desugaredPipeline = dynamic_cast<DocumentSourceSearch*>(searchStage.get())->desugar();
        sources.insert(sources.begin(), desugaredPipeline.begin(), desugaredPipeline.end());
        Pipeline::stitch(&sources);
    }

    auto internalSearchLookupIt = sources.begin();
    // Bail early if the pipeline is not $_internalSearchMongotRemote stage or doesn't need to apply
    // shardFilter.
    if (internalSearchLookupIt == sources.end() || !applyShardFilter ||
        (mongo::DocumentSourceInternalSearchMongotRemote::kStageName !=
             (*internalSearchLookupIt)->getSourceName() &&
         mongo::DocumentSourceVectorSearch::kStageName !=
             (*internalSearchLookupIt)->getSourceName())) {
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
        if (OperationShardingState::isComingFromRouter(expCtx->opCtx)) {
            // We can only rely on the ownership filter if the operation is coming from the router
            // (i.e. it is versioned).
            auto collectionFilter =
                CollectionShardingState::acquire(expCtx->opCtx, expCtx->ns)
                    ->getOwnershipFilter(
                        expCtx->opCtx,
                        CollectionShardingState::OrphanCleanupPolicy::kDisallowOrphanCleanup);
            auto doc = new DocumentSourceInternalShardFilter(
                expCtx, std::make_unique<ShardFiltererImpl>(std::move(collectionFilter)));
            internalSearchLookupIt++;
            sources.insert(internalSearchLookupIt, doc);
            Pipeline::stitch(&sources);
        }
    }
}

/**
 * Send the search command `cmdObj` to the remote search server this process is connected to.
 * Retry the command on failure whenever the retryPolicy argument indicates we should; the policy
 * accepts a Status encoding the error the command failed with (local or remote) and returns a
 * bool that is `true` when we should retry. The default is to retry once on network errors.
 *
 * Returns the RemoteCommandResponse we received from the remote. If we fail to get an OK
 * response from the remote after all retry attempts conclude, we throw the error the most
 * recent attempt failed with.
 */
executor::RemoteCommandResponse runSearchCommandWithRetries(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& cmdObj,
    std::function<bool(Status)> retryPolicy = makeRetryOnNetworkErrorPolicy()) {
    using namespace fmt::literals;
    auto taskExecutor = executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext());
    executor::RemoteCommandResponse response =
        Status(ErrorCodes::InternalError, "Internal error running search command");
    for (;;) {
        Status err = Status::OK();
        do {
            auto swCbHnd = taskExecutor->scheduleRemoteCommand(
                getRemoteCommandRequest(expCtx, cmdObj),
                [&](const auto& args) { response = args.response; });
            err = swCbHnd.getStatus();
            if (!err.isOK()) {
                // scheduling error
                err.addContext("Failed to execute search command: {}"_format(cmdObj.toString()));
                break;
            }
            taskExecutor->wait(swCbHnd.getValue(), expCtx->opCtx);
            err = response.status;
            if (!err.isOK()) {
                // Local error running the command.
                err.addContext("Failed to execute search command: {}"_format(cmdObj.toString()));
                break;
            }
            err = getStatusFromCommandResult(response.data);
            if (!err.isOK()) {
                // Mongot ran the command and returned an error.
                err.addContext("mongot returned an error");
                break;
            }
        } while (0);

        if (err.isOK())
            return response;
        if (!retryPolicy(err))
            uassertStatusOK(err);
    }
}

ServiceContext::ConstructorActionRegisterer searchQueryImplementation{
    "searchQueryImplementation", {"searchQueryHelperRegisterer"}, [](ServiceContext* context) {
        invariant(context);
        getSearchHelpers(context) = std::make_unique<SearchImplementedHelperFunctions>();
    }};
}  // namespace

InternalSearchMongotRemoteSpec planShardedSearch(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const BSONObj& searchRequest) {
    // Mongos issues the 'planShardedSearch' command rather than 'search' in order to:
    // * Create the merging pipeline.
    // * Get a sortSpec.
    const auto cmdObj = [&]() {
        PlanShardedSearchSpec cmd(expCtx->ns.coll().rawData() /* planShardedSearch */,
                                  searchRequest /* query */);

        if (expCtx->explain) {
            cmd.setExplain(BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
        }

        // Add the searchFeatures field.
        cmd.setSearchFeatures(
            BSON(SearchFeatures_serializer(SearchFeaturesEnum::kShardedSort) << 1));

        return cmd.toBSON();
    }();
    // Send the planShardedSearch to the remote, retrying on network errors.
    auto response = runSearchCommandWithRetries(expCtx, cmdObj);

    InternalSearchMongotRemoteSpec remoteSpec(searchRequest.getOwned(),
                                              response.data["protocolVersion"_sd].Int());
    auto parsedPipeline = mongo::Pipeline::parseFromArray(response.data["metaPipeline"], expCtx);
    remoteSpec.setMergingPipeline(parsedPipeline->serializeToBson());
    if (response.data.hasElement("sortSpec")) {
        remoteSpec.setSortSpec(response.data["sortSpec"].Obj().getOwned());
    }

    return remoteSpec;
}

void SearchImplementedHelperFunctions::assertSearchMetaAccessValid(
    const Pipeline::SourceContainer& pipeline, ExpressionContext* expCtx) {
    // If we already validated this pipeline on mongos, no need to do it again on a shard. Check
    // mergeCursors because we could be on a shard doing the merge.
    if ((expCtx->inMongos || !expCtx->needsMerge) &&
        pipeline.front()->getSourceName() != DocumentSourceMergeCursors::kStageName) {
        assertSearchMetaAccessValidHelper(pipeline);
    }
}

void mongo::mongot_cursor::SearchImplementedHelperFunctions::prepareSearchForTopLevelPipeline(
    Pipeline* pipeline) {
    prepareSearchPipeline(pipeline, true);
}

void mongo::mongot_cursor::SearchImplementedHelperFunctions::prepareSearchForNestedPipeline(
    Pipeline* pipeline) {
    prepareSearchPipeline(pipeline, false);
}

boost::optional<executor::TaskExecutorCursor>
SearchImplementedHelperFunctions::establishSearchCursor(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& query,
    CursorResponse&& response,
    boost::optional<long long> docsRequested,
    std::function<void(BSONObjBuilder& bob)> augmentGetMore,
    const boost::optional<int>& protocolVersion) {

    auto executor = executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext());
    auto req = getRemoteCommandRequestForSearchQuery(expCtx, query, docsRequested, protocolVersion);
    return executor::TaskExecutorCursor(
        executor,
        nullptr /* underlyingExec */,
        std::move(response),
        req,
        getSearchCursorOptions(!docsRequested.has_value(), augmentGetMore));
}

bool hasReferenceToSearchMeta(const DocumentSource& ds) {
    std::set<Variables::Id> refs;
    ds.addVariableRefs(&refs);
    return Variables::hasVariableReferenceTo(refs,
                                             std::set<Variables::Id>{Variables::kSearchMetaId});
}

}  // namespace mongo::mongot_cursor
