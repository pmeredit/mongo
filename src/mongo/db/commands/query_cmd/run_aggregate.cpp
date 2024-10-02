/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/commands/query_cmd/run_aggregate.h"

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <boost/cstdint.hpp>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
// IWYU pragma: no_include "ext/alloc_traits.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/client/read_preference.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/api_parameters.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/basic_types.h"
#include "mongo/db/basic_types_gen.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/catalog/collection_uuid_mismatch.h"
#include "mongo/db/catalog/external_data_source_scope_guard.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/change_stream_serverless_helpers.h"
#include "mongo/db/client.h"
#include "mongo/db/cluster_role.h"
#include "mongo/db/commands/query_cmd/aggregation_execution_state.h"
#include "mongo/db/curop.h"
#include "mongo/db/database_name.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/disk_use_options_gen.h"
#include "mongo/db/fle_crud.h"
#include "mongo/db/logical_time.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/change_stream_invalidation_info.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_exchange.h"
#include "mongo/db/pipeline/document_source_geo_near.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/initialize_auto_get_helper.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_d.h"
#include "mongo/db/pipeline/plan_executor_pipeline.h"
#include "mongo/db/pipeline/process_interface/mongo_process_interface.h"
#include "mongo/db/pipeline/search/search_helper.h"
#include "mongo/db/pipeline/visitors/document_source_visitor_docs_needed_bounds.h"
#include "mongo/db/profile_settings.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/client_cursor/clientcursor.h"
#include "mongo/db/query/client_cursor/collect_query_stats_mongod.h"
#include "mongo/db/query/client_cursor/cursor_id.h"
#include "mongo/db/query/client_cursor/cursor_manager.h"
#include "mongo/db/query/client_cursor/cursor_response.h"
#include "mongo/db/query/collation/collation_spec.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/db/query/collection_query_info.h"
#include "mongo/db/query/explain.h"
#include "mongo/db/query/explain_options.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/query/multiple_collection_accessor.h"
#include "mongo/db/query/optimizer/defs.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/db/query/plan_explainer.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/query/query_knob_configuration.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/db/query/query_request_helper.h"
#include "mongo/db/query/query_settings/query_settings_utils.h"
#include "mongo/db/query/query_shape/agg_cmd_shape.h"
#include "mongo/db/query/query_stats/agg_key.h"
#include "mongo/db/query/query_stats/key.h"
#include "mongo/db/query/query_stats/query_stats.h"
#include "mongo/db/read_concern.h"
#include "mongo/db/read_write_concern_provenance.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/tenant_migration_access_blocker_util.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/s/query_analysis_writer.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameter.h"
#include "mongo/db/stats/resource_consumption_metrics.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/tenant_id.h"
#include "mongo/db/transaction/transaction_participant.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/db/views/resolved_view.h"
#include "mongo/db/views/view.h"
#include "mongo/db/views/view_catalog_helpers.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/redaction.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/analyze_shard_key_common_gen.h"
#include "mongo/s/query_analysis_sampler_util.h"
#include "mongo/s/resource_yielders.h"
#include "mongo/s/router_role.h"
#include "mongo/s/shard_version.h"
#include "mongo/s/sharding_state.h"
#include "mongo/s/stale_exception.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/future.h"
#include "mongo/util/intrusive_counter.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/serialization_context.h"
#include "mongo/util/str.h"
#include "mongo/util/string_map.h"
#include "mongo/util/synchronized_value.h"
#include "mongo/util/time_support.h"
#include "mongo/util/uuid.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo {

using boost::intrusive_ptr;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::unique_ptr;
using NamespaceStringSet = stdx::unordered_set<NamespaceString>;

Counter64& allowDiskUseFalseCounter = *MetricBuilder<Counter64>{"query.allowDiskUseFalse"};

namespace {
std::unique_ptr<Pipeline, PipelineDeleter> handleViewHelper(
    const AggExState& aggExState,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
    boost::optional<UUID> uuid) {
    if (aggExState.getResolvedView()->timeseries()) {
        // For timeseries, there may have been rewrites done on the raw BSON pipeline
        // during view resolution. We must parse the request's full resolved pipeline
        // which will account for those rewrites.
        // TODO SERVER-82101 Re-organize timeseries rewrites so timeseries can follow the
        // same pattern here as other views
        return Pipeline::parse(aggExState.getRequest().getPipeline(), expCtx);
    }
    // Search queries on views behave differently than non-search aggregations on views.
    // When a user pipeline contains a $search/$vectorSearch stage, idLookup will apply the
    // view transforms as part of its subpipeline. In this way, the view stages will always
    // be applied directly after $_internalSearchMongotRemote and before the remaining
    // stages of the user pipeline. This is to ensure the stages following
    // $search/$vectorSearch in the user pipeline will receive the modified documents: when
    // storedSource is disabled, idLookup will retrieve full/unmodified documents during
    // (from the _id values returned by mongot), apply the view's data transforms, and pass
    // said transformed documents through the rest of the user pipeline.
    else if (search_helpers::isMongotPipeline(pipeline.get())) {
        search_helpers::setResolvedNamespaceForSearch(
            aggExState.getOriginalNss(), aggExState.getResolvedView().value(), expCtx, uuid);
        return pipeline;
    }
    // Parse the view pipeline, then stitch the user pipeline and view pipeline together
    // to build the total aggregation pipeline.
    auto userPipeline = std::move(pipeline);
    pipeline = Pipeline::parse(aggExState.getResolvedView()->getPipeline(), expCtx);
    pipeline->appendPipeline(std::move(userPipeline));
    return pipeline;
}
}  // namespace

namespace {
// Ticks for server-side Javascript deprecation log messages.
Rarely _samplerAccumulatorJs, _samplerFunctionJs;

MONGO_FAIL_POINT_DEFINE(hangAfterCreatingAggregationPlan);
MONGO_FAIL_POINT_DEFINE(hangAfterAcquiringCollectionCatalog);

/**
 * If a pipeline is empty (assuming that a $cursor stage hasn't been created yet), it could mean
 * that we were able to absorb all pipeline stages and pull them into a single PlanExecutor. So,
 * instead of creating a whole pipeline to do nothing more than forward the results of its cursor
 * document source, we can optimize away the entire pipeline and answer the request using the query
 * engine only. This function checks if such optimization is possible.
 */
bool canOptimizeAwayPipeline(const AggExState& aggExState,
                             const Pipeline* pipeline,
                             const PlanExecutor* exec,
                             bool hasGeoNearStage) {
    return pipeline && exec && !hasGeoNearStage && !aggExState.hasChangeStream() &&
        pipeline->getSources().empty() &&
        // For exchange we will create a number of pipelines consisting of a single
        // DocumentSourceExchange stage, so cannot not optimize it away.
        !aggExState.getRequest().getExchange();
}

/**
 * Creates and registers a cursor with the global cursor manager. Returns the pinned cursor.
 */
ClientCursorPin registerCursor(const AggExState& aggExState,
                               const boost::intrusive_ptr<ExpressionContext>& expCtx,
                               unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec) {
    auto opCtx = aggExState.getOpCtx();
    ClientCursorParams cursorParams(
        std::move(exec),
        aggExState.getOriginalNss(),
        AuthorizationSession::get(opCtx->getClient())->getAuthenticatedUserName(),
        APIParameters::get(opCtx),
        opCtx->getWriteConcern(),
        repl::ReadConcernArgs::get(opCtx),
        ReadPreferenceSetting::get(opCtx),
        *aggExState.getDeferredCmd(),
        aggExState.getPrivileges());
    cursorParams.setTailableMode(expCtx->tailableMode);

    auto pin = CursorManager::get(opCtx)->registerCursor(opCtx, std::move(cursorParams));

    pin->incNBatches();
    auto extDataSrcGuard = aggExState.getExternalDataSourceScopeGuard();
    if (extDataSrcGuard) {
        ExternalDataSourceScopeGuard::get(pin.getCursor()) = extDataSrcGuard;
    }
    return pin;
}

/**
 * Updates query stats in OpDebug using the plan explainer from the pinned cursor (if given)
 * or the given executor (otherwise) and collects them in the query stats store.
 */
void collectQueryStats(const AggExState& aggExState,
                       const boost::intrusive_ptr<ExpressionContext>& expCtx,
                       mongo::PlanExecutor* maybeExec,
                       ClientCursorPin* maybePinnedCursor) {
    invariant(maybeExec || maybePinnedCursor);
    auto opCtx = aggExState.getOpCtx();
    auto curOp = CurOp::get(opCtx);
    const auto& planExplainer = maybePinnedCursor
        ? maybePinnedCursor->getCursor()->getExecutor()->getPlanExplainer()
        : maybeExec->getPlanExplainer();
    PlanSummaryStats stats;
    planExplainer.getSummaryStats(&stats);
    curOp->setEndOfOpMetrics(stats.nReturned);
    curOp->debug().setPlanSummaryMetrics(std::move(stats));

    if (maybePinnedCursor) {
        collectQueryStatsMongod(opCtx, *maybePinnedCursor);
    } else {
        collectQueryStatsMongod(opCtx, expCtx, std::move(curOp->debug().queryStatsInfo.key));
    }
}

/**
 * Builds the reply for a pipeline over a sharded collection that contains an exchange stage.
 */
void handleMultipleCursorsForExchange(const AggExState& aggExState,
                                      const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                      std::vector<ClientCursorPin>& pinnedCursors,
                                      rpc::ReplyBuilderInterface* result) {
    invariant(pinnedCursors.size() > 1);
    collectQueryStats(aggExState, expCtx, nullptr, &pinnedCursors[0]);

    long long batchSize = aggExState.getRequest().getCursor().getBatchSize().value_or(
        aggregation_request_helper::kDefaultBatchSize);

    uassert(ErrorCodes::BadValue, "the exchange initial batch size must be zero", batchSize == 0);

    BSONArrayBuilder cursorsBuilder;
    for (auto&& pinnedCursor : pinnedCursors) {
        auto cursor = pinnedCursor.getCursor();
        invariant(cursor);

        BSONObjBuilder cursorResult;
        appendCursorResponseObject(cursor->cursorid(),
                                   aggExState.getOriginalNss(),
                                   BSONArray(),
                                   cursor->getExecutor()->getExecutorType(),
                                   &cursorResult,
                                   SerializationContext::stateCommandReply(
                                       aggExState.getRequest().getSerializationContext()));
        cursorResult.appendBool("ok", 1);

        cursorsBuilder.append(cursorResult.obj());

        // If a time limit was set on the pipeline, remaining time is "rolled over" to the cursor
        // (for use by future getmore ops).
        cursor->setLeftoverMaxTimeMicros(aggExState.getOpCtx()->getRemainingMaxTimeMicros());

        // Cursor needs to be in a saved state while we yield locks for getmore. State will be
        // restored in getMore().
        cursor->getExecutor()->saveState();
        cursor->getExecutor()->detachFromOperationContext();
    }

    auto bodyBuilder = result->getBodyBuilder();
    bodyBuilder.appendArray("cursors", cursorsBuilder.obj());
}

/**
 * Gets the first batch of documents produced by this pipeline by calling 'getNext()' on the
 * provided PlanExecutor. The provided CursorResponseBuilder will be populated with the batch.
 *
 * Returns true if we need to register a ClientCursor saved for this pipeline (for future getMore
 * requests). Otherwise, returns false.
 */
bool getFirstBatch(const AggExState& aggExState,
                   boost::intrusive_ptr<ExpressionContext> expCtx,
                   PlanExecutor& exec,
                   CursorResponseBuilder& responseBuilder) {
    auto opCtx = aggExState.getOpCtx();
    long long batchSize = aggExState.getRequest().getCursor().getBatchSize().value_or(
        aggregation_request_helper::kDefaultBatchSize);

    auto curOp = CurOp::get(opCtx);
    ResourceConsumption::DocumentUnitCounter docUnitsReturned;

    bool doRegisterCursor = true;
    bool stashedResult = false;
    // We are careful to avoid ever calling 'getNext()' on the PlanExecutor when the batchSize is
    // zero to avoid doing any query execution work.
    for (int objCount = 0; objCount < batchSize; objCount++) {
        PlanExecutor::ExecState state;
        BSONObj nextDoc;

        try {
            state = exec.getNext(&nextDoc, nullptr);
        } catch (const ExceptionFor<ErrorCodes::CloseChangeStream>&) {
            // This exception is thrown when a $changeStream stage encounters an event that
            // invalidates the cursor. We should close the cursor and return without error.
            doRegisterCursor = false;
            break;
        } catch (const ExceptionFor<ErrorCodes::ChangeStreamInvalidated>& ex) {
            // This exception is thrown when a change-stream cursor is invalidated. Set the PBRT to
            // the resume token of the invalidating event, and mark the cursor response as
            // invalidated. We expect ExtraInfo to always be present for this exception.
            const auto extraInfo = ex.extraInfo<ChangeStreamInvalidationInfo>();
            tassert(5493701, "Missing ChangeStreamInvalidationInfo on exception", extraInfo);

            responseBuilder.setPostBatchResumeToken(extraInfo->getInvalidateResumeToken());
            responseBuilder.setInvalidated();

            doRegisterCursor = false;
            break;
        } catch (DBException& exception) {
            auto&& explainer = exec.getPlanExplainer();
            auto&& [stats, _] =
                explainer.getWinningPlanStats(ExplainOptions::Verbosity::kExecStats);
            LOGV2_WARNING(23799,
                          "Aggregate command executor error",
                          "error"_attr = exception.toStatus(),
                          "stats"_attr = redact(stats),
                          "cmd"_attr = *aggExState.getDeferredCmd());

            exception.addContext("PlanExecutor error during aggregation");
            throw;
        }

        if (state == PlanExecutor::IS_EOF) {
            // If this executor produces a postBatchResumeToken, add it to the cursor response. We
            // call this on EOF because the PBRT may advance even when there are no further results.
            responseBuilder.setPostBatchResumeToken(exec.getPostBatchResumeToken());

            if (!expCtx->isTailable()) {
                // There are no more documents, and the query is not tailable, so no need to create
                // a cursor.
                doRegisterCursor = false;
            }

            break;
        }

        invariant(state == PlanExecutor::ADVANCED);

        // If adding this object will cause us to exceed the message size limit, then we stash it
        // for later.
        if (!FindCommon::haveSpaceForNext(nextDoc, objCount, responseBuilder.bytesUsed())) {
            exec.stashResult(nextDoc);
            stashedResult = true;
            break;
        }

        // If this executor produces a postBatchResumeToken, add it to the cursor response.
        responseBuilder.setPostBatchResumeToken(exec.getPostBatchResumeToken());
        responseBuilder.append(nextDoc);
        docUnitsReturned.observeOne(nextDoc.objsize());
    }

    if (doRegisterCursor) {
        // For empty batches, or in the case where the final result was added to the batch rather
        // than being stashed, we update the PBRT to ensure that it is the most recent available.
        if (!stashedResult) {
            responseBuilder.setPostBatchResumeToken(exec.getPostBatchResumeToken());
        }

        // Cursor needs to be in a saved state while we yield locks for getmore. State will be
        // restored in getMore().
        exec.saveState();
        exec.detachFromOperationContext();
    } else {
        curOp->debug().cursorExhausted = true;
    }

    auto& metricsCollector = ResourceConsumption::MetricsCollector::get(opCtx);
    metricsCollector.incrementDocUnitsReturned(curOp->getNS(), docUnitsReturned);

    return doRegisterCursor;
}

boost::optional<ClientCursorPin> executeSingleExecUntilFirstBatch(
    const AggExState& aggExState,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    std::vector<unique_ptr<PlanExecutor, PlanExecutor::Deleter>>& execs,
    rpc::ReplyBuilderInterface* result) {
    auto opCtx = aggExState.getOpCtx();
    boost::optional<ClientCursorPin> maybePinnedCursor;
    CursorResponseBuilder::Options options;
    options.isInitialResponse = true;
    if (!opCtx->inMultiDocumentTransaction()) {
        options.atClusterTime = repl::ReadConcernArgs::get(opCtx).getArgsAtClusterTime();
    }
    CursorResponseBuilder responseBuilder(result, options);

    auto cursorId = 0LL;
    const bool doRegisterCursor = getFirstBatch(aggExState, expCtx, *execs[0], responseBuilder);

    if (doRegisterCursor) {
        auto curOp = CurOp::get(opCtx);
        // Only register a cursor for the pipeline if we have found that we need one for future
        // calls to 'getMore()'.
        maybePinnedCursor = registerCursor(aggExState, expCtx, std::move(execs[0]));
        auto cursor = maybePinnedCursor->getCursor();
        cursorId = cursor->cursorid();
        curOp->debug().cursorid = cursorId;

        // If a time limit was set on the pipeline, remaining time is "rolled over" to the
        // cursor (for use by future getmore ops).
        cursor->setLeftoverMaxTimeMicros(opCtx->getRemainingMaxTimeMicros());
    }

    collectQueryStats(aggExState, expCtx, execs[0].get(), maybePinnedCursor.get_ptr());

    boost::optional<CursorMetrics> metrics = aggExState.getRequest().getIncludeQueryStatsMetrics()
        ? boost::make_optional(CurOp::get(opCtx)->debug().getCursorMetrics())
        : boost::none;
    responseBuilder.done(
        cursorId,
        aggExState.getOriginalNss(),
        std::move(metrics),
        SerializationContext::stateCommandReply(aggExState.getRequest().getSerializationContext()));

    return maybePinnedCursor;
}

/**
 * Executes the aggregation pipeline, registering any cursors needed for subsequent calls to
 * getMore() if necessary. Returns the first ClientCursorPin, if any cursor was registered.
 */
boost::optional<ClientCursorPin> executeUntilFirstBatch(
    const AggExState& aggExState,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    std::vector<unique_ptr<PlanExecutor, PlanExecutor::Deleter>>& execs,
    rpc::ReplyBuilderInterface* result) {
    if (execs.size() == 1) {
        return executeSingleExecUntilFirstBatch(aggExState, expCtx, execs, result);
    }

    // We disallowed external data sources in queries with multiple plan executors due to a data
    // race (see SERVER-85453 for more details).
    tassert(8545301,
            "External data sources are not currently compatible with queries that use multiple "
            "plan executors.",
            aggExState.getExternalDataSourceScopeGuard() == nullptr);

    // If there is more than one executor, that means this query will be running on multiple
    // shards via exchange and merge. Such queries always require a cursor to be registered for
    // each PlanExecutor.
    std::vector<ClientCursorPin> pinnedCursors;
    for (auto&& exec : execs) {
        auto pinnedCursor = registerCursor(aggExState, expCtx, std::move(exec));
        pinnedCursors.emplace_back(std::move(pinnedCursor));
    }
    handleMultipleCursorsForExchange(aggExState, expCtx, pinnedCursors, result);

    if (pinnedCursors.size() > 0) {
        return std::move(pinnedCursors[0]);
    }

    return boost::none;
}

boost::intrusive_ptr<ExpressionContext> makeExpressionContext(
    const AggExState& aggExState,
    std::unique_ptr<CollatorInterface> collator,
    boost::optional<UUID> uuid,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault) {
    auto opCtx = aggExState.getOpCtx();
    auto expCtx =
        make_intrusive<ExpressionContext>(aggExState.getOpCtx(),
                                          aggExState.getRequest(),
                                          std::move(collator),
                                          MongoProcessInterface::create(opCtx),
                                          uassertStatusOK(aggExState.resolveInvolvedNamespaces()),
                                          uuid,
                                          CurOp::get(opCtx)->dbProfileLevel() > 0,
                                          allowDiskUseByDefault.load());
    expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";
    expCtx->collationMatchesDefault = collationMatchesDefault;
    return expCtx;
}

/**
 * If the aggregation 'request' contains an exchange specification, create a new pipeline for each
 * consumer and put it into the resulting vector. Otherwise, return the original 'pipeline' as a
 * single vector element.
 */
std::vector<std::unique_ptr<Pipeline, PipelineDeleter>> createExchangePipelinesIfNeeded(
    const AggExState& aggExState, std::unique_ptr<Pipeline, PipelineDeleter> pipeline) {
    std::vector<std::unique_ptr<Pipeline, PipelineDeleter>> pipelines;

    if (aggExState.getRequest().getExchange() && !pipeline->getContext()->explain) {
        auto expCtx = pipeline->getContext();
        // The Exchange constructor deregisters the pipeline from the context. Since we need a valid
        // opCtx for the makeExpressionContext call below, store the pointer ahead of the Exchange()
        // call.
        auto* opCtx = aggExState.getOpCtx();
        auto exchange = make_intrusive<Exchange>(aggExState.getRequest().getExchange().value(),
                                                 std::move(pipeline));

        for (size_t idx = 0; idx < exchange->getConsumers(); ++idx) {
            // For every new pipeline we have create a new ExpressionContext as the context
            // cannot be shared between threads. There is no synchronization for pieces of
            // the execution machinery above the Exchange, so nothing above the Exchange can be
            // shared between different exchange-producer cursors.
            expCtx = makeExpressionContext(aggExState,
                                           expCtx->getCollator() ? expCtx->getCollator()->clone()
                                                                 : nullptr,
                                           expCtx->uuid,
                                           expCtx->collationMatchesDefault);

            // Create a new pipeline for the consumer consisting of a single
            // DocumentSourceExchange.
            auto consumer = make_intrusive<DocumentSourceExchange>(
                expCtx,
                exchange,
                idx,
                // Assumes this is only called from the 'aggregate' or 'getMore' commands.  The code
                // which relies on this parameter does not distinguish/care about the difference so
                // we simply always pass 'aggregate'.
                ResourceYielderFactory::get(*opCtx->getService()).make(opCtx, "aggregate"_sd));
            pipelines.emplace_back(Pipeline::create({consumer}, expCtx));
        }
    } else {
        pipelines.emplace_back(std::move(pipeline));
    }

    return pipelines;
}

std::vector<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> createExecutor(
    const AggExState& aggExState,
    std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
    const MultipleCollectionAccessor& collections,
    CurOp* curOp,
    const std::function<void(void)>& resetContextFn) {
    const auto expCtx = pipeline->getContext();
    // Check if the pipeline has a $geoNear stage, as it will be ripped away during the build query
    // executor phase below (to be replaced with a $geoNearCursorStage later during the executor
    // attach phase).
    auto hasGeoNearStage = !pipeline->getSources().empty() &&
        dynamic_cast<DocumentSourceGeoNear*>(pipeline->peekFront());

    // Prepare a PlanExecutor to provide input into the pipeline, if needed; and additional
    // executors if needed to serve the aggregation, this currently only includes search commands
    // that generate metadata.
    auto [executor, attachCallback, additionalExecutors] = PipelineD::buildInnerQueryExecutor(
        collections, aggExState.getExecutionNss(), &aggExState.getRequest(), pipeline.get());

    std::vector<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> execs;
    if (canOptimizeAwayPipeline(aggExState, pipeline.get(), executor.get(), hasGeoNearStage)) {
        // This pipeline is currently empty, but once completed it will have only one source,
        // which is a DocumentSourceCursor. Instead of creating a whole pipeline to do nothing
        // more than forward the results of its cursor document source, we can use the
        // PlanExecutor by itself. The resulting cursor will look like what the client would
        // have gotten from find command.
        execs.emplace_back(std::move(executor));
    } else {
        // Complete creation of the initial $cursor stage, if needed.
        PipelineD::attachInnerQueryExecutorToPipeline(
            collections, attachCallback, std::move(executor), pipeline.get());

        std::vector<std::unique_ptr<Pipeline, PipelineDeleter>> pipelines;
        // Any pipeline that relies on calls to mongot requires additional setup.
        if (search_helpers::isMongotPipeline(pipeline.get())) {
            uassert(6253506,
                    "Cannot have exchange specified in a search pipeline",
                    !aggExState.getRequest().getExchange());

            // Release locks early, before we generate the search pipeline, so that we don't hold
            // them during network calls to mongot. This is fine for search pipelines since they are
            // not reading any local (lock-protected) data in the main pipeline.
            resetContextFn();
            pipelines.push_back(std::move(pipeline));

            // TODO SERVER-89546 extractDocsNeededBounds should be called internally within
            // DocumentSourceSearch optimization; that also means we'd be skipping that step when
            // optimization is off.
            auto bounds = extractDocsNeededBounds(*pipelines.back().get());
            auto metadataPipe = search_helpers::prepareSearchForTopLevelPipelineLegacyExecutor(
                expCtx,
                pipelines.back().get(),
                bounds,
                aggExState.getRequest().getCursor().getBatchSize());
            if (metadataPipe) {
                pipelines.push_back(std::move(metadataPipe));
            }
        } else {
            // Takes ownership of 'pipeline'.
            pipelines = createExchangePipelinesIfNeeded(aggExState, std::move(pipeline));
        }

        for (auto&& pipelineIt : pipelines) {
            // There are separate ExpressionContexts for each exchange pipeline, so make sure to
            // pass the pipeline's ExpressionContext to the plan executor factory.
            auto pipelineExpCtx = pipelineIt->getContext();
            execs.emplace_back(plan_executor_factory::make(
                std::move(pipelineExpCtx),
                std::move(pipelineIt),
                aggregation_request_helper::getResumableScanType(aggExState.getRequest(),
                                                                 aggExState.hasChangeStream())));
        }

        // With the pipelines created, we can relinquish locks as they will manage the locks
        // internally further on. We still need to keep the lock for an optimized away pipeline
        // though, as we will be changing its lock policy to 'kLockExternally' (see details
        // below), and in order to execute the initial getNext() call in 'handleCursorCommand',
        // we need to hold the collection lock.
        resetContextFn();
    }

    for (auto& exec : additionalExecutors) {
        execs.emplace_back(std::move(exec));
    }
    return execs;
}

/**
 * Executes the aggregation 'request' given a properly constructed AggregationExecutionState,
 * which holds the request and all necessary supporting context to execute request.
 *
 * If the query over a view that's already been resolved, the appropriate state should
 * have already been set in AggregationExecutionState.
 *
 * On success, fills out 'result' with the command response.
 */
Status _runAggregate(AggExState& aggExState, rpc::ReplyBuilderInterface* result);

// TODO SERVER-93539 take a ResolvedViewAggExState instead of a AggExState that gets set as a view.
Status runAggregateOnView(AggExState& aggExState,
                          const MultipleCollectionAccessor& collections,
                          boost::optional<std::unique_ptr<CollatorInterface>> collatorToUse,
                          const ViewDefinition* view,
                          std::shared_ptr<const CollectionCatalog> catalog,
                          rpc::ReplyBuilderInterface* result,
                          const std::function<void(void)>& resetContextFn) {
    uassert(ErrorCodes::CommandNotSupportedOnView,
            "mapReduce on a view is not supported",
            !aggExState.getRequest().getIsMapReduceCommand());

    // Check that the default collation of 'view' is compatible with the operation's
    // collation. The check is skipped if the request did not specify a collation.
    if (!aggExState.getRequest().getCollation().get_value_or(BSONObj()).isEmpty()) {
        invariant(collatorToUse);  // Should already be resolved at this point.
        if (!CollatorInterface::collatorsMatch(view->defaultCollator(), collatorToUse->get()) &&
            !view->timeseries()) {

            return {ErrorCodes::OptionNotSupportedOnView,
                    "Cannot override a view's default collation"};
        }
    }

    aggExState.setView(catalog, view);
    // Resolved view will be available after view has been set on AggregationExecutionState
    auto resolvedView = aggExState.getResolvedView().value();

    // With the view & collation resolved, we can relinquish locks.
    resetContextFn();

    auto status{Status::OK()};
    if (!OperationShardingState::get(aggExState.getOpCtx())
             .shouldBeTreatedAsFromRouter(aggExState.getOpCtx())) {
        // Non sharding-aware operation.
        // Run the translated query on the view on this node.
        status = _runAggregate(aggExState, result);
    } else {
        // Sharding-aware operation.

        // Stash the shard role for the resolved view nss, in case it was set, as we are about to
        // transition into the router role for it.
        const ScopedStashShardRole scopedUnsetShardRole{aggExState.getOpCtx(),
                                                        resolvedView.getNamespace()};

        sharding::router::CollectionRouter router(aggExState.getOpCtx()->getServiceContext(),
                                                  resolvedView.getNamespace(),
                                                  false  // retryOnStaleShard=false
        );
        status = router.route(
            aggExState.getOpCtx(),
            "runAggregateOnView",
            [&](OperationContext* opCtx, const CollectionRoutingInfo& cri) {
                // TODO: SERVER-77402 Use a ShardRoleLoop here and remove this usage of
                // CollectionRouter's retryOnStaleShard=false.

                // Setup the opCtx's OperationShardingState with the expected placement versions for
                // the underlying collection. Use the same 'placementConflictTime' from the original
                // request, if present.
                const auto scopedShardRole = aggExState.setShardRole(cri);

                // If the underlying collection is unsharded and is located on this shard, then we
                // can execute the view aggregation locally. Otherwise, we need to kick-back to the
                // router.
                if (!aggExState.canReadUnderlyingCollectionLocally(cri)) {
                    // Cannot execute the resolved aggregation locally. The router must do it.
                    //
                    // Before throwing the kick-back exception, validate the routing table
                    // we are basing this decision on. We do so by briefly entering into
                    // the shard-role by acquiring the underlying collection.
                    const auto underlyingColl = acquireCollectionMaybeLockFree(
                        opCtx,
                        CollectionAcquisitionRequest::fromOpCtx(
                            opCtx,
                            resolvedView.getNamespace(),
                            AcquisitionPrerequisites::OperationType::kRead));

                    // Throw the kick-back exception.
                    uasserted(std::move(resolvedView),
                              "Resolved views on collections that do not exclusively live on the "
                              "db-primary shard must be executed by mongos");
                }

                // Run the resolved aggregation locally.
                return _runAggregate(aggExState, result);
            });
    }

    {
        // Set the namespace of the curop back to the view namespace so ctx records
        // stats on this view namespace on destruction.
        stdx::lock_guard<Client> lk(*aggExState.getOpCtx()->getClient());
        CurOp::get(aggExState.getOpCtx())->setNS(lk, aggExState.getOriginalNss());
    }

    return status;
}

/**
 * Determines the collection type of the query by precedence of various configurations. The order
 * of these checks is critical since there may be overlap (e.g., a view over a virtual collection
 * is classified as a view).
 */
query_shape::CollectionType determineCollectionType(
    const AggExState& aggExState,
    const boost::optional<AutoGetCollectionForReadCommandMaybeLockFree>& ctx,
    bool isCollectionless) {
    if (aggExState.getResolvedView().has_value()) {
        if (aggExState.getResolvedView()->timeseries()) {
            return query_shape::CollectionType::kTimeseries;
        }
        return query_shape::CollectionType::kView;
    }
    if (isCollectionless) {
        return query_shape::CollectionType::kVirtual;
    }
    if (aggExState.hasChangeStream()) {
        return query_shape::CollectionType::kChangeStream;
    }
    return ctx ? ctx->getCollectionType() : query_shape::CollectionType::kUnknown;
}

std::unique_ptr<Pipeline, PipelineDeleter> parsePipelineAndRegisterQueryStats(
    const AggExState& aggExState,
    const boost::optional<AutoGetCollectionForReadCommandMaybeLockFree>& ctx,
    std::unique_ptr<CollatorInterface> collator,
    boost::optional<UUID> uuid,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault,
    const MultipleCollectionAccessor& collections,
    bool isCollectionless) {
    // If we're operating over a view, we first parse just the original user-given request
    // for the sake of registering query stats. Then, we'll parse the view pipeline and stitch
    // the two pipelines together below.
    auto expCtx =
        makeExpressionContext(aggExState, std::move(collator), uuid, collationMatchesDefault);

    // If any involved collection contains extended-range data, set a flag which individual
    // DocumentSource parsers can check.
    collections.forEach([&](const CollectionPtr& coll) {
        if (coll->getRequiresTimeseriesExtendedRangeSupport())
            expCtx->setRequiresTimeseriesExtendedRangeSupport(true);
    });

    auto requestForQueryStats = aggExState.getOriginalRequest();
    expCtx->startExpressionCounters();
    auto pipeline = Pipeline::parse(requestForQueryStats.getPipeline(), expCtx);
    expCtx->stopExpressionCounters();

    // Register query stats with the pre-optimized pipeline. Exclude queries against collections
    // with encrypted fields. We still collect query stats on collection-less aggregations.
    bool hasEncryptedFields = ctx && ctx->getCollection() &&
        ctx->getCollection()->getCollectionOptions().encryptedFieldConfig;
    if (!hasEncryptedFields) {
        // If this is a query over a resolved view, we want to register query stats with the
        // original user-given request and pipeline, rather than the new request generated when
        // resolving the view.
        auto collectionType = determineCollectionType(aggExState, ctx, isCollectionless);
        NamespaceStringSet pipelineInvolvedNamespaces(aggExState.getInvolvedNamespaces());
        query_stats::registerRequest(
            aggExState.getOpCtx(),
            aggExState.getOriginalNss(),
            [&]() {
                return std::make_unique<query_stats::AggKey>(requestForQueryStats,
                                                             *pipeline,
                                                             expCtx,
                                                             std::move(pipelineInvolvedNamespaces),
                                                             aggExState.getOriginalNss(),
                                                             collectionType);
            },
            aggExState.hasChangeStream());

        if (aggExState.getRequest().getIncludeQueryStatsMetrics() &&
            feature_flags::gFeatureFlagQueryStatsDataBearingNodes.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            CurOp::get(aggExState.getOpCtx())->debug().queryStatsInfo.metricsRequested = true;
        }
    }

    // Lookup the query settings and attach it to the 'expCtx'.
    // TODO: SERVER-73632 Remove feature flag for PM-635.
    // Query settings will only be looked up on mongos and therefore should be part of command
    // body on mongod if present.
    expCtx->setQuerySettingsIfNotPresent(
        query_settings::lookupQuerySettingsForAgg(expCtx,
                                                  requestForQueryStats,
                                                  *pipeline,
                                                  aggExState.getInvolvedNamespaces(),
                                                  aggExState.getOriginalNss()));

    if (aggExState.getResolvedView().has_value()) {
        expCtx->startExpressionCounters();
        pipeline = handleViewHelper(aggExState, expCtx, std::move(pipeline), uuid);
        expCtx->stopExpressionCounters();
    }

    expCtx->initializeReferencedSystemVariables();

    return pipeline;
}

Status _runAggregate(AggExState& aggExState, rpc::ReplyBuilderInterface* result) {
    // Perform the validation checks on the request and its derivatives before proceeding.
    aggExState.performValidationChecks();

    // If we are running a retryable write without shard key, check if the write was applied on this
    // shard, and if so, return early with an empty cursor with $_wasStatementExecuted
    // set to true. The isRetryableWrite() check here is to check that the client executed write was
    // a retryable write (which would've spawned an internal session for a retryable write to
    // execute the two phase write without shard key protocol), otherwise we skip the retryable
    // write check.
    auto isClusterQueryWithoutShardKeyCmd =
        aggExState.getRequest().getIsClusterQueryWithoutShardKeyCmd();
    if (aggExState.getOpCtx()->isRetryableWrite() && isClusterQueryWithoutShardKeyCmd) {
        auto stmtId = aggExState.getRequest().getStmtId();
        tassert(7058100, "StmtId must be set for a retryable write without shard key", stmtId);
        if (TransactionParticipant::get(aggExState.getOpCtx())
                .checkStatementExecuted(aggExState.getOpCtx(), *stmtId)) {
            CursorResponseBuilder::Options options;
            options.isInitialResponse = true;
            CursorResponseBuilder responseBuilder(result, options);
            boost::optional<CursorMetrics> metrics =
                aggExState.getRequest().getIncludeQueryStatsMetrics()
                ? boost::make_optional(CursorMetrics{})
                : boost::none;
            responseBuilder.setWasStatementExecuted(true);
            responseBuilder.done(0LL,
                                 aggExState.getOriginalNss(),
                                 std::move(metrics),
                                 SerializationContext::stateCommandReply(
                                     aggExState.getRequest().getSerializationContext()));
            return Status::OK();
        }
    }

    // Determine if this aggregation has foreign collections that the execution subsystem needs
    // to be aware of.
    std::vector<NamespaceStringOrUUID> secondaryExecNssList =
        aggExState.getForeignExecutionNamespaces();

    // The collation to use for this aggregation. boost::optional to distinguish between the case
    // where the collation has not yet been resolved, and where it has been resolved to nullptr.
    boost::optional<std::unique_ptr<CollatorInterface>> collatorToUse;
    ExpressionContext::CollationMatchesDefault collatorToUseMatchesDefault;

    // The UUID of the collection for the execution namespace of this aggregation.
    boost::optional<UUID> uuid;

    // If emplaced, AutoGetCollectionForReadCommand will throw if the sharding version for this
    // connection is out of date. If the namespace is a view, the lock will be released before
    // re-running the expanded aggregation.
    boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> ctx;
    MultipleCollectionAccessor collections;

    // Going forward this operation must never ignore interrupt signals while waiting for lock
    // acquisition. This InterruptibleLockGuard will ensure that waiting for lock re-acquisition
    // after yielding will not ignore interrupt signals. This is necessary to avoid deadlocking with
    // replication rollback, which at the storage layer waits for all cursors to be closed under the
    // global MODE_X lock, after having sent interrupt signals to read operations. This operation
    // must never hold open storage cursors while ignoring interrupt.
    InterruptibleLockGuard interruptibleLockAcquisition(aggExState.getOpCtx());

    auto catalog = CollectionCatalog::latest(aggExState.getOpCtx());

    hangAfterAcquiringCollectionCatalog.executeIf(
        [&](const auto&) { hangAfterAcquiringCollectionCatalog.pauseWhileSet(); },
        [&](const BSONObj& data) {
            return aggExState.getExecutionNss().coll() == data["collection"].valueStringData();
        });

    auto initContext = [&](auto_get_collection::ViewMode m) {
        auto initAutoGetCallback = [&]() {
            ctx.emplace(aggExState.getOpCtx(),
                        aggExState.getExecutionNss(),
                        AutoGetCollection::Options{}.viewMode(m).secondaryNssOrUUIDs(
                            secondaryExecNssList.cbegin(), secondaryExecNssList.cend()),
                        AutoStatsTracker::LogMode::kUpdateTopAndCurOp);
        };
        bool anySecondaryCollectionNotLocal = intializeAutoGet(aggExState.getOpCtx(),
                                                               aggExState.getExecutionNss(),
                                                               secondaryExecNssList,
                                                               initAutoGetCallback);
        tassert(8322000,
                "Should have initialized AutoGet* after calling 'initializeAutoGet'",
                ctx.has_value());
        collections = MultipleCollectionAccessor(aggExState.getOpCtx(),
                                                 &ctx->getCollection(),
                                                 ctx->getNss(),
                                                 ctx->isAnySecondaryNamespaceAView() ||
                                                     anySecondaryCollectionNotLocal,
                                                 secondaryExecNssList);

        // Return the catalog that gets implicitly stashed during the collection acquisition
        // above, which also implicitly opened a storage snapshot. This catalog object can
        // be potentially different than the one obtained before and will be in sync with
        // the opened snapshot.
        return CollectionCatalog::get(aggExState.getOpCtx());
    };

    auto resetContext = [&]() -> void {
        ctx.reset();
        collections.clear();
    };

    std::vector<unique_ptr<PlanExecutor, PlanExecutor::Deleter>> execs;
    boost::intrusive_ptr<ExpressionContext> expCtx;
    auto curOp = CurOp::get(aggExState.getOpCtx());

    {
        const auto& pipelineInvolvedNamespaces = aggExState.getInvolvedNamespaces();

        // If this is a collectionless aggregation, we won't create 'ctx' but will still need an
        // AutoStatsTracker to record CurOp and Top entries.
        boost::optional<AutoStatsTracker> statsTracker;

        // The aggregation can be on / from one of three mutally exclusive collection spaces:
        // (1) The oplog (for change streams)
        // (2) Originating from no collection(s)
        // (3) From standard user defined collections(s)
        if (aggExState.hasChangeStream()) {
            // If this is a change stream, perform special checks and change the execution
            // namespace.
            uassert(4928900,
                    str::stream() << AggregateCommandRequest::kCollectionUUIDFieldName
                                  << " is not supported for a change stream",
                    !aggExState.getRequest().getCollectionUUID());

            // Replace the execution namespace with the oplog.
            aggExState.setExecutionNss(NamespaceString::kRsOplogNamespace);

            // In case of serverless the change stream will be opened on the change collection.
            const bool isServerless = change_stream_serverless_helpers::isServerlessEnvironment();
            if (isServerless) {
                const auto tenantId = change_stream_serverless_helpers::resolveTenantId(
                    aggExState.getOriginalNss().tenantId());

                uassert(ErrorCodes::BadValue,
                        "Change streams cannot be used without tenant id",
                        tenantId);
                aggExState.setExecutionNss(NamespaceString::makeChangeCollectionNSS(tenantId));
            }

            // Assert that a change stream on the config server is always opened on the oplog.
            tassert(6763400,
                    str::stream() << "Change stream was unexpectedly opened on the namespace: "
                                  << aggExState.getExecutionNss().toStringForErrorMsg()
                                  << " in the config server",
                    !serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer) ||
                        aggExState.getExecutionNss().isOplog());

            // Upgrade and wait for read concern if necessary.
            aggExState.adjustChangeStreamReadConcern();

            // Obtain collection locks on the execution namespace; that is, the oplog.
            catalog = initContext(auto_get_collection::ViewMode::kViewsForbidden);

            // Raise an error if original nss is a view. We do not need to check this if we are
            // opening a stream on an entire db or across the cluster.
            if (!aggExState.getOriginalNss().isCollectionlessAggregateNS()) {
                auto view = catalog->lookupView(aggExState.getOpCtx(), aggExState.getOriginalNss());
                uassert(ErrorCodes::CommandNotSupportedOnView,
                        str::stream() << "Cannot run aggregation on timeseries with namespace "
                                      << aggExState.getOriginalNss().toStringForErrorMsg(),
                        !view || !view->timeseries());
                uassert(ErrorCodes::CommandNotSupportedOnView,
                        str::stream()
                            << "Namespace " << aggExState.getOriginalNss().toStringForErrorMsg()
                            << " is a view, not a collection",
                        !view);
            }

            // If the user specified an explicit collation, adopt it; otherwise, use the simple
            // collation. We do not inherit the collection's default collation or UUID, since
            // the stream may be resuming from a point before the current UUID existed.
            auto [collator, match] =
                resolveCollator(aggExState.getOpCtx(),
                                aggExState.getRequest().getCollation().get_value_or(BSONObj()),
                                CollectionPtr());
            collatorToUse.emplace(std::move(collator));
            collatorToUseMatchesDefault = match;

            uassert(ErrorCodes::ChangeStreamNotEnabled,
                    "Change streams must be enabled before being used",
                    !isServerless ||
                        change_stream_serverless_helpers::isChangeStreamEnabled(
                            aggExState.getOpCtx(), *aggExState.getExecutionNss().tenantId()));
        } else if (aggExState.getExecutionNss().isCollectionlessAggregateNS() &&
                   pipelineInvolvedNamespaces.empty()) {
            uassert(4928901,
                    str::stream() << AggregateCommandRequest::kCollectionUUIDFieldName
                                  << " is not supported for a collectionless aggregation",
                    !aggExState.getRequest().getCollectionUUID());

            // If this is a collectionless agg with no foreign namespaces, don't acquire any locks.
            statsTracker.emplace(
                aggExState.getOpCtx(),
                aggExState.getExecutionNss(),
                Top::LockType::NotLocked,
                AutoStatsTracker::LogMode::kUpdateTopAndCurOp,
                DatabaseProfileSettings::get(aggExState.getOpCtx()->getServiceContext())
                    .getDatabaseProfileLevel(aggExState.getExecutionNss().dbName()));
            auto [collator, match] =
                resolveCollator(aggExState.getOpCtx(),
                                aggExState.getRequest().getCollation().get_value_or(BSONObj()),
                                CollectionPtr());
            collatorToUse.emplace(std::move(collator));
            collatorToUseMatchesDefault = match;
            tassert(6235101,
                    "A collection-less aggregate should not take any locks",
                    ctx == boost::none);
        } else {
            // This is a regular aggregation. Lock the collection or view.
            catalog = initContext(auto_get_collection::ViewMode::kViewsPermitted);
            auto [collator, match] =
                resolveCollator(aggExState.getOpCtx(),
                                aggExState.getRequest().getCollation().get_value_or(BSONObj()),
                                collections.getMainCollection());
            collatorToUse.emplace(std::move(collator));
            collatorToUseMatchesDefault = match;
            if (collections.hasMainCollection()) {
                uuid = collections.getMainCollection()->uuid();
            }
        }
        if (aggExState.getRequest().getResumeAfter()) {
            uassert(ErrorCodes::InvalidPipelineOperator,
                    "$_resumeAfter is not supported on view",
                    !ctx->getView());
            const auto& collection = ctx->getCollection();
            const bool isClusteredCollection = collection && collection->isClustered();
            uassertStatusOK(
                query_request_helper::validateResumeAfter(aggExState.getOpCtx(),
                                                          *aggExState.getRequest().getResumeAfter(),
                                                          isClusteredCollection));
        }

        // If collectionUUID was provided, verify the collection exists and has the expected UUID.
        checkCollectionUUIDMismatch(aggExState.getOpCtx(),
                                    aggExState.getExecutionNss(),
                                    collections.getMainCollection(),
                                    aggExState.getRequest().getCollectionUUID());

        // If this is a view, resolve it by finding the underlying collection and stitching view
        // pipelines and this request's pipeline together. We then release our locks before
        // recursively calling runAggregate(), which will re-acquire locks on the underlying
        // collection.  (The lock must be released because recursively acquiring locks on the
        // database will prohibit yielding.)
        // We do not need to expand the view pipeline when there is a $collStats stage, as
        // $collStats is supported on a view namespace. For a time-series collection, however, the
        // view is abstracted out for the users, so we needed to resolve the namespace to get the
        // underlying bucket collection.
        if (ctx && ctx->getView() &&
            (!aggExState.startsWithCollStats() || ctx->getView()->timeseries())) {
            return runAggregateOnView(aggExState,
                                      collections,
                                      std::move(collatorToUse),
                                      ctx->getView(),
                                      catalog,
                                      result,
                                      resetContext);
        }

        invariant(collatorToUse);

        std::unique_ptr<Pipeline, PipelineDeleter> pipeline = parsePipelineAndRegisterQueryStats(
            aggExState,
            ctx,
            std::move(*collatorToUse),
            uuid,
            collatorToUseMatchesDefault,
            collections,
            aggExState.getExecutionNss().isCollectionlessAggregateNS());
        expCtx = pipeline->getContext();

        // Start the query planning timer right after parsing.
        CurOp::get(aggExState.getOpCtx())->beginQueryPlanningTimer();

        if (expCtx->hasServerSideJs.accumulator && _samplerAccumulatorJs.tick()) {
            LOGV2_WARNING(
                8996502,
                "$accumulator is deprecated. For more information, see "
                "https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/");
        }

        if (expCtx->hasServerSideJs.function && _samplerFunctionJs.tick()) {
            LOGV2_WARNING(
                8996503,
                "$function is deprecated. For more information, see "
                "https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/");
        }

        // Only allow the use of runtime constants when 'fromRouter' is true.
        uassert(463840,
                "Manually setting 'runtimeConstants' is not supported. Use 'let' for user-defined "
                "constants.",
                expCtx->fromRouter || !aggExState.getRequest().getLegacyRuntimeConstants());

        // This prevents opening a new change stream in the critical section of a serverless shard
        // split or merge operation to prevent resuming on the recipient with a resume token higher
        // than that operation's blockTimestamp.
        //
        // If we do this check before picking a startTime for a change stream then the primary could
        // go into a blocking state between the check and getting the timestamp resulting in a
        // startTime greater than blockTimestamp. Therefore we must do this check here, after the
        // pipeline has been parsed and startTime has been initialized.
        if (aggExState.hasChangeStream()) {
            tenant_migration_access_blocker::assertCanOpenChangeStream(
                aggExState.getOpCtx(), aggExState.getExecutionNss().dbName());
        }

        if (!aggExState.getRequest().getAllowDiskUse().value_or(true)) {
            allowDiskUseFalseCounter.increment();
        }

        // Check that the view's collation matches the collation of any views involved in the
        // pipeline.
        if (!pipelineInvolvedNamespaces.empty()) {
            auto pipelineCollationStatus =
                aggExState.collatorCompatibleWithPipeline(expCtx->getCollator());
            if (!pipelineCollationStatus.isOK()) {
                return pipelineCollationStatus;
            }
        }

        // If the aggregate command supports encrypted collections, do rewrites of the pipeline to
        // support querying against encrypted fields.
        if (shouldDoFLERewrite(aggExState.getRequest())) {
            {
                stdx::lock_guard<Client> lk(*aggExState.getOpCtx()->getClient());
                CurOp::get(aggExState.getOpCtx())->setShouldOmitDiagnosticInformation(lk, true);
            }

            if (!aggExState.getRequest().getEncryptionInformation()->getCrudProcessed().value_or(
                    false)) {
                pipeline =
                    processFLEPipelineD(aggExState.getOpCtx(),
                                        aggExState.getExecutionNss(),
                                        aggExState.getRequest().getEncryptionInformation().value(),
                                        std::move(pipeline));
                aggExState.getRequest().getEncryptionInformation()->setCrudProcessed(true);
            }
        }

        pipeline->optimizePipeline();

        constexpr bool alreadyOptimized = true;
        pipeline->validateCommon(alreadyOptimized);

        if (auto sampleId = analyze_shard_key::getOrGenerateSampleId(
                aggExState.getOpCtx(),
                expCtx->ns,
                analyze_shard_key::SampledCommandNameEnum::kAggregate,
                aggExState.getRequest())) {
            analyze_shard_key::QueryAnalysisWriter::get(aggExState.getOpCtx())
                ->addAggregateQuery(*sampleId,
                                    expCtx->ns,
                                    pipeline->getInitialQuery(),
                                    expCtx->getCollatorBSON(),
                                    aggExState.getRequest().getLet())
                .getAsync([](auto) {});
        }

        execs = createExecutor(aggExState, std::move(pipeline), collections, curOp, resetContext);

        tassert(6624353, "No executors", !execs.empty());

        {
            auto planSummary = execs[0]->getPlanExplainer().getPlanSummary();
            stdx::lock_guard<Client> lk(*aggExState.getOpCtx()->getClient());
            curOp->setPlanSummary(lk, std::move(planSummary));
            curOp->debug().queryFramework = execs[0]->getQueryFramework();
        }
    }

    // Having released the collection lock, we can now begin to fetch results from the pipeline. If
    // the documents in the result exceed the batch size, a cursor will be created. This cursor owns
    // no collection state, and thus we register it with the global cursor manager. The global
    // cursor manager does not deliver invalidations or kill notifications; the underlying
    // PlanExecutor(s) used by the pipeline will be receiving invalidations and kill notifications
    // themselves, not the cursor we create here.
    hangAfterCreatingAggregationPlan.executeIf(
        [](const auto&) { hangAfterCreatingAggregationPlan.pauseWhileSet(); },
        [&](const BSONObj& data) { return uuid && UUID::parse(data["uuid"]) == *uuid; });
    // Report usage statistics for each stage in the pipeline.
    aggExState.tickGlobalStageCounters();

    // If both explain and cursor are specified, explain wins.
    if (expCtx->explain) {
        auto explainExecutor = execs[0].get();
        auto bodyBuilder = result->getBodyBuilder();
        if (auto pipelineExec = dynamic_cast<PlanExecutorPipeline*>(explainExecutor)) {
            Explain::explainPipeline(pipelineExec,
                                     true /* executePipeline */,
                                     *(expCtx->explain),
                                     *aggExState.getDeferredCmd(),
                                     &bodyBuilder);
        } else {
            invariant(explainExecutor->getOpCtx() == aggExState.getOpCtx());
            // The explainStages() function for a non-pipeline executor may need to execute the plan
            // to collect statistics. If the PlanExecutor uses kLockExternally policy, the
            // appropriate collection lock must be already held. Make sure it has not been released
            // yet.
            invariant(ctx);
            Explain::explainStages(explainExecutor,
                                   collections,
                                   *(expCtx->explain),
                                   BSON("optimizedPipeline" << true),
                                   SerializationContext::stateCommandReply(
                                       aggExState.getRequest().getSerializationContext()),
                                   *aggExState.getDeferredCmd(),
                                   &bodyBuilder);
        }
        collectQueryStatsMongod(
            aggExState.getOpCtx(), expCtx, std::move(curOp->debug().queryStatsInfo.key));
    } else {
        auto maybePinnedCursor = executeUntilFirstBatch(aggExState, expCtx, execs, result);

        // For an optimized away pipeline, signal the cache that a query operation has completed.
        // For normal pipelines this is done in DocumentSourceCursor.
        if (ctx) {
            auto exec =
                maybePinnedCursor ? maybePinnedCursor->getCursor()->getExecutor() : execs[0].get();
            const auto& planExplainer = exec->getPlanExplainer();
            if (const auto& coll = ctx->getCollection()) {
                CollectionQueryInfo::get(coll).notifyOfQuery(coll, curOp->debug());
            }
            // For SBE pushed down pipelines, we may need to report stats saved for secondary
            // collections separately.
            for (const auto& [secondaryNss, coll] : collections.getSecondaryCollections()) {
                if (coll) {
                    PlanSummaryStats secondaryStats;
                    planExplainer.getSecondarySummaryStats(secondaryNss, &secondaryStats);
                    CollectionQueryInfo::get(coll).notifyOfQuery(
                        aggExState.getOpCtx(), coll, secondaryStats);
                }
            }
        }
    }

    // The aggregation pipeline may change the namespace of the curop and we need to set it back to
    // the original namespace to correctly report command stats. One example when the namespace can
    // be changed is when the pipeline contains an $out stage, which executes an internal command to
    // create a temp collection, changing the curop namespace to the name of this temp collection.
    {
        stdx::lock_guard<Client> lk(*aggExState.getOpCtx()->getClient());
        curOp->setNS(lk, aggExState.getOriginalNss());
    }

    return Status::OK();
}

}  // namespace

// TODO SERVER-93536 take these variables in by rvalue to take internal ownership of them.
Status runAggregate(
    OperationContext* opCtx,
    AggregateCommandRequest& request,
    const LiteParsedPipeline& liteParsedPipeline,
    const BSONObj& cmdObj,
    const PrivilegeVector& privileges,
    rpc::ReplyBuilderInterface* result,
    const std::vector<std::pair<NamespaceString, std::vector<ExternalDataSourceInfo>>>&
        usedExternalDataSources) {

    AggExState aggExState(
        opCtx, request, liteParsedPipeline, cmdObj, privileges, usedExternalDataSources);

    return _runAggregate(aggExState, result);
}
}  // namespace mongo
