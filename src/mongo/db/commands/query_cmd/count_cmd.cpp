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

#include <boost/smart_ptr.hpp>
#include <functional>
#include <limits>
#include <string>
#include <utility>
#include <variant>

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/read_preference.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/auth/authorization_checks.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/validated_tenancy_scope_factory.h"
#include "mongo/db/basic_types.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/query_cmd/run_aggregate.h"
#include "mongo/db/curop.h"
#include "mongo/db/curop_failpoint_helpers.h"
#include "mongo/db/database_name.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/feature_flag.h"
#include "mongo/db/fle_crud.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/query/client_cursor/collect_query_stats_mongod.h"
#include "mongo/db/query/collection_query_info.h"
#include "mongo/db/query/command_diagnostic_printer.h"
#include "mongo/db/query/count_command_as_aggregation_command.h"
#include "mongo/db/query/count_command_gen.h"
#include "mongo/db/query/explain.h"
#include "mongo/db/query/explain_options.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_explainer.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/query/query_settings/query_settings_gen.h"
#include "mongo/db/query/query_stats/count_key.h"
#include "mongo/db/query/query_stats/query_stats.h"
#include "mongo/db/query/view_response_formatter.h"
#include "mongo/db/read_concern_support_result.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/query_analysis_writer.h"
#include "mongo/db/s/scoped_collection_metadata.h"
#include "mongo/db/service_context.h"
#include "mongo/db/tenant_id.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/rpc/reply_builder_interface.h"
#include "mongo/s/analyze_shard_key_common_gen.h"
#include "mongo/s/query_analysis_sampler_util.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/future.h"
#include "mongo/util/serialization_context.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {

std::unique_ptr<ExtensionsCallback> getExtensionsCallback(const CollectionPtr& collection,
                                                          OperationContext* opCtx,
                                                          const NamespaceString& nss) {
    if (collection) {
        return std::make_unique<ExtensionsCallbackReal>(ExtensionsCallbackReal(opCtx, &nss));
    }
    return std::make_unique<ExtensionsCallbackNoop>(ExtensionsCallbackNoop());
}

// The # of documents returned is always 1 for the count command.
static constexpr long long kNReturned = 1;

// Failpoint which causes to hang "count" cmd after acquiring the DB lock.
MONGO_FAIL_POINT_DEFINE(hangBeforeCollectionCount);

/**
 * Implements the MongoD side of the count command.
 */
class CmdCount : public CountCmdVersion1Gen<CmdCount> {
public:
    std::string help() const override {
        return "count objects in collection";
    }

    class Invocation final : public InvocationBaseGen {
    public:
        using InvocationBaseGen::InvocationBaseGen;

        Invocation(OperationContext* opCtx,
                   const Command* command,
                   const OpMsgRequest& opMsgRequest)
            : InvocationBaseGen(opCtx, command, opMsgRequest),
              _ns(request().getNamespaceOrUUID().isNamespaceString()
                      ? request().getNamespaceOrUUID().nss()
                      : CollectionCatalog::get(opCtx)->resolveNamespaceStringFromDBNameAndUUID(
                            opCtx,
                            request().getNamespaceOrUUID().dbName(),
                            request().getNamespaceOrUUID().uuid())) {
            uassert(ErrorCodes::InvalidNamespace,
                    str::stream() << "Invalid namespace specified '" << _ns.toStringForErrorMsg()
                                  << "'",
                    _ns.isValid());
        }

        bool supportsWriteConcern() const final {
            return false;
        }

        bool isSubjectToIngressAdmissionControl() const override {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            AuthorizationSession* authSession = AuthorizationSession::get(opCtx->getClient());
            const auto& req = request();
            NamespaceStringOrUUID const& nsOrUUID = req.getNamespaceOrUUID();
            uassert(ErrorCodes::Unauthorized,
                    "Unauthorized",
                    authSession->isAuthorizedToParseNamespaceElement(nsOrUUID));

            constexpr auto hasTerm = false;
            uassertStatusOK(auth::checkAuthForFind(authSession, _ns, hasTerm));
        }

        void explain(OperationContext* opCtx,
                     ExplainOptions::Verbosity verbosity,
                     rpc::ReplyBuilderInterface* replyBuilder) override {
            // Using explain + count + UUID is not supported here so that there is "feature parity"
            // with mongos, which also does not support using a UUID for count + explain.
            uassert(ErrorCodes::InvalidNamespace,
                    str::stream() << "Collection name must be provided. UUID is not valid in this "
                                  << "context",
                    !request().getNamespaceOrUUID().isUUID());

            // This optional will contain the request object in case it was modified by the
            // prepareRequest function. We need this so that the request object remains valid during
            // the lifetime of this function. This approach avoids making a copy of the request
            // object in case no FLE is used. As a future refactoring, this should be generalized
            // and moved into the base class(es), so that other commands can make use of the same
            // mechanism.
            // TODO(SERVER-94834): clean this up when centralizing processing for FLE in the
            // TypedCommand base class.
            boost::optional<CountCommandRequest> potentiallyRewrittenReq;
            auto req = prepareRequest(opCtx, potentiallyRewrittenReq);

            // Acquire locks. The RAII object is optional, because in the case of a view, the locks
            // need to be released.
            boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> ctx;
            ctx.emplace(opCtx,
                        _ns,
                        AutoGetCollection::Options{}.viewMode(
                            auto_get_collection::ViewMode::kViewsPermitted));

            // Start the query planning timer.
            CurOp::get(opCtx)->beginQueryPlanningTimer();

            if (ctx->getView()) {
                // Relinquish locks. The aggregation command will re-acquire them.
                ctx.reset();
                return runExplainOnView(opCtx, req.get(), verbosity, replyBuilder);
            }

            const auto& collection = ctx->getCollection();

            // RAII object that prevents chunks from being cleaned on sharded collections.
            auto rangePreverser = buildRangePreserverForShardedCollections(opCtx, collection);

            auto expCtx = makeExpressionContextForGetExecutor(
                opCtx, req.get().getCollation().value_or(BSONObj()), _ns, verbosity);
            const auto extensionsCallback = getExtensionsCallback(collection, opCtx, _ns);
            auto parsedFind = uassertStatusOK(
                parsed_find_command::parseFromCount(expCtx, req.get(), *extensionsCallback, _ns));

            auto statusWithPlanExecutor =
                getExecutorCount(expCtx, &collection, std::move(parsedFind), req.get());
            uassertStatusOK(statusWithPlanExecutor.getStatus());

            auto exec = std::move(statusWithPlanExecutor.getValue());
            auto bodyBuilder = replyBuilder->getBodyBuilder();
            Explain::explainStages(
                exec.get(),
                collection,
                verbosity,
                BSONObj(),
                SerializationContext::stateCommandReply(req.get().getSerializationContext()),
                req.get().toBSON(),
                &bodyBuilder);
        }

        NamespaceString ns() const final {
            // Guaranteed to be valid.
            return _ns;
        }

        CountCommandReply typedRun(OperationContext* opCtx) final {
            CommandHelpers::handleMarkKillOnClientDisconnect(opCtx);

            // Capture diagnostics for tassert and invariant failures that may occur during query
            // parsing, planning or execution. No work is done on the hot-path, all computation of
            // these diagnostics is done lazily during failure handling. This line just creates an
            // RAII object which holds references to objects on this stack frame, which will be used
            // to print diagnostics in the event of a tassert or invariant.
            ScopedDebugInfo countCmdDiagnostics("commandDiagnostics",
                                                command_diagnostics::Printer{opCtx});

            // This optional will contain the request object in case it was modified by the
            // prepareRequest function. We need this so that the request object remains valid during
            // the lifetime of this function. This approach avoids making a copy of the request
            // object in case no FLE is used. As a future refactoring, this should be generalized
            // and moved into the base class(es), so that other commands can make use of the same
            // mechanism.
            // TODO(SERVER-94834): clean this up when centralizing processing for FLE in the
            // TypedCommand base class.
            boost::optional<CountCommandRequest> potentiallyRewrittenReq;
            auto req = prepareRequest(opCtx, potentiallyRewrittenReq);

            // Acquire locks. The RAII object is optional, because in the case of a view, the locks
            // need to be released.
            boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> ctx;
            ctx.emplace(opCtx,
                        _ns,
                        AutoGetCollection::Options{}.viewMode(
                            auto_get_collection::ViewMode::kViewsPermitted));

            CurOpFailpointHelpers::waitWhileFailPointEnabled(
                &hangBeforeCollectionCount, opCtx, "hangBeforeCollectionCount", []() {}, _ns);

            // Start the query planning timer.
            auto curOp = CurOp::get(opCtx);
            curOp->beginQueryPlanningTimer();

            if (req.get().getMirrored().value_or(false)) {
                const auto& invocation = CommandInvocation::get(opCtx);
                invocation->markMirrored();
            } else {
                analyzeShardKeyIfNeeded(opCtx, req.get());
            }

            auto expCtx =
                makeExpressionContextForGetExecutor(opCtx,
                                                    req.get().getCollation().value_or(BSONObj()),
                                                    _ns,
                                                    boost::none /* verbosity*/);

            const auto& collection = ctx->getCollection();
            const auto extensionsCallback = getExtensionsCallback(collection, opCtx, _ns);
            auto parsedFind = uassertStatusOK(
                parsed_find_command::parseFromCount(expCtx, req.get(), *extensionsCallback, _ns));

            registerRequestForQueryStats(opCtx, expCtx, curOp, ctx, req.get(), *parsedFind);

            if (ctx->getView()) {
                // Relinquish locks. The aggregation command will re-acquire them.
                ctx.reset();
                return runCountOnView(opCtx, req);
            }

            // Check whether we are allowed to read from this node after acquiring our locks.
            auto replCoord = repl::ReplicationCoordinator::get(opCtx);
            uassertStatusOK(replCoord->checkCanServeReadsFor(
                opCtx, _ns, ReadPreferenceSetting::get(opCtx).canRunOnSecondary()));

            // RAII object that prevents chunks from being cleaned on sharded collections.
            auto rangePreverser = buildRangePreserverForShardedCollections(opCtx, collection);

            auto statusWithPlanExecutor =
                getExecutorCount(expCtx, &collection, std::move(parsedFind), req.get());
            uassertStatusOK(statusWithPlanExecutor.getStatus());

            auto exec = std::move(statusWithPlanExecutor.getValue());

            // Store the plan summary string in CurOp.
            {
                stdx::lock_guard<Client> lk(*opCtx->getClient());
                curOp->setPlanSummary(lk, exec->getPlanExplainer().getPlanSummary());
            }

            auto countResult = exec->executeCount();

            // Store metrics for current operation.
            recordCurOpMetrics(opCtx, curOp, collection, *exec);

            // Store profiling data if profiling is enabled.
            collectProfilingDataIfNeeded(curOp, *exec);

            collectQueryStatsMongod(opCtx, expCtx, std::move(curOp->debug().queryStatsInfo.key));

            CountCommandReply reply = buildCountReply(countResult);
            if (curOp->debug().queryStatsInfo.metricsRequested) {
                reply.setMetrics(curOp->debug().getCursorMetrics().toBSON());
            }
            return reply;
        }

        ReadConcernSupportResult supportsReadConcern(repl::ReadConcernLevel level,
                                                     bool isImplicitDefault) const override {
            static const Status kSnapshotNotSupported{ErrorCodes::InvalidOptions,
                                                      "read concern snapshot not supported"};
            return {{level == repl::ReadConcernLevel::kSnapshotReadConcern, kSnapshotNotSupported},
                    Status::OK()};
        }

        bool supportsReadMirroring() const override {
            return true;
        }

        void appendMirrorableRequest(BSONObjBuilder* bob) const override {
            const auto& req = request();

            // Append the keys that can be mirrored.
            if (const auto& nsOrUUID = req.getNamespaceOrUUID(); nsOrUUID.isNamespaceString()) {
                bob->append(CountCommandRequest::kCommandName, nsOrUUID.nss().coll());
            } else {
                uassert(7145300, "expecting nsOrUUID to contain a UUID", nsOrUUID.isUUID());
                bob->append(CountCommandRequest::kCommandName, nsOrUUID.uuid().toBSON());
            }
            bob->append(CountCommandRequest::kQueryFieldName, req.getQuery());
            if (req.getSkip()) {
                bob->append(CountCommandRequest::kSkipFieldName, *req.getSkip());
            }
            if (req.getLimit()) {
                bob->append(CountCommandRequest::kLimitFieldName, *req.getLimit());
            }
            bob->append(CountCommandRequest::kHintFieldName, req.getHint());
            if (req.getCollation()) {
                bob->append(CountCommandRequest::kCollationFieldName, *req.getCollation());
            }
            if (req.getShardVersion()) {
                req.getShardVersion()->serialize(CountCommandRequest::kShardVersionFieldName, bob);
            }
            if (req.getDatabaseVersion()) {
                bob->append(CountCommandRequest::kDatabaseVersionFieldName,
                            req.getDatabaseVersion()->toBSON());
            }
            if (req.getEncryptionInformation()) {
                bob->append(CountCommandRequest::kEncryptionInformationFieldName,
                            req.getEncryptionInformation()->toBSON());
            }
        }

        bool canIgnorePrepareConflicts() const override {
            return true;
        }

    private:
        void registerRequestForQueryStats(
            OperationContext* opCtx,
            const boost::intrusive_ptr<ExpressionContext>& expCtx,
            CurOp* curOp,
            const boost::optional<AutoGetCollectionForReadCommandMaybeLockFree>& ctx,
            const CountCommandRequest& req,
            const ParsedFindCommand& parsedFind) {
            if (feature_flags::gFeatureFlagQueryStatsCountDistinct
                    .isEnabledUseLastLTSFCVWhenUninitialized(
                        serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
                query_stats::registerRequest(opCtx, _ns, [&]() {
                    return std::make_unique<query_stats::CountKey>(expCtx,
                                                                   parsedFind,
                                                                   req.getLimit().has_value(),
                                                                   req.getSkip().has_value(),
                                                                   req.getReadConcern(),
                                                                   req.getMaxTimeMS().has_value(),
                                                                   ctx->getCollectionType());
                });

                if (req.getIncludeQueryStatsMetrics() &&
                    feature_flags::gFeatureFlagQueryStatsDataBearingNodes.isEnabled(
                        serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
                    curOp->debug().queryStatsInfo.metricsRequested = true;
                }
            }
        }

        void recordCurOpMetrics(OperationContext* opCtx,
                                CurOp* curOp,
                                const CollectionPtr& collection,
                                const PlanExecutor& exec) {
            PlanSummaryStats summaryStats;
            exec.getPlanExplainer().getSummaryStats(&summaryStats);
            if (collection) {
                CollectionQueryInfo::get(collection).notifyOfQuery(opCtx, collection, summaryStats);
            }
            curOp->debug().setPlanSummaryMetrics(std::move(summaryStats));
            curOp->setEndOfOpMetrics(kNReturned);
        }

        void collectProfilingDataIfNeeded(CurOp* curOp, PlanExecutor& exec) {
            if (!curOp->shouldDBProfile()) {
                return;
            }
            auto&& explainer = exec.getPlanExplainer();
            auto&& [stats, _] =
                explainer.getWinningPlanStats(ExplainOptions::Verbosity::kExecStats);
            curOp->debug().execStats = std::move(stats);
        }

        void analyzeShardKeyIfNeeded(OperationContext* opCtx, const CountCommandRequest& req) {
            if (auto sampleId = analyze_shard_key::getOrGenerateSampleId(
                    opCtx, _ns, analyze_shard_key::SampledCommandNameEnum::kCount, req)) {
                analyze_shard_key::QueryAnalysisWriter::get(opCtx)
                    ->addCountQuery(
                        *sampleId, _ns, req.getQuery(), req.getCollation().value_or(BSONObj()))
                    .getAsync([](auto) {});
            }
        }

        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        boost::optional<ScopedCollectionFilter> buildRangePreserverForShardedCollections(
            OperationContext* opCtx, const CollectionPtr& collection) {
            boost::optional<ScopedCollectionFilter> rangePreserver;
            if (collection.isSharded_DEPRECATED()) {
                rangePreserver.emplace(
                    CollectionShardingState::acquire(opCtx, _ns)
                        ->getOwnershipFilter(
                            opCtx,
                            CollectionShardingState::OrphanCleanupPolicy::kDisallowOrphanCleanup));
            }
            return rangePreserver;
        }

        // Prepare request object so that it gets rewritten when FLE is enabled. Otherwise return
        // the original request object without modifying/copying it.
        std::reference_wrapper<const CountCommandRequest> prepareRequest(
            OperationContext* opCtx,
            boost::optional<CountCommandRequest>& potentiallyRewrittenReq) {
            auto req = std::cref(request());

            if (shouldDoFLERewrite(req.get())) {
                if (!req.get().getEncryptionInformation()->getCrudProcessed().value_or(false)) {
                    potentiallyRewrittenReq.emplace(req);
                    processFLECountD(opCtx, _ns, &*potentiallyRewrittenReq);
                    req = std::cref(potentiallyRewrittenReq.value());
                }
                stdx::lock_guard<Client> lk(*opCtx->getClient());
                CurOp::get(opCtx)->setShouldOmitDiagnosticInformation(lk, true);
            }

            return req;
        }

        // Build the return value for this command.
        CountCommandReply buildCountReply(long long countResult) {
            uassert(7145301, "count value must not be negative", countResult >= 0);

            // Return either BSON int32 or int64, depending on the value of countResult.
            // This is required so that drivers can continue to use a BSON int32 for count
            // values < 2 ^ 31, which is what some client applications may still depend on.
            // int64 is only used when the count value exceeds 2 ^ 31.
            auto count = [](long long countResult) -> std::variant<std::int32_t, std::int64_t> {
                constexpr long long maxIntCountResult = std::numeric_limits<std::int32_t>::max();
                if (countResult < maxIntCountResult) {
                    return static_cast<std::int32_t>(countResult);
                }
                return static_cast<std::int64_t>(countResult);
            }(countResult);

            CountCommandReply reply;
            reply.setCount(count);
            return reply;
        }

        void runExplainOnView(OperationContext* opCtx,
                              const RequestType& req,
                              ExplainOptions::Verbosity verbosity,
                              rpc::ReplyBuilderInterface* replyBuilder) {
            auto curOp = CurOp::get(opCtx);
            curOp->debug().queryStatsInfo.disableForSubqueryExecution = true;
            const auto vts = auth::ValidatedTenancyScope::get(opCtx);
            auto viewAggregation = countCommandAsAggregationCommand(req, _ns);
            uassertStatusOK(viewAggregation);

            auto viewAggCmd =
                OpMsgRequestBuilder::create(vts, _ns.dbName(), viewAggregation.getValue()).body;
            auto viewAggRequest = aggregation_request_helper::parseFromBSON(
                viewAggCmd, vts, verbosity, req.getSerializationContext());

            // An empty PrivilegeVector is acceptable because these privileges are only checked
            // on getMore and explain will not open a cursor.
            auto runStatus = runAggregate(opCtx,
                                          viewAggRequest,
                                          {viewAggRequest},
                                          viewAggregation.getValue(),
                                          PrivilegeVector(),
                                          replyBuilder);
            uassertStatusOK(runStatus);
        }

        CountCommandReply runCountOnView(OperationContext* opCtx, const RequestType& req) {
            const auto vts = auth::ValidatedTenancyScope::get(opCtx);
            auto viewAggregation = countCommandAsAggregationCommand(req, _ns);
            uassertStatusOK(viewAggregation.getStatus());
            auto aggRequest = OpMsgRequestBuilder::create(
                vts, _ns.dbName(), std::move(viewAggregation.getValue()));

            BSONObj aggResult = CommandHelpers::runCommandDirectly(opCtx, aggRequest);
            long long countResult = ViewResponseFormatter(aggResult).getCountValue(
                _ns.dbName().tenantId(),
                SerializationContext::stateCommandReply(req.getSerializationContext()));

            return buildCountReply(countResult);
        }

    private:
        const NamespaceString _ns;
    };

    bool collectsResourceConsumptionMetrics() const override {
        return true;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext* serviceContext) const override {
        return Command::AllowedOnSecondary::kOptIn;
    }

    bool maintenanceOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return false;
    }

    bool allowedWithSecurityToken() const final {
        return true;
    }

    bool shouldAffectReadOptionCounters() const override {
        return true;
    }

    ReadWriteType getReadWriteType() const override {
        return ReadWriteType::kRead;
    }
};
MONGO_REGISTER_COMMAND(CmdCount).forShard();

}  // namespace
}  // namespace mongo
