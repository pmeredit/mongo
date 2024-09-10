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
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>

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
#include "mongo/db/api_parameters.h"
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
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
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
#include "mongo/idl/idl_parser.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/rpc/reply_builder_interface.h"
#include "mongo/s/analyze_shard_key_common_gen.h"
#include "mongo/s/query_analysis_sampler_util.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/future.h"
#include "mongo/util/serialization_context.h"
#include "mongo/util/uuid.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

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
class CmdCount : public BasicCommand {
public:
    CmdCount() : BasicCommand("count") {}

    const std::set<std::string>& apiVersions() const override {
        return kApiVersions1;
    }

    std::string help() const override {
        return "count objects in collection";
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool collectsResourceConsumptionMetrics() const override {
        return true;
    }

    bool canIgnorePrepareConflicts() const override {
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

    ReadConcernSupportResult supportsReadConcern(const BSONObj& cmdObj,
                                                 repl::ReadConcernLevel level,
                                                 bool isImplicitDefault) const override {
        static const Status kSnapshotNotSupported{ErrorCodes::InvalidOptions,
                                                  "read concern snapshot not supported"};
        return {{level == repl::ReadConcernLevel::kSnapshotReadConcern, kSnapshotNotSupported},
                Status::OK()};
    }

    bool isSubjectToIngressAdmissionControl() const override {
        return true;
    }

    bool shouldAffectReadOptionCounters() const override {
        return true;
    }

    bool supportsReadMirroring(const BSONObj&) const override {
        return true;
    }

    ReadWriteType getReadWriteType() const override {
        return ReadWriteType::kRead;
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbname,
                                 const BSONObj& cmdObj) const override {
        AuthorizationSession* authSession = AuthorizationSession::get(opCtx->getClient());

        if (!authSession->isAuthorizedToParseNamespaceElement(cmdObj.firstElement())) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }

        const auto hasTerm = false;
        const auto nsOrUUID = CommandHelpers::parseNsOrUUID(dbname, cmdObj);
        if (nsOrUUID.isNamespaceString()) {
            uassert(ErrorCodes::InvalidNamespace,
                    str::stream() << "Namespace " << nsOrUUID.toStringForErrorMsg()
                                  << " is not a valid collection name",
                    nsOrUUID.nss().isValid());
            return auth::checkAuthForFind(authSession, nsOrUUID.nss(), hasTerm);
        }

        const auto resolvedNss =
            CollectionCatalog::get(opCtx)->resolveNamespaceStringFromDBNameAndUUID(
                opCtx, nsOrUUID.dbName(), nsOrUUID.uuid());
        return auth::checkAuthForFind(authSession, resolvedNss, hasTerm);
    }

    Status explain(OperationContext* opCtx,
                   const OpMsgRequest& opMsgRequest,
                   ExplainOptions::Verbosity verbosity,
                   rpc::ReplyBuilderInterface* result) const override {
        DatabaseName dbName = opMsgRequest.parseDbName();
        const BSONObj& cmdObj = opMsgRequest.body;
        // Acquire locks. The RAII object is optional, because in the case
        // of a view, the locks need to be released.
        boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> ctx;
        ctx.emplace(
            opCtx,
            CommandHelpers::parseNsCollectionRequired(dbName, cmdObj),
            AutoGetCollection::Options{}.viewMode(auto_get_collection::ViewMode::kViewsPermitted));
        const auto nss = ctx->getNss();

        std::unique_ptr<CountCommandRequest> request;
        try {
            request = std::make_unique<CountCommandRequest>(
                CountCommandRequest::parse(IDLParserContext("count"), opMsgRequest));
        } catch (...) {
            return exceptionToStatus();
        }

        // Start the query planning timer right after parsing.
        CurOp::get(opCtx)->beginQueryPlanningTimer();

        if (shouldDoFLERewrite(request)) {
            if (!request->getEncryptionInformation()->getCrudProcessed().value_or(false)) {
                processFLECountD(opCtx, nss, request.get());
            }
            stdx::lock_guard<Client> lk(*opCtx->getClient());
            CurOp::get(opCtx)->setShouldOmitDiagnosticInformation_inlock(lk, true);
        }

        SerializationContext serializationCtx = request->getSerializationContext();
        if (ctx->getView()) {
            // Relinquish locks. The aggregation command will re-acquire them.
            ctx.reset();

            auto curOp = CurOp::get(opCtx);
            curOp->debug().queryStatsInfo.disableForSubqueryExecution = true;

            auto viewAggregation = countCommandAsAggregationCommand(*request, nss);
            if (!viewAggregation.isOK()) {
                return viewAggregation.getStatus();
            }

            auto viewAggCmd = OpMsgRequestBuilder::create(opMsgRequest.validatedTenancyScope,
                                                          nss.dbName(),
                                                          viewAggregation.getValue())
                                  .body;
            auto viewAggRequest = aggregation_request_helper::parseFromBSON(
                viewAggCmd, opMsgRequest.validatedTenancyScope, verbosity, serializationCtx);

            // An empty PrivilegeVector is acceptable because these privileges are only checked
            // on getMore and explain will not open a cursor.
            return runAggregate(opCtx,
                                viewAggRequest,
                                {viewAggRequest},
                                viewAggregation.getValue(),
                                PrivilegeVector(),
                                result);
        }

        const auto& collection = ctx->getCollection();

        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        boost::optional<ScopedCollectionFilter> rangePreserver;
        if (collection.isSharded_DEPRECATED()) {
            rangePreserver.emplace(
                CollectionShardingState::acquire(opCtx, nss)
                    ->getOwnershipFilter(
                        opCtx,
                        CollectionShardingState::OrphanCleanupPolicy::kDisallowOrphanCleanup));
        }

        auto expCtx = makeExpressionContextForGetExecutor(
            opCtx, request->getCollation().value_or(BSONObj()), nss, verbosity);

        const auto extensionsCallback = getExtensionsCallback(collection, opCtx, nss);
        auto parsedFind = uassertStatusOK(
            parsed_find_command::parseFromCount(expCtx, *request, *extensionsCallback, nss));

        auto statusWithPlanExecutor =
            getExecutorCount(expCtx, &collection, std::move(parsedFind), *request);

        if (!statusWithPlanExecutor.isOK()) {
            return statusWithPlanExecutor.getStatus();
        }

        auto exec = std::move(statusWithPlanExecutor.getValue());

        auto bodyBuilder = result->getBodyBuilder();
        Explain::explainStages(exec.get(),
                               collection,
                               verbosity,
                               BSONObj(),
                               SerializationContext::stateCommandReply(serializationCtx),
                               cmdObj,
                               &bodyBuilder);
        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const DatabaseName& dbName,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        CommandHelpers::handleMarkKillOnClientDisconnect(opCtx);

        // Capture diagnostics for tassert and invariant failures that may occur during query
        // parsing, planning or execution. No work is done on the hot-path, all computation of
        // these diagnostics is done lazily during failure handling. This line just creates an
        // RAII object which holds references to objects on this stack frame, which will be used
        // to print diagnostics in the event of a tassert or invariant.
        ScopedDebugInfo countCmdDiagnostics("commandDiagnostics",
                                            command_diagnostics::Printer{opCtx});

        // Acquire locks and resolve possible UUID. The RAII object is optional, because in the case
        // of a view, the locks need to be released.
        boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> ctx;
        ctx.emplace(
            opCtx,
            CommandHelpers::parseNsOrUUID(dbName, cmdObj),
            AutoGetCollection::Options{}.viewMode(auto_get_collection::ViewMode::kViewsPermitted));
        const auto& nss = ctx->getNss();

        CurOpFailpointHelpers::waitWhileFailPointEnabled(
            &hangBeforeCollectionCount, opCtx, "hangBeforeCollectionCount", []() {}, nss);

        const auto vts = auth::ValidatedTenancyScope::get(opCtx);
        const auto sc = vts != boost::none
            ? SerializationContext::stateCommandRequest(vts->hasTenantId(), vts->isFromAtlasProxy())
            : SerializationContext::stateCommandRequest();

        auto request = CountCommandRequest::parse(
            IDLParserContext("count", vts, dbName.tenantId(), sc), cmdObj);

        // Start the query planning timer right after parsing.
        auto curOp = CurOp::get(opCtx);
        curOp->beginQueryPlanningTimer();

        if (shouldDoFLERewrite(request)) {
            if (!request.getEncryptionInformation()->getCrudProcessed().value_or(false)) {
                processFLECountD(opCtx, nss, &request);
            }
            stdx::lock_guard<Client> lk(*opCtx->getClient());
            CurOp::get(opCtx)->setShouldOmitDiagnosticInformation_inlock(lk, true);
        }
        if (request.getMirrored().value_or(false)) {
            const auto& invocation = CommandInvocation::get(opCtx);
            invocation->markMirrored();
        }

        if (!request.getMirrored()) {
            if (auto sampleId = analyze_shard_key::getOrGenerateSampleId(
                    opCtx, nss, analyze_shard_key::SampledCommandNameEnum::kCount, request)) {
                analyze_shard_key::QueryAnalysisWriter::get(opCtx)
                    ->addCountQuery(*sampleId,
                                    nss,
                                    request.getQuery(),
                                    request.getCollation().value_or(BSONObj()))
                    .getAsync([](auto) {});
            }
        }

        auto expCtx = makeExpressionContextForGetExecutor(
            opCtx, request.getCollation().value_or(BSONObj()), nss, boost::none /* verbosity*/);

        const auto& collection = ctx->getCollection();
        const auto extensionsCallback = getExtensionsCallback(collection, opCtx, nss);
        auto parsedFind = uassertStatusOK(
            parsed_find_command::parseFromCount(expCtx, request, *extensionsCallback, nss));

        if (feature_flags::gFeatureFlagQueryStatsCountDistinct
                .isEnabledUseLastLTSFCVWhenUninitialized(
                    serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            query_stats::registerRequest(opCtx, nss, [&]() {
                return std::make_unique<query_stats::CountKey>(expCtx,
                                                               *parsedFind,
                                                               request.getLimit().has_value(),
                                                               request.getSkip().has_value(),
                                                               request.getReadConcern(),
                                                               request.getMaxTimeMS().has_value(),
                                                               ctx->getCollectionType());
            });

            if (request.getIncludeQueryStatsMetrics() &&
                feature_flags::gFeatureFlagQueryStatsDataBearingNodes.isEnabled(
                    serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
                curOp->debug().queryStatsInfo.metricsRequested = true;
            }
        }

        if (ctx->getView()) {
            auto viewAggregation = countCommandAsAggregationCommand(request, nss);
            // Relinquish locks. The aggregation command will re-acquire them.
            ctx.reset();

            uassertStatusOK(viewAggregation.getStatus());

            auto aggRequest =
                OpMsgRequestBuilder::create(vts, dbName, std::move(viewAggregation.getValue()));
            BSONObj aggResult = CommandHelpers::runCommandDirectly(opCtx, aggRequest);

            uassertStatusOK(ViewResponseFormatter(aggResult).appendAsCountResponse(
                &result,
                dbName.tenantId(),
                SerializationContext::stateCommandReply(request.getSerializationContext())));
            return true;
        }

        // Check whether we are allowed to read from this node after acquiring our locks.
        auto replCoord = repl::ReplicationCoordinator::get(opCtx);
        uassertStatusOK(replCoord->checkCanServeReadsFor(
            opCtx, nss, ReadPreferenceSetting::get(opCtx).canRunOnSecondary()));


        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        boost::optional<ScopedCollectionFilter> rangePreserver;
        if (collection.isSharded_DEPRECATED()) {
            rangePreserver.emplace(
                CollectionShardingState::acquire(opCtx, nss)
                    ->getOwnershipFilter(
                        opCtx,
                        CollectionShardingState::OrphanCleanupPolicy::kDisallowOrphanCleanup));
        }

        auto statusWithPlanExecutor =
            getExecutorCount(expCtx, &collection, std::move(parsedFind), request);
        uassertStatusOK(statusWithPlanExecutor.getStatus());

        auto exec = std::move(statusWithPlanExecutor.getValue());

        // Store the plan summary string in CurOp.
        {
            stdx::lock_guard<Client> lk(*opCtx->getClient());
            curOp->setPlanSummary_inlock(exec->getPlanExplainer().getPlanSummary());
        }

        auto countResult = exec->executeCount();

        PlanSummaryStats summaryStats;
        exec->getPlanExplainer().getSummaryStats(&summaryStats);
        if (collection) {
            CollectionQueryInfo::get(collection).notifyOfQuery(opCtx, collection, summaryStats);
        }
        curOp->debug().setPlanSummaryMetrics(std::move(summaryStats));
        curOp->setEndOfOpMetrics(kNReturned);

        if (curOp->shouldDBProfile()) {
            auto&& explainer = exec->getPlanExplainer();
            auto&& [stats, _] =
                explainer.getWinningPlanStats(ExplainOptions::Verbosity::kExecStats);
            curOp->debug().execStats = std::move(stats);
        }

        result.appendNumber("n", countResult);

        const auto* cq = exec->getCanonicalQuery();
        expCtx = cq ? cq->getExpCtx()
                    : ExpressionContext::makeBlankExpressionContext(opCtx, exec->nss());
        collectQueryStatsMongod(opCtx, expCtx, std::move(curOp->debug().queryStatsInfo.key));

        if (curOp->debug().queryStatsInfo.metricsRequested) {
            result.append("metrics", curOp->debug().getCursorMetrics().toBSON());
        }

        return true;
    }

    void appendMirrorableRequest(BSONObjBuilder* bob, const BSONObj& cmdObj) const override {
        static const auto kMirrorableKeys = [] {
            BSONObjBuilder keyBob;

            keyBob.append("count", 1);
            keyBob.append("query", 1);
            keyBob.append("skip", 1);
            keyBob.append("limit", 1);
            keyBob.append("hint", 1);
            keyBob.append("collation", 1);
            keyBob.append("shardVersion", 1);
            keyBob.append("databaseVersion", 1);
            keyBob.append("encryptionInformation", 1);

            return keyBob.obj();
        }();

        // Filter the keys that can be mirrored
        cmdObj.filterFieldsUndotted(bob, kMirrorableKeys, true);
    }
};
MONGO_REGISTER_COMMAND(CmdCount).forShard();

}  // namespace
}  // namespace mongo
