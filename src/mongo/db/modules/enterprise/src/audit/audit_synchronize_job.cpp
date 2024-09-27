/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "audit/audit_commands_gen.h"
#include "audit/audit_config_gen.h"
#include "audit/audit_manager.h"
#include "audit/audit_options_gen.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/feature_compatibility_version_parser.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/transaction/transaction_api.h"
#include "mongo/db/vector_clock.h"
#include "mongo/logv2/log.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"
#include "mongo/util/duration.h"
#include "mongo/util/periodic_runner.h"
#include "mongo/util/version/releases.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace audit {
namespace {

constexpr auto kOK = "ok"_sd;
std::unique_ptr<PeriodicJobAnchor> anchor;

template <typename Cmd>
boost::optional<typename Cmd::Reply> runReadCommand(OperationContext* opCtx) {
    auto response = uassertStatusOK(
        Grid::get(opCtx)->shardRegistry()->getConfigShard()->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting(ReadPreference::PrimaryPreferred, TagSet{}),
            DatabaseName::kAdmin,
            BSON(Cmd::kCommandName << 1),
            Milliseconds(defaultConfigCommandTimeoutMS.load()),
            Shard::RetryPolicy::kIdempotent));

    if (!response.commandStatus.isOK()) {
        return boost::none;
    }
    BSONObjBuilder result;
    CommandHelpers::filterCommandReplyForPassthrough(response.response, &result);
    return Cmd::Reply::parse(IDLParserContext{Cmd::kCommandName}, result.obj().removeField(kOK));
}

multiversion::FeatureCompatibilityVersion fetchFCV(OperationContext* opCtx) {
    auto response = uassertStatusOK(
        Grid::get(opCtx)->shardRegistry()->getConfigShard()->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            DatabaseName::kAdmin,
            BSON("getParameter"_sd << 1 << "featureCompatibilityVersion"_sd << 1),
            Milliseconds(defaultConfigCommandTimeoutMS.load()),
            Shard::RetryPolicy::kIdempotent));

    uassertStatusOK(response.commandStatus);
    BSONObjBuilder result;
    CommandHelpers::filterCommandReplyForPassthrough(response.response, &result);
    return FeatureCompatibilityVersionParser::parseVersion(
        result.obj()["featureCompatibilityVersion"]["version"].String());
}

std::pair<multiversion::FeatureCompatibilityVersion, boost::optional<AuditConfigDocument>>
fetchFCVAndAuditConfig(OperationContext* opCtx) {
    auto fcv = std::make_shared<multiversion::FeatureCompatibilityVersion>();
    auto configDoc = std::make_shared<AuditConfigDocument>();
    auto doFetch = [fcv, configDoc](const txn_api::TransactionClient& txnClient,
                                    ExecutorPtr txnExec) {
        FindCommandRequest findFCV{NamespaceString::kServerConfigurationNamespace};
        findFCV.setFilter(BSON("_id"
                               << "featureCompatibilityVersion"));
        return txnClient.exhaustiveFind(findFCV)
            .thenRunOn(txnExec)
            .then([&fcv, &configDoc, &txnClient, txnExec](auto foundDocs) {
                uassert(7410712,
                        "Expected to find FCV in admin.system.version but found nothing!",
                        !foundDocs.empty());
                *fcv = FeatureCompatibilityVersionParser::parseVersion(
                    foundDocs[0]["version"].String());

                FindCommandRequest findAuditConfig{NamespaceString::kConfigSettingsNamespace};
                findAuditConfig.setFilter(BSON("_id"
                                               << "audit"));
                return txnClient.exhaustiveFind(findAuditConfig)
                    .thenRunOn(txnExec)
                    .then([&configDoc](auto foundDocs) {
                        if (foundDocs.empty()) {
                            *configDoc = AuditConfigDocument{{}, false};
                            configDoc->setGeneration(OID());
                        } else {
                            *configDoc = AuditConfigDocument::parse(
                                IDLParserContext{"fetchFCVAndAuditConfig"}, foundDocs[0]);
                        }
                    })
                    .semi();
            })
            .semi();
    };

    repl::ReadConcernArgs::get(opCtx) =
        repl::ReadConcernArgs(repl::ReadConcernLevel::kSnapshotReadConcern);

    // We need to commit w/ writeConcern = majority for readConcern = snapshot to work.
    opCtx->setWriteConcern(WriteConcernOptions{WriteConcernOptions::kMajority,
                                               WriteConcernOptions::SyncMode::UNSET,
                                               WriteConcernOptions::kNoTimeout});

    auto executor = Grid::get(opCtx)->getExecutorPool()->getFixedExecutor();
    auto inlineExecutor = std::make_shared<executor::InlineExecutor>();
    txn_api::SyncTransactionWithRetries txn(opCtx, executor, nullptr, inlineExecutor);
    txn.run(opCtx, doFetch);
    return {*fcv, *configDoc};
}

bool shouldSkipSynchronizeOnFCV(const multiversion::FeatureCompatibilityVersion& fcv) {
    // (Generic FCV reference): Block auditSynchronizeJob while FCV is transitioning between a
    // version where the feature flag is enabled and one where it is disabled, to prevent
    // overwriting the in-memory config in an undesirable way.
    if (ServerGlobalParams::FCVSnapshot::isUpgradingOrDowngrading(fcv)) {
        auto [fromFCV, toFCV] = multiversion::getTransitionFCVFromAndTo(fcv);
        return feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabledOnVersion(fromFCV) ||
            feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabledOnVersion(toFCV);
    } else {
        return feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabledOnVersion(fcv);
    }
}

void synchronize(Client* client) try {
    auto opCtx = client->makeOperationContext();
    auto as = AuthorizationSession::get(client);
    as->grantInternalAuthorization();

    AuditConfigDocument auditConfigDoc;
    boost::optional<FixedFCVRegion> fixedFcvRegion;
    if (serverGlobalParams.clusterRole.hasExclusively(ClusterRole::RouterServer)) {
        // Do a simple FCV check first so that we can early exit.
        auto fcv = fetchFCV(opCtx.get());
        if (shouldSkipSynchronizeOnFCV(fcv)) {
            LOGV2_DEBUG(7410716,
                        5,
                        "On first FCV fetch, featureFlagAuditConfigClusterParameter is enabled on "
                        "the cluster, or may become enabled after FCV transition (we are mongos), "
                        "don't run auditSynchronizeJob");
            return;
        }
        // We need to refetch FCV transactionally with the audit config to ensure that FCV didn't
        // change between the fetchFCV and the fetch of the config.
        auto [fcvRefetch, fetchedDoc] = fetchFCVAndAuditConfig(opCtx.get());
        if (shouldSkipSynchronizeOnFCV(fcvRefetch)) {
            LOGV2_DEBUG(7410720,
                        5,
                        "On FCV refetch, featureFlagAuditConfigClusterParameter is enabled on the "
                        "cluster, or may become enabled after FCV transition (we are mongos), "
                        "don't run auditSynchronizeJob");
            return;
        }
        auditConfigDoc = std::move(*fetchedDoc);
    } else {
        fixedFcvRegion.emplace(opCtx.get());

        const auto fcvSnapshot = serverGlobalParams.featureCompatibility.acquireFCVSnapshot();
        if (!fcvSnapshot.isVersionInitialized()) {
            // We skip synchronization if it version is uninitialized, because we don't know the
            // true FCV and thus we avoid operations which rely on FCV.
            LOGV2_DEBUG(8047500, 5, "FCV is not initialized, don't run auditSynchronizeJob");
            return;
        }

        if (shouldSkipSynchronizeOnFCV(fcvSnapshot.getVersion())) {
            LOGV2_DEBUG(7410717,
                        5,
                        "featureFlagAuditConfigClusterParameter is enabled on the cluster, or may "
                        "become enabled after FCV transition (we are "
                        "mongod), don't run auditSynchronizeJob");
            return;
        }
        auditConfigDoc = *runReadCommand<GetAuditConfigCommand>(opCtx.get());
    }

    uassert(7410711,
            "Generation was not present or cluster parameter time was present on audit config "
            "document fetched from cluster when feature flag was disabled",
            auditConfigDoc.getGeneration() && !auditConfigDoc.getClusterParameterTime());

    auto* am = getGlobalAuditManager();
    LOGV2_DEBUG(7410718,
                5,
                "Setting new audit configuration in auditSynchronizeJob",
                "config"_attr = auditConfigDoc);
    am->setConfigurationUsingFormatIfNotSet(
        client, auditConfigDoc, AuditConfigFormat::WithGeneration);

} catch (const DBException& ex) {
    LOGV2_WARNING(
        5497400, "Failed attempting to update runtime audit config", "status"_attr = ex.toStatus());
}

void initializeSynchronizeJobImpl(ServiceContext* service) try {
    auto* am = getGlobalAuditManager();

    if (!am->isEnabled() || !am->getRuntimeConfiguration()) {
        // Nothing to do, runtime audit configuration is not enabled.
        return;
    }

    auto periodicRunner = service->getPeriodicRunner();
    invariant(periodicRunner);

    // Initially run the job at an aggressive rate until sharding initialization completes, then the
    // job will throttle itself back.
    // This job is killable. If interrupted, we will warn, and retry after the configured interval.
    PeriodicRunner::PeriodicJob job("AuditConfigSynchronizer",
                                    synchronize,
                                    Seconds(gAuditConfigPollingFrequencySecs),
                                    true /*isKillableByStepdown*/);

    anchor = std::make_unique<PeriodicJobAnchor>(periodicRunner->makeJob(std::move(job)));
    anchor->start();
} catch (const DBException& ex) {
    uassertStatusOK(ex.toStatus().withContext(
        "Failed setting up periodic job for audit config synchronization"));
}

void shutdownSynchronizeJobImpl() {
    if (anchor && anchor->isValid()) {
        anchor->pause();
    }
}

MONGO_INITIALIZER(AuditSynchronize)(InitializerContext*) {
    initializeSynchronizeJob = initializeSynchronizeJobImpl;
    shutdownSynchronizeJob = shutdownSynchronizeJobImpl;
}

}  // namespace
}  // namespace audit
}  // namespace mongo
