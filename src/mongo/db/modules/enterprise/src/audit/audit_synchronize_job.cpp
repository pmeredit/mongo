/**
 *    Copyright (C) 2021 MongoDB Inc.
 */


#include "audit/audit_commands_gen.h"
#include "audit/audit_config_gen.h"
#include "audit/audit_manager.h"
#include "audit/audit_options_gen.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/audit.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"
#include "mongo/s/is_mongos.h"
#include "mongo/util/periodic_runner.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace audit {
namespace {

constexpr auto kOK = "ok"_sd;
const std::string kAdmin = "admin";
std::unique_ptr<PeriodicJobAnchor> anchor;

template <typename Cmd>
boost::optional<typename Cmd::Reply> runReadCommand(Client* client) {
    auto opCtx = client->makeOperationContext();
    auto response =
        uassertStatusOK(Grid::get(opCtx.get())
                            ->shardRegistry()
                            ->getConfigShard()
                            ->runCommandWithFixedRetryAttempts(
                                opCtx.get(),
                                ReadPreferenceSetting(ReadPreference::PrimaryPreferred, TagSet{}),
                                kAdmin,
                                BSON(Cmd::kCommandName << 1),
                                Shard::kDefaultConfigCommandTimeout,
                                Shard::RetryPolicy::kIdempotent));

    if (!response.commandStatus.isOK()) {
        return boost::none;
    }
    BSONObjBuilder result;
    CommandHelpers::filterCommandReplyForPassthrough(response.response, &result);
    return Cmd::Reply::parse(IDLParserContext{Cmd::kCommandName}, result.obj().removeField(kOK));
}

void synchronize(Client* client) try {
    // Have to check for !isMongos() because the FCV is incorrect on mongos.
    if (!isMongos() &&
        feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
            serverGlobalParams.featureCompatibility)) {
        return;
    }

    auto generationResult = runReadCommand<GetAuditConfigGenerationCommand>(client);
    if (!generationResult) {
        // This will occur when the FCV is high enough and the feature flag is enabled on the config
        // server, causing the getAuditConfigGeneration command to fail. This is OK because the
        // newest audit configuration will be fetched by getConfig and the cluster parameter
        // refresher.
        return;
    }

    auto generation = generationResult->getGeneration();
    auto* am = getGlobalAuditManager();
    if (generation == am->getConfigGeneration()) {
        // No change, carry on.
        LOGV2_DEBUG(5497499, 5, "No change to audit config generation");
        return;
    }

    am->setConfiguration(client, *runReadCommand<GetAuditConfigCommand>(client));

} catch (const DBException& ex) {
    LOGV2_ERROR(
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

    // Initially run the job at an aggressive rate until
    // Sharding initialization completes
    // then the job will throttle itself back.
    PeriodicRunner::PeriodicJob job(
        "AuditConfigSynchronizer", synchronize, Seconds(gAuditConfigPollingFrequencySecs));

    anchor = std::make_unique<PeriodicJobAnchor>(periodicRunner->makeJob(std::move(job)));
    anchor->start();
} catch (const DBException& ex) {
    uassertStatusOK(ex.toStatus().withContext(
        "Failed setting up periodic job for audit config synchronization"));
}

MONGO_INITIALIZER(AuditSynchronize)(InitializerContext*) {
    initializeSynchronizeJob = initializeSynchronizeJobImpl;
}

}  // namespace
}  // namespace audit
}  // namespace mongo
