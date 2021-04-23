/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_gen.h"
#include "audit/audit_manager.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/audit.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"
#include "mongo/util/periodic_runner.h"

namespace mongo {
namespace audit {
namespace {

constexpr auto kOK = "ok"_sd;
const std::string kAdmin = "admin";
std::unique_ptr<PeriodicJobAnchor> anchor;

template <typename Cmd>
typename Cmd::Reply runReadCommand(Client* client) {
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

    BSONObjBuilder result;
    CommandHelpers::filterCommandReplyForPassthrough(response.response, &result);
    return Cmd::Reply::parse({Cmd::kCommandName}, result.obj().removeField(kOK));
}

void synchronize(Client* client) try {
    auto generation = runReadCommand<GetAuditConfigGenerationCommand>(client).getGeneration();

    auto* am = getGlobalAuditManager();
    if (generation == am->getConfigGeneration()) {
        // No change, carry on.
        LOGV2_DEBUG(5497499, 5, "No change to audit config generation");
        return;
    }

    am->setConfiguration(client, runReadCommand<GetAuditConfigCommand>(client));

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
