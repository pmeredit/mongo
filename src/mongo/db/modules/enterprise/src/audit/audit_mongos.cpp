/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_command.h"
#include "audit/audit_manager.h"
#include "audit/audit_options_gen.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/cluster_server_parameter_cmds_gen.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/idl/cluster_server_parameter_refresher.h"
#include "mongo/logv2/log.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo {
namespace audit {
namespace {

static constexpr StringData kAuditConfigParameter = "auditConfig"_sd;
struct SetAuditConfigCmd {
    using Request = SetAuditConfigCommand;
    using Reply = void;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to change audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kNever;

    static void typedRun(OperationContext* opCtx, const Request& cmd) {
        // Always forward setAuditConfig to the config server; it will deal with feature flag/FCV
        // complications.
        auto response = uassertStatusOK(
            Grid::get(opCtx)->shardRegistry()->getConfigShard()->runCommandWithFixedRetryAttempts(
                opCtx,
                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                cmd.getDbName(),
                cmd.toBSON(),
                Milliseconds(defaultConfigCommandTimeoutMS.load()),
                Shard::RetryPolicy::kIdempotent));
        uassertStatusOK(response.commandStatus);
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<SetAuditConfigCmd>).forRouter();

struct GetAuditConfigGenerationCmd {
    using Request = GetAuditConfigGenerationCommand;
    using Reply = GetAuditConfigGenerationReply;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        // (Ignore FCV check): This check is on mongos so we expect to ignore FCV here.
        if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabledAndIgnoreFCVUnsafe()) {
            // Forward the command to the config server, as we are unsure of whether the feature
            // flag is active on the cluster.
            auto response =
                uassertStatusOK(Grid::get(opCtx)
                                    ->shardRegistry()
                                    ->getConfigShard()
                                    ->runCommandWithFixedRetryAttempts(
                                        opCtx,
                                        ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                        cmd.getDbName(),
                                        cmd.toBSON(),
                                        Milliseconds(defaultConfigCommandTimeoutMS.load()),
                                        Shard::RetryPolicy::kIdempotent));

            uassertStatusOK(response.commandStatus);
            BSONObjBuilder result;
            CommandHelpers::filterCommandReplyForPassthrough(response.response, &result);
            return Reply::parse(IDLParserContext{"GetAuditConfigGenerationReply"},
                                result.obj().removeField("ok"_sd));
        }
        // Grab generation locally
        return Reply(getGlobalAuditManager()->getConfigGeneration());
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<GetAuditConfigGenerationCmd>).forRouter();

struct GetAuditConfigCmd {
    using Request = GetAuditConfigCommand;
    using Reply = AuditConfigDocument;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        // (Ignore FCV check): This check is on mongos so we expect to ignore FCV here.
        if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabledAndIgnoreFCVUnsafe()) {
            // If the feature flag is enabled, we refresh our audit configuration. If FCV is high
            // enough that the feature flag is active on the cluster, the in-memory audit config
            // will be updated by this refresh. Otherwise, we will be refreshing the audit config
            // with the auditSynchronizeJob, so we can just return the in-memory state here.
            uassertStatusOK(ClusterServerParameterRefresher::get(opCtx)->refreshParameters(opCtx));
        }
        return getGlobalAuditManager()->getAuditConfig();
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<GetAuditConfigCmd>).forRouter();

}  // namespace
}  // namespace audit
}  // namespace mongo
