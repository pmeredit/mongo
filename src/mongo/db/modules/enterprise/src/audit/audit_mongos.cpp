/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_command.h"
#include "audit/audit_manager.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"

namespace mongo {
namespace audit {
namespace {

struct SetAuditConfigCmd {
    using Request = SetAuditConfigCommand;
    using Reply = void;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to change audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kNever;
    static void typedRun(OperationContext* opCtx, const Request& cmd) {
        uassertStatusOK(
            Grid::get(opCtx)->shardRegistry()->getConfigShard()->runCommandWithFixedRetryAttempts(
                opCtx,
                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                cmd.getDbName().toString(),
                cmd.toBSON({}),
                Shard::kDefaultConfigCommandTimeout,
                Shard::RetryPolicy::kNotIdempotent));
    }
};
AuditConfigCmd<SetAuditConfigCmd> setAuditConfigCmd;

struct GetAuditConfigGenerationCmd {
    using Request = GetAuditConfigGenerationCommand;
    using Reply = GetAuditConfigGenerationReply;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        return {getGlobalAuditManager()->getConfigGeneration()};
    }
};
AuditConfigCmd<GetAuditConfigGenerationCmd> getAuditConfigGenerationCmd;

struct GetAuditConfigCmd {
    using Request = GetAuditConfigCommand;
    using Reply = AuditConfigDocument;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        return getGlobalAuditManager()->getAuditConfig();
    }
};
AuditConfigCmd<GetAuditConfigCmd> getAuditConfigCmd;

}  // namespace
}  // namespace audit
}  // namespace mongo
