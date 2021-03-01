/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit/audit_commands_gen.h"
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

class SetAuditConfigCmd final : public TypedCommand<SetAuditConfigCmd> {
public:
    using Request = SetAuditConfigCommand;

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        void typedRun(OperationContext* opCtx) {
            uassertStatusOK(Grid::get(opCtx)
                                ->shardRegistry()
                                ->getConfigShard()
                                ->runCommandWithFixedRetryAttempts(
                                    opCtx,
                                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                    request().getDbName().toString(),
                                    request().toBSON({}),
                                    Shard::kDefaultConfigCommandTimeout,
                                    Shard::RetryPolicy::kNotIdempotent));
        }

    private:
        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            auto* as = AuthorizationSession::get(opCtx->getClient());
            uassert(ErrorCodes::Unauthorized,
                    "Not authorized to change audit configuration",
                    as->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                         ActionType::auditConfigure));
        }

        NamespaceString ns() const final {
            return NamespaceString(request().getDbName(), "");
        }
    };

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const final {
        return true;
    }
} cmdSetAuditConfig;

}  // namespace
}  // namespace audit
}  // namespace mongo
