/*
 * Copyright (C) 2012-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_commands_gen.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

namespace mongo::audit {
namespace {

class CmdLogApplicationMessage final : public TypedCommand<CmdLogApplicationMessage> {
public:
    using Request = LogApplicationMessageCommand;
    using Reply = LogApplicationMessageCommand::Reply;

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        bool supportsWriteConcern() const final {
            return false;
        }

        NamespaceString ns() const final {
            return NamespaceString(request().getDbName());
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            auto* as = AuthorizationSession::get(opCtx->getClient());
            uassert(ErrorCodes::Unauthorized,
                    "Not authorized to send custom message to auditlog",
                    as->isAuthorizedForActionsOnResource(
                        ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                        ActionType::applicationMessage));
        }

        Reply typedRun(OperationContext* opCtx) {
            audit::logApplicationMessage(opCtx->getClient(), request().getCommandParameter());
            return Reply{};
        }
    };

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kAlways;
    }

    bool allowedWithSecurityToken() const final {
        return true;
    }
};
MONGO_REGISTER_COMMAND(CmdLogApplicationMessage).forShard().forRouter();

}  // namespace
}  // namespace mongo::audit
