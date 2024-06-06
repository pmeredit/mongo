/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "live_import/commands/vote_commit_import_collection_gen.h"
#include "live_import/import_collection_coordinator.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/logv2/log.h"
#include "mongo/util/fail_point.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(hangVoteCommitImportCollectionCommand);

/**
 * Vote commit of an ongoing live import.
 *
 * {
 *     voteCommitImportCollection: <NamespaceString>,
 *     from: <HostAndPort>,
 *     dryRunSuccess: <bool>,
 *     reason: <string>,
 * }
 */
class VoteCommitImportCollectionCommand final
    : public TypedCommand<VoteCommitImportCollectionCommand> {
public:
    using Request = VoteCommitImportCollection;

    std::string help() const override {
        return "An internal mongod command to coordinate live import of collection files in a "
               "replica set";
    }

    bool adminOnly() const override {
        return true;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        void typedRun(OperationContext* opCtx) {
            BSONObjBuilder result;
            uassertStatusOK(
                repl::ReplicationCoordinator::get(opCtx)->checkReplEnabledForCommand(&result));

            const auto& cmd = request();
            LOGV2(5085500,
                  "Received voteCommitImportCollection request",
                  "uuid"_attr = cmd.getCommandParameter(),
                  "from"_attr = cmd.getFrom(),
                  "dryRunSuccess"_attr = cmd.getDryRunSuccess(),
                  "reason"_attr = cmd.getReason());
            hangVoteCommitImportCollectionCommand.pauseWhileSet();
            ImportCollectionCoordinator::get(opCtx)->voteForImport(
                cmd.getCommandParameter(), cmd.getFrom(), cmd.getDryRunSuccess(), cmd.getReason());
        }

    private:
        NamespaceString ns() const override {
            return NamespaceString(request().getDbName());
        }

        bool supportsWriteConcern() const override {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "Unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnResource(
                            ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                            ActionType::internal));
        }
    };
};
MONGO_REGISTER_COMMAND(VoteCommitImportCollectionCommand).forShard();

}  // namespace
}  // namespace mongo
