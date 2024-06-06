/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "live_import/commands/import_collection_gen.h"
#include "live_import/import_collection.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {
namespace {

/**
 * Live import collection files.
 *
 * {
 *     importCollection: <CollectionProperties object>,
 *     force: <bool>
 * }
 */
class ImportCollectionCommand final : public TypedCommand<ImportCollectionCommand> {
public:
    using Request = ImportCollection;

    std::string help() const override {
        return "An enterprise-only mongod command for live import of collection files";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        void typedRun(OperationContext* opCtx) {
            uassert(5114100,
                    str::stream() << "This command only works with the WiredTiger storage engine. "
                                     "The current storage engine is: "
                                  << storageGlobalParams.engine,
                    storageGlobalParams.engine == "wiredTiger");
            uassert(ErrorCodes::CommandNotSupported,
                    "importCollection command not supported on sharded clusters",
                    serverGlobalParams.clusterRole.has(ClusterRole::None));
            BSONObjBuilder result;
            uassertStatusOK(
                repl::ReplicationCoordinator::get(opCtx)->checkReplEnabledForCommand(&result));

            const auto& cmd = request();
            LOGV2(5070501,
                  "Received importCollection request",
                  "collectionProperties"_attr = redact(cmd.getCommandParameter().toBSON()),
                  "force"_attr = cmd.getForce());
            runImportCollectionCommand(opCtx, cmd.getCommandParameter(), cmd.getForce());
        }

    private:
        NamespaceString ns() const override {
            return request().getCommandParameter().getNs();
        }

        bool supportsWriteConcern() const override {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(request().getCommandParameter().getNs(),
                                                            ActionType::importCollection));
        }
    };
};
MONGO_REGISTER_COMMAND(ImportCollectionCommand).forShard();

}  // namespace
}  // namespace mongo
