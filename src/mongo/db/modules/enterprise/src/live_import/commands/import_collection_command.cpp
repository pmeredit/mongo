/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "live_import/commands/import_collection_gen.h"
#include "live_import/import_collection.h"
#include "live_import/import_export_options_gen.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"

namespace mongo {
namespace {

/**
 * Live import collection files.
 *
 * {
 *     importCollection: <name>,
 *     collectionProperties: <metadata object>,
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
            uassert(ErrorCodes::CommandNotSupported,
                    "importCollection command not enabled",
                    feature_flags::gLiveImportExport.isEnabledAndIgnoreFCV());
            uassert(5114100,
                    str::stream() << "This command only works with the WiredTiger storage engine. "
                                     "The current storage engine is: "
                                  << storageGlobalParams.engine,
                    storageGlobalParams.engine == "wiredTiger");
            uassert(ErrorCodes::CommandNotSupported,
                    "importCollection command not supported on sharded clusters",
                    serverGlobalParams.clusterRole == ClusterRole::None);
            BSONObjBuilder result;
            uassertStatusOK(
                repl::ReplicationCoordinator::get(opCtx)->checkReplEnabledForCommand(&result));

            const auto& cmd = request();
            LOGV2(5070501,
                  "Received importCollection request",
                  "namespace"_attr = cmd.getNamespace(),
                  "collectionProperties"_attr = redact(cmd.getCollectionProperties()),
                  "force"_attr = cmd.getForce());
            runImportCollectionCommand(
                opCtx, cmd.getNamespace(), cmd.getCollectionProperties(), cmd.getForce());
        }

    private:
        NamespaceString ns() const override {
            return request().getNamespace();
        }

        bool supportsWriteConcern() const override {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(request().getNamespace(),
                                                            ActionType::importCollection));
        }
    };

} importCollectionCommand;

}  // namespace
}  // namespace mongo
