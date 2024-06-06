/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "live_import/commands/export_collection_gen.h"
#include "live_import/export_collection.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/global_settings.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {

namespace {

/**
 * Live exportation of collection files.
 *
 * {
 *     exportCollection: <collectionName>,
 * }
 */
class ExportCollectionCommand final : public TypedCommand<ExportCollectionCommand> {
public:
    using Request = ExportCollection;

    std::string help() const override {
        return "An enterprise-only mongod command for live exportation of collection files. Usage: "
               "{ exportCollection: <collectionName> }";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    class Invocation final : public MinimalInvocationBase {
    public:
        using MinimalInvocationBase::MinimalInvocationBase;

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
                                                            ActionType::exportCollection));
        }

        void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* result) override {
            uassert(5070400,
                    str::stream() << "This command only works with the WiredTiger storage engine. "
                                     "The current storage engine is: "
                                  << storageGlobalParams.engine,
                    storageGlobalParams.engine == "wiredTiger");
            uassert(5070401,
                    str::stream() << "This command only works in standalone mode.",
                    !getGlobalReplSettings().isReplSet());

            const auto& cmd = request();
            LOGV2(5070402,
                  "Received exportCollection request",
                  "namespace"_attr = cmd.getNamespace());

            auto bodyBuilder = result->getBodyBuilder();
            exportCollection(opCtx, cmd.getNamespace(), &bodyBuilder);
        }
    };
};
MONGO_REGISTER_COMMAND(ExportCollectionCommand).forShard();

}  // namespace

}  // namespace mongo
