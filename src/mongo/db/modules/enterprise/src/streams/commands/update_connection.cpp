/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/commands.h"
#include "mongo/db/service_context.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/management/stream_manager.h"

namespace streams {

using namespace mongo;

/**
 * UpdateConnection command implementation
 */

class UpdateConnectionCmd : public TypedCommand<UpdateConnectionCmd> {
public:
    using Request = UpdateConnectionCommand;
    using Reply = UpdateConnectionReply;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return false;
    }

    std::string help() const override {
        return "Update connection information for a streamProcessor.";
    }

    bool requiresAuth() const override {
        return false;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;
        Reply typedRun(OperationContext* opCtx) {
            const auto& requestParams = request();
            StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());
            return streamManager->updateConnection(requestParams);
        }

    private:
        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            // We have no authorization check for now.
        }

        NamespaceString ns() const final {
            return NamespaceString(request().getDbName());
        }
    };
};

MONGO_REGISTER_COMMAND(UpdateConnectionCmd)
    .requiresFeatureFlag(mongo::gFeatureFlagStreams)
    .forShard();

}  // namespace streams
