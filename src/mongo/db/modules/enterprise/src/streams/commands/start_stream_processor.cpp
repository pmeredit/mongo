/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/management/stream_manager.h"

namespace streams {

using namespace mongo;

/**
 * startStreamProcessor command implementation.
 */
class StartStreamProcessorCmd : public TypedCommand<StartStreamProcessorCmd> {
public:
    using Request = StartStreamProcessorCommand;
    using Reply = StartStreamProcessorCommand::Reply;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
    bool adminOnly() const override {
        return false;
    }
    std::string help() const override {
        return "Start a streamProcessor.";
    }
    bool requiresAuth() const override {
        return false;
    }

    std::set<StringData> sensitiveFieldNames() const final {
        return {Request::kConnectionsFieldName, Request::kOptionsFieldName};
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;
        Reply typedRun(OperationContext* opCtx) {
            const auto& requestParams = request();
            StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());
            streamManager->startStreamProcessor(requestParams);

            return Reply{};
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

MONGO_REGISTER_COMMAND(StartStreamProcessorCmd).requiresFeatureFlag(&mongo::gFeatureFlagStreams);

}  // namespace streams
