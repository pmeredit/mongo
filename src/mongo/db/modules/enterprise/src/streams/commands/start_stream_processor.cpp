/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "streams/commands/start_stream_processor_gen.h"
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

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;
        void typedRun(OperationContext* opCtx) {
            Reply reply;

            StartStreamProcessorCommand requestParams = request();
            StreamManager& streamManager = StreamManager::get();
            streamManager.startStreamProcessor(requestParams.getName().toString(),
                                               requestParams.getPipeline(),
                                               requestParams.getConnections());
        }

    private:
        bool supportsWriteConcern() const final {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            // We have no authorization check for now.
        }

        NamespaceString ns() const final {
            return NamespaceString(request().getDbName());
        }
    };
};

MONGO_REGISTER_FEATURE_FLAGGED_COMMAND(StartStreamProcessorCmd, mongo::gFeatureFlagStreams);

}  // namespace streams
