/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/management/stream_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

/**
 * streams_startStreamSample command implementation.
 */
class StartStreamSampleCmd : public TypedCommand<StartStreamSampleCmd> {
public:
    using Request = StartStreamSampleCommand;
    using Reply = StartStreamSampleCommand::Reply;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
    bool adminOnly() const override {
        return false;
    }
    std::string help() const override {
        return "Start a stream sample.";
    }
    bool requiresAuth() const override {
        return false;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;
        Reply typedRun(OperationContext* opCtx) {
            const StartStreamSampleCommand& requestParams = request();
            try {
                StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());
                int64_t cursorId = streamManager->startSample(requestParams);

                Reply reply;
                reply.setName(requestParams.getName());
                reply.setCursorId(cursorId);
                return reply;
            } catch (const std::exception& e) {
                LOGV2_INFO(9643606,
                           "Exception in streams_startStreamSample",
                           "streamProcessorName"_attr = requestParams.getName().toString(),
                           "streamProcessorId"_attr = requestParams.getProcessorId(),
                           "tenantId"_attr = requestParams.getTenantId(),
                           "exception"_attr = e.what());
                throw;
            }
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

MONGO_REGISTER_COMMAND(StartStreamSampleCmd)
    .requiresFeatureFlag(mongo::gFeatureFlagStreams)
    .forShard();

}  // namespace streams
