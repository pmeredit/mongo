/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/optional.hpp>

#include "mongo/db/commands.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/platform/basic.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/management/stream_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

/**
 * streams_writeCheckpoint command implementation.
 */
class WriteCheckpointCmd : public TypedCommand<WriteCheckpointCmd> {
public:
    using Request = mongo::WriteStreamCheckpointCommand;
    using Reply = mongo::WriteStreamCheckpointCommand::Reply;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
    bool adminOnly() const override {
        return false;
    }
    std::string help() const override {
        return "Request a StreamProcessor to write a checkpoint.";
    }
    bool requiresAuth() const override {
        return false;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;
        Reply typedRun(OperationContext* opCtx) {
            const Request& requestParams = request();
            try {
                StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());
                streamManager->writeCheckpoint(requestParams);
                return Reply{};
            } catch (const std::exception& e) {
                LOGV2_INFO(9643609,
                           "Exception in streams_writeCheckpoint",
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

MONGO_REGISTER_COMMAND(WriteCheckpointCmd)
    .requiresFeatureFlag(&mongo::gFeatureFlagStreams)
    .forShard();

}  // namespace streams
