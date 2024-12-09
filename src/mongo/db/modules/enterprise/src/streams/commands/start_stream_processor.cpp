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
            try {
                StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());
                return streamManager->startStreamProcessor(requestParams);
            } catch (const std::exception& e) {
                LOGV2_ERROR(9643605,
                            "Unexpected std::exception in streams_startStreamProcessor",
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

MONGO_REGISTER_COMMAND(StartStreamProcessorCmd)
    .requiresFeatureFlag(&mongo::gFeatureFlagStreams)
    .forShard();

}  // namespace streams
