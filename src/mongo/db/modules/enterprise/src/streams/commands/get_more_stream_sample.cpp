/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/commands.h"
#include "mongo/db/cursor_id.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/constants.h"
#include "streams/management/stream_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

/**
 * streams_getMoreStreamSample command implementation.
 */
class GetMoreStreamSampleCmd : public Command {
public:
    using Request = GetMoreStreamSampleCommand;
    using Reply = GetMoreStreamSampleCommand::Reply;

    GetMoreStreamSampleCmd() : Command(Request::kCommandName) {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) override {
        return std::make_unique<Invocation>(this, opMsgRequest);
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
    bool adminOnly() const override {
        return false;
    }
    std::string help() const override {
        return "Get more output records from a stream sample.";
    }
    bool requiresAuth() const override {
        return false;
    }

    class Invocation final : public CommandInvocation {
    public:
        Invocation(Command* cmd, const OpMsgRequest& request)
            : CommandInvocation(cmd),
              _request(Request::parse(IDLParserContext{Request::kCommandName}, request)) {}

        void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* reply) override {
            dassert(reply);
            CursorId cursorId = _request.getCommandParameter();
            CursorResponseBuilder nextBatch(reply, CursorResponseBuilder::Options{});
            try {
                StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());
                auto outputSample = streamManager->getMoreFromSample(
                    _request.getName().toString(), cursorId, _request.getBatchSize());

                size_t bytesToReserve{0};
                for (auto& doc : outputSample.outputDocs) {
                    bytesToReserve += doc.objsize();
                }
                nextBatch.reserveReplyBuffer(bytesToReserve);

                for (auto& doc : outputSample.outputDocs) {
                    nextBatch.append(doc);
                }

                // If the cursor has been exhausted, we will communicate this by returning a
                // CursorId of zero.
                cursorId = (outputSample.done ? CursorId(0) : cursorId);
            } catch (DBException&) {
                nextBatch.abandon();
                throw;
            }

            nextBatch.done(cursorId, ns());
        }

    private:
        bool supportsWriteConcern() const override {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            // We have no authorization check for now.
        }

        NamespaceString ns() const final {
            return NamespaceString(_request.getDbName());
        }

        const Request _request;
    };
};

MONGO_REGISTER_COMMAND(GetMoreStreamSampleCmd)
    .requiresFeatureFlag(&mongo::gFeatureFlagStreams)
    .forShard();

}  // namespace streams
