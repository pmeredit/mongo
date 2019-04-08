/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/dbmessage.h"
#include "mongo/rpc/factory.h"
#include "mongo/rpc/message.h"
#include "mongo/rpc/reply_builder_interface.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

#include "cryptd_service_entry_point.h"
#include "cryptd_watchdog.h"

namespace mongo {
namespace {
void generateErrorResponse(OperationContext* opCtx,
                           rpc::ReplyBuilderInterface* replyBuilder,
                           const DBException& exception,
                           const BSONObj& replyMetadata,
                           BSONObj extraFields = {}) {

    // We could have thrown an exception after setting fields in the builder,
    // so we need to reset it to a clean state just to be sure.
    replyBuilder->reset();
    replyBuilder->setCommandReply(exception.toStatus(), extraFields);
    replyBuilder->getBodyBuilder().appendElements(replyMetadata);
}

void runComand(OperationContext* opCtx,
               Command* command,
               const OpMsgRequest& request,
               rpc::ReplyBuilderInterface* replyBuilder) {

    auto invocation = command->parse(opCtx, request);

    const auto dbname = request.getDatabase().toString();
    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << "Invalid database name: '" << dbname << "'",
            NamespaceString::validDBName(dbname, NamespaceString::DollarInDbNameBehavior::Allow));

    invocation->run(opCtx, replyBuilder);

    auto body = replyBuilder->getBodyBuilder();
    CommandHelpers::extractOrAppendOk(body);
}
}  // namespace

void ServiceEntryPointCryptD::startSession(transport::SessionHandle session) {
    ServiceEntryPointImpl::startSession(session);

    signalIdleWatchdog();
}

DbResponse ServiceEntryPointCryptD::handleRequest(OperationContext* opCtx, const Message& message) {
    auto replyBuilder = rpc::makeReplyBuilder(rpc::protocolForMessage(message));

    OpMsgRequest request;
    try {  // Parse.
        request = rpc::opMsgRequestFromAnyProtocol(message);
    } catch (const DBException& ex) {
        // If this error needs to fail the connection, propagate it out.
        if (ErrorCodes::isConnectionFatalMessageParseError(ex.code()))
            throw;

        // Otherwise, reply with the parse error. This is useful for cases where parsing fails
        // due to user-supplied input, such as the document too deep error. Since we failed
        // during parsing, we can't log anything about the command.
        log() << "assertion while parsing command: " << ex.toString();

        BSONObjBuilder metadataBob;

        generateErrorResponse(opCtx, replyBuilder.get(), ex, metadataBob.obj(), BSONObj());

        auto response = replyBuilder->done();
        return DbResponse{std::move(response)};
    }

    try {
        Command* c = nullptr;
        // In the absence of a Command object, no redaction is possible. Therefore
        // to avoid displaying potentially sensitive information in the logs,
        // we restrict the log message to the name of the unrecognized command.
        // However, the complete command object will still be echoed to the client.
        if (!(c = CommandHelpers::findCommand(request.getCommandName()))) {
            std::string msg = str::stream() << "no such command: '" << request.getCommandName()
                                            << "'";
            LOG(2) << msg;
            uasserted(ErrorCodes::CommandNotFound, str::stream() << msg);
        }

        LOG(2) << "run command " << request.getDatabase() << " cmd " << redact(request.body);

        runComand(opCtx, c, request, replyBuilder.get());

    } catch (const DBException& ex) {
        BSONObjBuilder metadataBob;

        LOG(1) << "assertion while executing command '" << request.getCommandName() << "' "
               << "on database '" << request.getDatabase() << "' "
               << "with arguments '" << redact(request.body) << "': " << redact(ex.toString());

        generateErrorResponse(opCtx, replyBuilder.get(), ex, metadataBob.obj(), BSONObj());

        auto response = replyBuilder->done();
        return DbResponse{std::move(response)};
    }

    auto response = replyBuilder->done();
    return DbResponse{std::move(response)};
}

}  // namespace mongo
