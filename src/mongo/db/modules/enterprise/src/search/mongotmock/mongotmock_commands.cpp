/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/getmore_command_gen.h"
#include "mongo/db/query/kill_cursors_gen.h"
#include "mongotmock_state.h"

namespace mongo {
namespace {

using mongotmock::CursorState;
using mongotmock::getMongotMockState;
using mongotmock::MongotMockStateGuard;


const BSONObj placeholderCmd = BSON("placeholder"
                                    << "expected");
const BSONObj placeholderResponse = BSON("placeholder"
                                         << "response");

// Do a "loose" check that every field in 'expectedCmd' is in 'userCmd'. Does not check in the
// other direction.
bool checkUserCommandMatchesExpectedCommand(BSONObj userCmd, BSONObj expectedCmd) {
    // Check that the given command matches the expected command's values.
    for (auto&& elem : expectedCmd) {
        if (!SimpleBSONElementComparator::kInstance.evaluate(userCmd[elem.fieldNameStringData()] ==
                                                             elem)) {
            return false;
        }
    }
    return true;
}

void assertUserCommandMatchesExpectedCommand(BSONObj userCmd, BSONObj expectedCmd) {
    uassert(31086,
            str::stream() << "Expected command matching " << expectedCmd << " but got " << userCmd,
            checkUserCommandMatchesExpectedCommand(userCmd, expectedCmd));
}

/**
 * Base class for MongotMock commands.
 */
class MongotMockBaseCmd : public BasicCommand {
public:
    MongotMockBaseCmd(StringData name) : BasicCommand(name, "") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        MONGO_UNREACHABLE;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        MONGO_UNREACHABLE;
    }

    Status explain(OperationContext* opCtx,
                   const OpMsgRequest& request,
                   ExplainOptions::Verbosity verbosity,
                   rpc::ReplyBuilderInterface* result) const final {
        MONGO_UNREACHABLE;
    }

    bool run(OperationContext* opCtx,
             const DatabaseName& dbName,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) final {
        processCommand(opCtx, dbName, cmdObj, &result);
        return true;
    }

    virtual void processCommand(OperationContext* opCtx,
                                const DatabaseName&,
                                const BSONObj& cmdObj,
                                BSONObjBuilder* result) const = 0;
};

class MongotMockCursorCommand : public MongotMockBaseCmd {
public:
    // This is not a real command, and thus must be built with a real command name.
    MongotMockCursorCommand() = delete;
    MongotMockCursorCommand(StringData commandName) : MongotMockBaseCmd(commandName) {}

private:
    void processCommand(OperationContext* opCtx,
                        const DatabaseName&,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());
        CursorState* state = stateGuard->claimAvailableState();
        uassert(31094,
                str::stream()
                    << "Cannot run search cursor command as there are no remaining unclaimed mock "
                       "cursor states. Received command: "
                    << cmdObj,
                state);

        // We should not have allowed an empty 'history'.
        invariant(state->hasNextCursorResponse());
        auto cmdResponsePair = state->peekNextCommandResponsePair();

        assertUserCommandMatchesExpectedCommand(cmdObj, cmdResponsePair.expectedCommand);

        // If this command returns multiple cursors, we need to claim a state for each one.
        // Note that this only uses one response even though it prepares two cursor states.
        if (cmdResponsePair.response.hasField("cursors")) {
            BSONElement cursorsArrayElem = cmdResponsePair.response.getField("cursors");
            uassert(6253508,
                    "Cursors field in response must be an array",
                    cursorsArrayElem.type() == BSONType::Array);
            auto cursorsArray = cursorsArrayElem.Array();
            uassert(
                6253509, "Cursors field must have exactly two cursors", cursorsArray.size() == 2);
            auto secondState = stateGuard->claimAvailableState();
            uassert(
                6253510,
                str::stream() << "Could not return multiple cursor states as there are no "
                                 "remaining unclaimed mock cursor states. Attempted response was "
                              << cmdResponsePair.response,
                secondState);
            // Pop the response from the second state if it is a placeholder.
            auto secondResponsePair = secondState->peekNextCommandResponsePair();
            if (checkUserCommandMatchesExpectedCommand(secondResponsePair.expectedCommand,
                                                       placeholderCmd)) {
                secondState->popNextCommandResponsePair();
            }
        }

        // Return the queued response.
        result->appendElements(cmdResponsePair.response);

        // Pop the first response.
        state->popNextCommandResponsePair();
    }
};

class MongotMockSearch final : public MongotMockCursorCommand {
public:
    MongotMockSearch() : MongotMockCursorCommand("search") {}

} cmdMongotMockSearch;

// A command that generates a merging pipeline from a search query.
class MongotMockPlanShardedSearchCommand final : public MongotMockCursorCommand {
public:
    MongotMockPlanShardedSearchCommand() : MongotMockCursorCommand("planShardedSearch") {}

} cmdMongotMockPlanSearchCommand;


class MongotMockGetMore final : public MongotMockBaseCmd {
public:
    MongotMockGetMore() : MongotMockBaseCmd("getMore") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName&,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        auto cmd = GetMoreCommandRequest::parse({"getMore"}, cmdObj);
        const auto cursorId = cmd.getCommandParameter();
        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());

        CursorState* cursorState = stateGuard->getCursorState(cursorId);
        uassert(31089,
                str::stream() << "Could not find cursor state associated with cursor id "
                              << cursorId,
                cursorState);
        uassert(31087,
                str::stream() << "Cursor for cursor id " << cursorId << " has no queued history",
                cursorState->hasNextCursorResponse());
        uassert(31088,
                str::stream() << "Cannot run getMore on cursor id " << cursorId
                              << " without having run search",
                cursorState->claimed());

        auto cmdResponsePair = cursorState->peekNextCommandResponsePair();
        assertUserCommandMatchesExpectedCommand(cmdObj, cmdResponsePair.expectedCommand);
        result->appendElements(cmdResponsePair.response);

        cursorState->popNextCommandResponsePair();
    }
} cmdMongotMockGetMore;


class MongotMockKillCursors final : public MongotMockBaseCmd {
public:
    MongotMockKillCursors() : MongotMockBaseCmd("killCursors") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName&,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        auto request = KillCursorsCommandRequest::parse(IDLParserContext("killCursors"), cmdObj);

        const auto& cursorList = request.getCursorIds();
        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());

        uassert(31090,
                "Mock mongot supports killCursors of only one cursor at a time",
                cursorList.size() == 1);

        const auto cursorId = cursorList.front();

        CursorState* cursorState = stateGuard->getCursorState(cursorId);
        uassert(31092,
                str::stream() << "Could not find cursor state associated with cursor id "
                              << cursorId,
                cursorState);
        uassert(31093,
                str::stream() << "Cannot run killCursors on cursor id " << cursorId
                              << " without having run search",
                cursorState->claimed());

        auto cmdResponsePair = cursorState->peekNextCommandResponsePair();
        assertUserCommandMatchesExpectedCommand(cmdObj, cmdResponsePair.expectedCommand);
        result->appendElements(cmdResponsePair.response);

        cursorState->popNextCommandResponsePair();
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Should not have any responses left after killCursors",
                !cursorState->hasNextCursorResponse());
    }
} cmdMongotMockKillCursors;

/*
 * If a command needs two cursor states, this command claims a state without needing an additional
 * history.
 */
class MongotMockAllowMultiCursorResponse final : public MongotMockBaseCmd {
public:
    MongotMockAllowMultiCursorResponse() : MongotMockBaseCmd("allowMultiCursorResponse") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName&,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {

        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());

        uassert(ErrorCodes::InvalidOptions,
                "cursorId should be type NumberLong",
                cmdObj["cursorId"].type() == BSONType::NumberLong);
        const CursorId id = cmdObj["cursorId"].Long();
        uassert(ErrorCodes::InvalidOptions, "cursorId may not equal 0", id != 0);

        std::deque<mongotmock::ExpectedCommandResponsePair> commandResponsePairs;

        uassert(ErrorCodes::InvalidOptions,
                "allowMultiCursorResponse should not have 'history'",
                !cmdObj.hasField("history"));

        // Use an empty response pair, it will be ignored as the response was specified by the
        // original setMockResponse.
        commandResponsePairs.push_back({placeholderCmd, placeholderResponse});

        stateGuard->setStateForId(id,
                                  std::make_unique<CursorState>(std::move(commandResponsePairs)));
    }
} cmdMongotMockAllowMultiCursorResponse;

class MongotMockSetMockResponse final : public MongotMockBaseCmd {
public:
    MongotMockSetMockResponse() : MongotMockBaseCmd("setMockResponses") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName&,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());

        uassert(ErrorCodes::InvalidOptions,
                "cursorId should be type NumberLong",
                cmdObj["cursorId"].type() == BSONType::NumberLong);
        const CursorId id = cmdObj["cursorId"].Long();
        uassert(ErrorCodes::InvalidOptions, "cursorId may not equal 0", id != 0);

        uassert(ErrorCodes::InvalidOptions,
                "'history' should be of type Array",
                cmdObj["history"].type() == BSONType::Array);

        std::deque<mongotmock::ExpectedCommandResponsePair> commandResponsePairs;

        for (auto&& cmdResponsePair : cmdObj["history"].embeddedObject()) {
            uassert(ErrorCodes::InvalidOptions,
                    "Each element of 'history' should be an object",
                    cmdResponsePair.type() == BSONType::Object);
            uassert(ErrorCodes::InvalidOptions,
                    "Each element of 'history' should have an 'expectedCommand' "
                    "field of type object",
                    cmdResponsePair["expectedCommand"].type() == BSONType::Object);
            uassert(ErrorCodes::InvalidOptions,
                    "Each element of 'history' should have a 'response' field of "
                    "type object",
                    cmdResponsePair["response"].type() == BSONType::Object);

            commandResponsePairs.push_back(
                {cmdResponsePair["expectedCommand"].embeddedObject().getOwned(),
                 cmdResponsePair["response"].embeddedObject().getOwned()});
        }
        uassert(ErrorCodes::InvalidOptions,
                "'history' should not be empty",
                !commandResponsePairs.empty());

        stateGuard->setStateForId(id,
                                  std::make_unique<CursorState>(std::move(commandResponsePairs)));
    }
} cmdMongotMockSetMockResponse;

/**
 * Command to check if there are any remaining queued responses.
 */
class MongotMockGetQueuedResponses final : public MongotMockBaseCmd {
public:
    MongotMockGetQueuedResponses() : MongotMockBaseCmd("getQueuedResponses") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName&,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {

        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());

        size_t remainingQueuedResponses = 0;
        const auto& cursorMap = stateGuard->getCursorMap();
        for (auto&& cursorIdCursorStatePair : cursorMap) {
            BSONArrayBuilder arrBuilder(
                result->subobjStart(str::stream() << "cursorID " << cursorIdCursorStatePair.first));

            auto& remainingResponses = cursorIdCursorStatePair.second->getRemainingResponses();
            for (auto&& remainingResponse : remainingResponses) {
                ++remainingQueuedResponses;

                BSONObjBuilder objBuilder(arrBuilder.subobjStart());
                objBuilder.append("expectedCommand", remainingResponse.expectedCommand);
                objBuilder.append("response", remainingResponse.response);
                objBuilder.doneFast();
            }

            arrBuilder.doneFast();
        }

        result->append("numRemainingResponses", static_cast<int>(remainingQueuedResponses));
    }
} cmdMongotMockGetQueuedResponses;


}  // namespace
}  // namespace mongo
