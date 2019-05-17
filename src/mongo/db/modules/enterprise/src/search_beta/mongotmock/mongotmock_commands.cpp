/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/getmore_request.h"
#include "mongo/db/query/killcursors_request.h"
#include "mongotmock_state.h"

namespace mongo {
namespace {

using mongotmock::MongotMockStateGuard;
using mongotmock::CursorState;
using mongotmock::getMongotMockState;

// Do a "loose" check that every field in 'expectedCmd' is in 'userCmd'. Does not check in the
// other direction.
void checkUserCommandMatchesExpectedCommand(BSONObj userCmd, BSONObj expectedCmd) {
    // Check that the given command matches the expected command's values.
    for (auto&& elem : expectedCmd) {
        uassert(31086,
                str::stream() << "Expected command matching " << expectedCmd << " but got "
                              << userCmd,
                SimpleBSONElementComparator::kInstance.evaluate(
                    userCmd[elem.fieldNameStringData()] == elem));
    }
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
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) final {
        processCommand(opCtx, dbname, cmdObj, &result);
        return true;
    }

    virtual void processCommand(OperationContext* opCtx,
                                const std::string& dbname,
                                const BSONObj& cmdObj,
                                BSONObjBuilder* result) const = 0;
};

class MongotMockSearchBeta final : public MongotMockBaseCmd {
public:
    MongotMockSearchBeta() : MongotMockBaseCmd("searchBeta") {}

    void processCommand(OperationContext* opCtx,
                        const std::string& dbname,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        MongotMockStateGuard stateGuard = getMongotMockState(opCtx->getServiceContext());

        CursorState* state = stateGuard->claimAvailableState();
        uassert(31094,
                str::stream() << "Cannot run searchBeta as there are no remaining unclaimed mock "
                                 "cursor states. Received command: "
                              << cmdObj,
                state);

        // We should not have allowed an empty 'history'.
        invariant(state->hasNextCursorResponse());
        auto cmdResponsePair = state->peekNextCommandResponsePair();

        checkUserCommandMatchesExpectedCommand(cmdObj, cmdResponsePair.expectedCommand);

        // Return the queued response.
        result->appendElements(cmdResponsePair.response);

        // Pop the first response.
        state->popNextCommandResponsePair();
    }
} cmdMongotMockSearchBeta;


class MongotMockGetMore final : public MongotMockBaseCmd {
public:
    MongotMockGetMore() : MongotMockBaseCmd("getMore") {}

    void processCommand(OperationContext* opCtx,
                        const std::string& dbname,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        auto request = uassertStatusOK(GetMoreRequest::parseFromBSON(dbname, cmdObj));

        const auto cursorId = request.cursorid;
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
                              << "without having run searchBeta",
                cursorState->claimed());

        auto cmdResponsePair = cursorState->peekNextCommandResponsePair();
        checkUserCommandMatchesExpectedCommand(cmdObj, cmdResponsePair.expectedCommand);
        result->appendElements(cmdResponsePair.response);

        cursorState->popNextCommandResponsePair();
    }
} cmdMongotMockGetMore;


class MongotMockKillCursors final : public MongotMockBaseCmd {
public:
    MongotMockKillCursors() : MongotMockBaseCmd("killCursors") {}

    void processCommand(OperationContext* opCtx,
                        const std::string& dbname,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        auto request = uassertStatusOK(KillCursorsRequest::parseFromBSON(dbname, cmdObj));

        const auto& cursorList = request.cursorIds;
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
                              << "without having run searchBeta",
                cursorState->claimed());

        auto cmdResponsePair = cursorState->peekNextCommandResponsePair();
        checkUserCommandMatchesExpectedCommand(cmdObj, cmdResponsePair.expectedCommand);
        result->appendElements(cmdResponsePair.response);

        cursorState->popNextCommandResponsePair();
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Should not have any responses left after killCursors",
                !cursorState->hasNextCursorResponse());
    }
} cmdMongotMockKillCursors;

class MongotMockSetMockResponse final : public MongotMockBaseCmd {
public:
    MongotMockSetMockResponse() : MongotMockBaseCmd("setMockResponses") {}

    void processCommand(OperationContext* opCtx,
                        const std::string& dbname,
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
                        const std::string& dbname,
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

        result->append("numRemainingResponses", static_cast<unsigned>(remainingQueuedResponses));
    }
} cmdMongotMockGetQueuedResponses;


}  // namespace
}  // namespace mongo
