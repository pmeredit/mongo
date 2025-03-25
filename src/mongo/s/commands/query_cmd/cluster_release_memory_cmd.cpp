/**
 *    Copyright (C) 2025-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "mongo/base/status.h"
#include "mongo/db/auth/authorization_checks.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/client_cursor/cursor_id.h"
#include "mongo/db/query/client_cursor/release_memory_gen.h"
#include "mongo/s/grid.h"
#include "mongo/s/query/exec/cluster_cursor_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo {

namespace {

MONGO_FAIL_POINT_DEFINE(failReleaseMemoryAfterCursorCheckout);


class ClusterReleaseMemoryCmd final : public TypedCommand<ClusterReleaseMemoryCmd> {
public:
    using Request = ReleaseMemoryCommandRequest;

    const std::set<std::string>& apiVersions() const final {
        return kNoApiVersions;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const final {
        return false;
    }

    bool allowedInTransactions() const final {
        return true;
    }

    bool allowedWithSecurityToken() const final {
        return true;
    }

    bool enableDiagnosticPrintingOnFailure() const final {
        return true;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        ReleaseMemoryCommandReply typedRun(OperationContext* opCtx) {
            ReleaseMemoryCommandRequest request = this->request();

            auto cursorManager = Grid::get(opCtx)->getCursorManager();
            auto cursorIds = request.getCommandParameter();

            std::vector<CursorId> cursorsReleased;
            std::vector<CursorId> cursorsNotFound;
            std::vector<CursorId> cursorsCurrentlyPinned;

            for (CursorId id : cursorIds) {
                auto pinnedCursor = cursorManager->checkOutCursorNoAuthCheck(id, opCtx);

                if (pinnedCursor.isOK()) {
                    ScopeGuard returnCursorGuard([&pinnedCursor] {
                        pinnedCursor.getValue().returnCursor(
                            ClusterCursorManager::CursorState::NotExhausted);
                    });
                    {
                        // If the 'failGetMoreAfterCursorCheckout' failpoint is enabled, throw an
                        // exception with the given 'errorCode' value, or ErrorCodes::InternalError
                        // if 'errorCode' is omitted.
                        auto nss = ns();
                        failReleaseMemoryAfterCursorCheckout.executeIf(
                            [](const BSONObj& data) {
                                auto errorCode =
                                    (data["errorCode"] ? data["errorCode"].safeNumberLong()
                                                       : ErrorCodes::InternalError);
                                uasserted(
                                    errorCode,
                                    "Hit the 'failReleaseMemoryAfterCursorCheckout' failpoint");
                            },
                            [&opCtx, nss](const BSONObj& data) {
                                auto dataForFailCommand = data.addField(
                                    BSON("failCommands" << BSON_ARRAY("releaseMemory"))
                                        .firstElement());
                                auto* command = CommandHelpers::findCommand(opCtx, "releaseMemory");
                                return CommandHelpers::shouldActivateFailCommandFailPoint(
                                    dataForFailCommand, nss, command, opCtx->getClient());
                            });
                    }
                    uassertStatusOK(pinnedCursor.getValue()->releaseMemory());
                    cursorsReleased.push_back(id);
                    // Upon successful completion, transfer ownership of the cursor back to the
                    // cursor manager.
                    pinnedCursor.getValue().returnCursor(
                        ClusterCursorManager::CursorState::NotExhausted);
                    returnCursorGuard.dismiss();

                } else if (pinnedCursor.getStatus().code() == ErrorCodes::CursorInUse) {
                    cursorsCurrentlyPinned.push_back(id);
                } else if (pinnedCursor.getStatus().code() == ErrorCodes::CursorNotFound) {
                    cursorsNotFound.push_back(id);
                } else {
                    uassertStatusOK(pinnedCursor.getStatus());
                }
            }

            return ReleaseMemoryCommandReply{std::move(cursorsReleased),
                                             std::move(cursorsNotFound),
                                             std::move(cursorsCurrentlyPinned)};
        }

    private:
        bool supportsWriteConcern() const override {
            return false;
        }

        NamespaceString ns() const override {
            return NamespaceString::makeCommandNamespace(db());
        }

        const DatabaseName& db() const override {
            return request().getDbName();
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {

            auto cursorIds = this->request().getCommandParameter();

            // Check each cursor to verify that it has privileges to release memory on it.
            for (CursorId id : cursorIds) {
                auto const authzSession = AuthorizationSession::get(opCtx->getClient());
                ReleaseMemoryAuthzCheckFn authChecker =
                    [&authzSession](ReleaseMemoryAuthzCheckFnInputType ns) -> Status {
                    return auth::checkAuthForReleaseMemory(authzSession, ns);
                };

                auto status =
                    Grid::get(opCtx)->getCursorManager()->checkAuthCursor(opCtx, id, authChecker);

                // audit::logReleaseMemoryAuthzCheck(opCtx->getClient(), nss, id, status.code());
                if (!status.isOK()) {
                    if (status.code() == ErrorCodes::CursorNotFound) {
                        // Not found isn't an authorization issue.
                        // run() will raise it as a return value.
                        continue;
                    }
                    uassertStatusOK(status);  // throws
                }
            }
        }
    };
};

MONGO_REGISTER_COMMAND(ClusterReleaseMemoryCmd).forRouter();


}  // namespace
}  // namespace mongo
