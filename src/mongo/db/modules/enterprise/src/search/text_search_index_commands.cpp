/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongot_task_executor.h"
#include "search/manage_search_index_request_gen.h"
#include "search/search_index_options.h"
#include "search/search_index_options_gen.h"
#include "search/text_search_index_commands_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo {
namespace {

/**
 * Takes the input for a ManageSearchIndexRequest and turns it into a RemoteCommandRequest targeting
 * the remote search index management endpoint.
 */
executor::RemoteCommandRequest createManageSearchIndexRemoteCommandRequest(
    OperationContext* opCtx, const NamespaceString& nss, const UUID& uuid, const BSONObj& userCmd) {
    // Fetch the search index management host and port.
    invariant(!globalSearchIndexParams.host.empty());
    auto swHostAndPort = HostAndPort::parse(globalSearchIndexParams.host);
    // This host and port string is configured and validated at startup.
    invariant(swHostAndPort.getStatus().isOK());

    // Format the command request.
    ManageSearchIndexRequest manageSearchIndexRequest;
    manageSearchIndexRequest.setManageSearchIndex(nss.coll());
    manageSearchIndexRequest.setCollectionUUID(uuid);
    manageSearchIndexRequest.setUserCommand(userCmd);

    // Create a RemoteCommandRequest with the request and host-and-port.
    executor::RemoteCommandRequest remoteManageSearchIndexRequest(
        executor::RemoteCommandRequest(swHostAndPort.getValue(),
                                       nss.dbName().toStringWithTenantId(),
                                       manageSearchIndexRequest.toBSON(),
                                       opCtx));
    remoteManageSearchIndexRequest.sslMode = transport::ConnectSSLMode::kDisableSSL;
    return remoteManageSearchIndexRequest;
}

/**
 * Runs a ManageSearchIndex command request against the remote search index management endpoint.
 * Passes the remote command response data back to the caller if the status is OK, otherwise throws
 * if the command failed.
 */
BSONObj getSearchIndexManagerResponse(OperationContext* opCtx,
                                      const NamespaceString& nss,
                                      const UUID& uuid,
                                      const BSONObj& userCmd) {
    // Create the RemoteCommandRequest.
    auto request = createManageSearchIndexRemoteCommandRequest(opCtx, nss, uuid, userCmd);
    auto [promise, future] = makePromiseFuture<executor::TaskExecutor::RemoteCommandCallbackArgs>();
    auto promisePtr = std::make_shared<Promise<executor::TaskExecutor::RemoteCommandCallbackArgs>>(
        std::move(promise));

    // Schedule and run the RemoteCommandRequest on the TaskExecutor.
    auto taskExecutor = executor::getMongotTaskExecutor(opCtx->getServiceContext());
    auto scheduleResult = taskExecutor->scheduleRemoteCommand(
        std::move(request), [promisePtr](const auto& args) { promisePtr->emplaceValue(args); });
    if (!scheduleResult.isOK()) {
        // Since the command failed to be scheduled, the callback above did not and will not run.
        // Thus, it is safe to fulfill the promise here without worrying about synchronizing access
        // with the executor's thread.
        promisePtr->setError(scheduleResult.getStatus());
    }

    auto response = future.getNoThrow(opCtx);
    try {
        // Pull out the command response. Throw if the command did not reach the remote server.
        uassertStatusOK(response.getStatus());
        uassertStatusOK(response.getValue().response.status);
    } catch (const ExceptionFor<ErrorCodes::HostUnreachable>&) {
        // Don't expose the remote server host-and-port information to clients.
        uasserted(ErrorCodes::HostUnreachable,
                  "Error connecting to Search Index Management service.");
    }
    BSONObj responseData = response.getValue().response.data;

    // Check the command response for an error and throw if there is one.
    uassertStatusOK(getStatusFromCommandResult(responseData));

    // Return the successful command data to the caller.
    return responseData.getOwned();
}


/**
 * Check that the 'searchIndexManagementHostAndPort' server parameter has been set.
 * The search index commands are only allowed to run with external search index management.
 */
void throwIfNotRunningWithRemoteSearchIndexManagement() {
    auto& managementHost = globalSearchIndexParams.host;
    uassert(ErrorCodes::CommandNotSupported,
            str::stream() << "Search index commands are only supported with Atlas.",
            !managementHost.empty());
}

/**
 * Returns the collection UUID or throws a NamespaceNotFound error.
 */
UUID fetchCollectionUUIDOrThrow(OperationContext* opCtx, const NamespaceString& nss) {
    auto optUuid = CollectionCatalog::get(opCtx)->lookupUUIDByNSS(opCtx, nss);
    uassert(ErrorCodes::NamespaceNotFound,
            str::stream() << "Collection '" << nss << "' does not exist.",
            optUuid);
    return optUuid.get();
}

/**
 * Passthrough command to the search index management endpoint on which the manageSearchIndex
 * command is called. Accepts requests of the IDL form createSearchIndexes.
 *
 * {
 *     createSearchIndexes: "<collection-name>",
 *     $db: "<database-name>",
 *     indexes: [
 *         {
 *             name: "<index-name>",
 *             definition: {
 *                 // search index definition fields
 *             }
 *         }
 *     ]
 * }
 *
 */
class CmdCreateSearchIndexesCommand final : public TypedCommand<CmdCreateSearchIndexesCommand> {
public:
    using Request = CreateSearchIndexesCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    std::string help() const override {
        return "Command to create a search index. Only supported with Atlas.";
    }

    ReadWriteType getReadWriteType() const override {
        return ReadWriteType::kWrite;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        bool supportsWriteConcern() const final {
            return false;
        }

        NamespaceString ns() const final {
            return request().getNamespace();
        }

        CreateSearchIndexesReply typedRun(OperationContext* opCtx) {
            throwIfNotRunningWithRemoteSearchIndexManagement();

            const auto& cmd = request();
            const auto& nss = cmd.getNamespace();

            auto collectionUUID = fetchCollectionUUIDOrThrow(opCtx, nss);

            // Run the search index command against the remote search index management server.
            BSONObj manageSearchIndexResponse = getSearchIndexManagerResponse(
                opCtx, nss, collectionUUID, cmd.toBSON(BSONObj() /* commandPassthroughFields */));

            IDLParserContext ctx("CreateSearchIndexesReply Parser");
            return CreateSearchIndexesReply::parseOwned(ctx, std::move(manageSearchIndexResponse));
        }

    private:
        void doCheckAuthorization(OperationContext* opCtx) const override {
            const NamespaceString& nss = request().getNamespace();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << "Not authorized to call createSearchIndexes on collection "
                                  << nss,
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(nss, ActionType::createSearchIndexes));
        }
    };
} cmdCreateSearchIndexesCommand;

/**
 * Passthrough command to the search index management endpoint on which the manageSearchIndex
 * command is called. Accepts requests of the form:
 *
 * {
 *     dropSearchIndex: "<collection-name>",
 *     $db: "<database-name>",
 *     indexId: "<index-Id>"   // Only indexId or name may be specified, both is not accepted.
 *     name: "<index-name>"
 * }
 *
 */
class CmdDropSearchIndexCommand final : public TypedCommand<CmdDropSearchIndexCommand> {
public:
    using Request = DropSearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    std::string help() const override {
        return "Command to drop a search index. Only supported with Atlas.";
    }

    ReadWriteType getReadWriteType() const override {
        return ReadWriteType::kWrite;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        bool supportsWriteConcern() const final {
            return false;
        }

        NamespaceString ns() const final {
            return request().getNamespace();
        }

        DropSearchIndexReply typedRun(OperationContext* opCtx) {
            throwIfNotRunningWithRemoteSearchIndexManagement();

            const auto& cmd = request();

            uassert(ErrorCodes::InvalidOptions,
                    "Cannot set both 'name' and 'indexID'.",
                    !(cmd.getName() && cmd.getIndexID()));

            const auto& nss = cmd.getNamespace();

            auto collectionUUID = fetchCollectionUUIDOrThrow(opCtx, nss);

            BSONObj manageSearchIndexResponse = getSearchIndexManagerResponse(
                opCtx, nss, collectionUUID, cmd.toBSON(BSONObj() /* commandPassthroughFields */));

            IDLParserContext ctx("DropSearchIndexReply Parser");
            return DropSearchIndexReply::parseOwned(ctx, std::move(manageSearchIndexResponse));
        }

    private:
        void doCheckAuthorization(OperationContext* opCtx) const override {
            const NamespaceString& nss = request().getNamespace();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << "Not authorized to call dropSearchIndex on collection " << nss,
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(nss, ActionType::dropSearchIndex));
        }
    };
} cmdDropSearchIndexCommand;

/**
 * Passthrough command to the search index management endpoint on which the manageSearchIndex
 * command is called. Accepts requests of the form:
 *
 * {
 *     updateSearchIndex: "<collection name>",
 *     $db: "<database name>",
 *     indexId: "<index Id>",  // Only indexId or name may be specified, both is not accepted.
 *     name: "<index name>",
 *     indexDefinition: {
 *         // search index definition fields
 *     }
 * }
 *
 */
class CmdUpdateSearchIndexCommand final : public TypedCommand<CmdUpdateSearchIndexCommand> {
public:
    using Request = UpdateSearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    std::string help() const override {
        return "Command to update a search index. Only supported with Atlas.";
    }

    ReadWriteType getReadWriteType() const override {
        return ReadWriteType::kWrite;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        bool supportsWriteConcern() const final {
            return false;
        }

        NamespaceString ns() const final {
            return request().getNamespace();
        }

        UpdateSearchIndexReply typedRun(OperationContext* opCtx) {
            throwIfNotRunningWithRemoteSearchIndexManagement();

            const auto& cmd = request();
            cmd.getName();

            uassert(ErrorCodes::InvalidOptions,
                    "Cannot set both 'name' and 'indexID'.",
                    !(cmd.getName() && cmd.getIndexID()));

            uassert(ErrorCodes::InvalidOptions,
                    "Must set either 'name' or 'indexID'.",
                    cmd.getName() || cmd.getIndexID());

            const auto& nss = cmd.getNamespace();

            auto collectionUUID = fetchCollectionUUIDOrThrow(opCtx, nss);

            BSONObj manageSearchIndexResponse = getSearchIndexManagerResponse(
                opCtx, nss, collectionUUID, cmd.toBSON(BSONObj() /* commandPassthroughFields */));

            IDLParserContext ctx("UpdateSearchIndexReply Parser");
            return UpdateSearchIndexReply::parseOwned(ctx, std::move(manageSearchIndexResponse));
        }

    private:
        void doCheckAuthorization(OperationContext* opCtx) const override {
            const NamespaceString& nss = request().getNamespace();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << "Not authorized to call updateSearchIndex on collection "
                                  << nss,
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(nss, ActionType::updateSearchIndex));
        }
    };
} cmdUpdateSearchIndexCommand;

/**
 * Passthrough command to the search index management endpoint on which the manageSearchIndex
 * command is called. Accepts requests of the form:
 *
 * {
 *     listSearchIndexes: "<collection-name>",
 *     $db: "<database-name>",
 *     indexId: "<index-Id>",
 *     name: "<index-name>"
 * }
 *
 * indexId and name are optional. Both cannot be specified at the same time. If neither of them are
 * specified, then all indexes are returned for the collection.
 *
 * The command returns a 'cursor' field like listIndexes, but the cursorId will always be 0,
 * indicating there is no more data to fetch than that which is returned in the first batch.
 * The response created by the remote search index management host should look something like this:
 *
 * {
 *   ok: 1,
 *   cursor: {
 *     id: Long("0"),
 *     ns: "<database name>.<collection name>",
 *     firstBatch: [
 *       {
 *         indexId: "<index Id>",
 *         name: "<index name>",
 *         status: "INITIAL SYNC",
 *         indexDefinition: {
 *           mappings: {
 *             dynamic: true,
 *           }
 *         }
 *       },
 *       {
 *         indexId: "<index Id>",
 *         name: "<index name>",
 *         status: "ACTIVE",
 *         indexDefinition: {
 *           mappings: {
 *             dynamic: true,
 *           },
 *           synonyms: [<synonym mapping>]
 *         }
 *       }
 *     ]
 *   }
 * }
 *
 */
class CmdListSearchIndexesCommand final : public TypedCommand<CmdListSearchIndexesCommand> {
public:
    using Request = ListSearchIndexesCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    std::string help() const override {
        return "Command to list search indexes. Only supported with Atlas.";
    }

    ReadWriteType getReadWriteType() const override {
        return ReadWriteType::kWrite;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        bool supportsWriteConcern() const final {
            return false;
        }

        NamespaceString ns() const final {
            return request().getNamespace();
        }

        ListSearchIndexesReply typedRun(OperationContext* opCtx) {
            throwIfNotRunningWithRemoteSearchIndexManagement();

            const auto& cmd = request();

            uassert(ErrorCodes::InvalidOptions,
                    "Cannot set both 'name' and 'indexID'.",
                    !(cmd.getName() && cmd.getIndexID()));

            const auto& nss = cmd.getNamespace();

            auto collectionUUID = fetchCollectionUUIDOrThrow(opCtx, nss);

            BSONObj manageSearchIndexResponse = getSearchIndexManagerResponse(
                opCtx, nss, collectionUUID, cmd.toBSON(BSONObj() /* commandPassthroughFields */));

            IDLParserContext ctx("ListSearchIndexesReply Parser");
            return ListSearchIndexesReply::parseOwned(ctx, std::move(manageSearchIndexResponse));
        }

    private:
        void doCheckAuthorization(OperationContext* opCtx) const override {
            const NamespaceString& nss = request().getNamespace();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << "Not authorized to call listSearchIndexes on collection "
                                  << nss,
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(nss, ActionType::listSearchIndexes));
        }
    };
} cmdListSearchIndexesCommand;

}  // namespace
}  // namespace mongo
