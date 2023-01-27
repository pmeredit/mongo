/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/logv2/log.h"
#include "search/text_search_index_commands_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo {
namespace {

class CmdCreateSearchIndexCommand final : public TypedCommand<CmdCreateSearchIndexCommand> {
public:
    using Request = CreateSearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
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

        CreateSearchIndexReply typedRun(OperationContext* opCtx) {
            CreateSearchIndexReply reply;
            return reply;
        }

    private:
        void doCheckAuthorization(OperationContext* opCtx) const override {
            const NamespaceString& nss = request().getNamespace();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << "Not authorized to call createSearchIndex on collection "
                                  << nss,
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(nss, ActionType::createSearchIndex));
        }
    };
} cmdCreateSearchIndexCommand;

class CmdDropSearchIndexCommand final : public TypedCommand<CmdDropSearchIndexCommand> {
public:
    using Request = DropSearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
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
            DropSearchIndexReply reply;
            return reply;
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

class CmdModifySearchIndexCommand final : public TypedCommand<CmdModifySearchIndexCommand> {
public:
    using Request = ModifySearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    std::string help() const override {
        return "Command to modify a search index. Only supported with Atlas.";
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

        ModifySearchIndexReply typedRun(OperationContext* opCtx) {
            ModifySearchIndexReply reply;
            return reply;
        }

    private:
        void doCheckAuthorization(OperationContext* opCtx) const override {
            const NamespaceString& nss = request().getNamespace();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << "Not authorized to call modifySearchIndex on collection "
                                  << nss,
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnNamespace(nss, ActionType::modifySearchIndex));
        }
    };
} cmdModifySearchIndexCommand;

class CmdListSearchIndexesCommand final : public TypedCommand<CmdListSearchIndexesCommand> {
public:
    using Request = ListSearchIndexesCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
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
            ListSearchIndexesReply reply;
            return reply;
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
