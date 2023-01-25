/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "search/text_search_index_commands_gen.h"

namespace mongo {
namespace {

class CmdCreateSearchIndexCommand final : public TypedCommand<CmdCreateSearchIndexCommand> {
public:
    using Request = CreateSearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    std::string help() const override {
        return "Command to create a text-search index. Only supported with Atlas.";
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
        void doCheckAuthorization(OperationContext* opCtx) const override {}
    };
} cmdCreateSearchIndexCommand;

class CmdDropSearchIndexCommand final : public TypedCommand<CmdDropSearchIndexCommand> {
public:
    using Request = DropSearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    std::string help() const override {
        return "Command to drop a text-search index. Only supported with Atlas.";
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
        void doCheckAuthorization(OperationContext* opCtx) const override {}
    };
} cmdDropSearchIndexCommand;

class CmdModifySearchIndexCommand final : public TypedCommand<CmdModifySearchIndexCommand> {
public:
    using Request = ModifySearchIndexCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    std::string help() const override {
        return "Command to modify a text-search index. Only supported with Atlas.";
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
        void doCheckAuthorization(OperationContext* opCtx) const override {}
    };
} cmdModifySearchIndexCommand;

class CmdListSearchIndexesCommand final : public TypedCommand<CmdListSearchIndexesCommand> {
public:
    using Request = ListSearchIndexesCommand;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    std::string help() const override {
        return "Command to list text-search indexes. Only supported with Atlas.";
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
        void doCheckAuthorization(OperationContext* opCtx) const override {}
    };
} cmdListSearchIndexesCommand;

}  // namespace
}  // namespace mongo
