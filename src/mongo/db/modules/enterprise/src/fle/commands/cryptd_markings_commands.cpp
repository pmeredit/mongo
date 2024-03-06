/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/api_parameters.h"
#include "mongo/db/commands.h"
#include "mongo/db/explain_gen.h"

#include "../query_analysis/query_analysis.h"

namespace mongo {
namespace {
constexpr auto kExplainField = "explain"_sd;
constexpr auto kResultField = "result"_sd;
constexpr auto kVerbosityField = "verbosity"_sd;

/**
 * For explain we need to re-wrap the inner command with placeholders inside an explain
 * command.
 */
void buildExplainReturnMessage(OperationContext* opCtx,
                               BSONObjBuilder* responseBuilder,
                               const BSONObj& innerObj,
                               const ExplainOptions::Verbosity& verbosity) {
    // All successful commands have a result field.
    invariant(innerObj.hasField(kResultField) &&
              innerObj.getField(kResultField).type() == BSONType::Object);
    for (auto&& elem : innerObj) {
        if (elem.fieldNameStringData() == kResultField) {
            // Hoist "result" up into result.explain.
            BSONObjBuilder result(responseBuilder->subobjStart(kResultField));
            result.append(kExplainField, elem.Obj());

            // TODO: SERVER-40354 Only send back verbosity if it was sent in the original message.
            result.append(kVerbosityField, ExplainOptions::verbosityString(verbosity));

            // Add apiVersion field to reply if it was provided by the client.
            if (auto apiVersion = APIParameters::get(opCtx).getAPIVersion()) {
                result.append(APIParametersFromClient::kApiVersionFieldName, apiVersion.value());
            }

            result.doneFast();
        } else {
            responseBuilder->append(elem);
        }
    }
}

/**
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CryptDPlaceholder : public BasicCommand {
public:
    CryptDPlaceholder(StringData name, StringData oldName = StringData())
        : BasicCommand(name, oldName) {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        MONGO_UNREACHABLE;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        MONGO_UNREACHABLE;
    }

    Status explain(OperationContext* opCtx,
                   const OpMsgRequest& request,
                   ExplainOptions::Verbosity verbosity,
                   rpc::ReplyBuilderInterface* result) const final {
        try {
            BSONObjBuilder innerBuilder;
            processCommand(opCtx,
                           DatabaseName(request.getValidatedTenantId(), request.getDatabase()),
                           request.body,
                           &innerBuilder);
            auto explainBuilder = result->getBodyBuilder();
            buildExplainReturnMessage(opCtx, &explainBuilder, innerBuilder.obj(), verbosity);
        } catch (...) {
            return exceptionToStatus();
        }

        return Status::OK();
    }

    Status checkAuthForOperation(OperationContext*,
                                 const DatabaseName&,
                                 const BSONObj&) const final {
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
                                const DatabaseName& dbname,
                                const BSONObj& cmdObj,
                                BSONObjBuilder* result) const = 0;
};


class CryptDFind final : public CryptDPlaceholder {
public:
    CryptDFind() : CryptDPlaceholder("find") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processFindCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDFind;


class CryptDAggregate final : public CryptDPlaceholder {
public:
    CryptDAggregate() : CryptDPlaceholder("aggregate") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processAggregateCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDAggregate;


class CryptDDistinct final : public CryptDPlaceholder {
public:
    CryptDDistinct() : CryptDPlaceholder("distinct") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processDistinctCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDDistinct;

class CryptDCount final : public CryptDPlaceholder {
public:
    CryptDCount() : CryptDPlaceholder("count") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processCountCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDCount;


class CryptDFindAndModify final : public CryptDPlaceholder {
public:
    CryptDFindAndModify() : CryptDPlaceholder("findAndModify", "findandmodify") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processFindAndModifyCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDFindAndModify;

class CryptDCreate final : public CryptDPlaceholder {
public:
    CryptDCreate() : CryptDPlaceholder("create") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processCreateCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDCreate;

class CryptDCollMod final : public CryptDPlaceholder {
public:
    CryptDCollMod() : CryptDPlaceholder("collMod") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processCollModCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDCollMod;

class CryptDCreateIndexes final : public CryptDPlaceholder {
public:
    CryptDCreateIndexes() : CryptDPlaceholder("createIndexes") {}

    void processCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* result) const final {
        query_analysis::processCreateIndexesCommand(
            opCtx, dbName, cmdObj, result, CommandHelpers::parseNsFromCommand(dbName, cmdObj));
    }
} cmdCryptDCreateIndexes;


/**
 * A command for running write ops.
 */
class CryptDWriteOp : public Command {
public:
    CryptDWriteOp(StringData name) : Command(name) {}

    AllowedOnSecondary secondaryAllowed(ServiceContext* context) const final {
        MONGO_UNREACHABLE;
    }

    bool maintenanceOk() const final {
        MONGO_UNREACHABLE;
    }

    bool adminOnly() const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        MONGO_UNREACHABLE;
    }

    LogicalOp getLogicalOp() const final {
        MONGO_UNREACHABLE;
    }

    ReadWriteType getReadWriteType() const final {
        MONGO_UNREACHABLE;
    }

    std::size_t reserveBytesForReply() const final {
        MONGO_UNREACHABLE;
    }

    bool shouldAffectCommandCounter() const final {
        MONGO_UNREACHABLE;
    }

    class InvocationBase : public CommandInvocation {
    public:
        InvocationBase(const CryptDWriteOp* definition,
                       const OpMsgRequest& request,
                       const DatabaseName& dbName)
            : CommandInvocation(definition), _request(request), _dbName(dbName) {}

    protected:
        NamespaceString ns() const final {
            return NamespaceString(CommandHelpers::parseNsFromCommand(_dbName, _request.body));
        }

    private:
        bool supportsWriteConcern() const final {
            MONGO_UNREACHABLE;
        }

        ReadConcernSupportResult supportsReadConcern(repl::ReadConcernLevel level,
                                                     bool isImplicitDefault) const final {
            MONGO_UNREACHABLE;
        }

        bool allowsSpeculativeMajorityReads() const final {
            MONGO_UNREACHABLE;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {}

        void explain(OperationContext* opCtx,
                     ExplainOptions::Verbosity verbosity,
                     rpc::ReplyBuilderInterface* result) final {
            BSONObjBuilder innerBuilder;
            processWriteCommand(opCtx, _request, &innerBuilder);
            auto explainBuilder = result->getBodyBuilder();
            buildExplainReturnMessage(opCtx, &explainBuilder, innerBuilder.obj(), verbosity);
        }


        void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* result) final {

            auto builder = result->getBodyBuilder();

            processWriteCommand(opCtx, _request, &builder);
        }


        virtual void processWriteCommand(OperationContext* opCtx,
                                         const OpMsgRequest& request,
                                         BSONObjBuilder* builder) = 0;


    private:
        const OpMsgRequest& _request;
        const DatabaseName _dbName;
    };
};


class CryptDInsertCmd final : public CryptDWriteOp {
public:
    CryptDInsertCmd() : CryptDWriteOp("insert") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) final {
        return std::make_unique<Invocation>(this, opMsgRequest, opMsgRequest.getDatabase());
    }

    class Invocation : public CryptDWriteOp::InvocationBase {
    public:
        Invocation(const CryptDInsertCmd* definition,
                   const OpMsgRequest& request,
                   const DatabaseName& dbName)
            : InvocationBase(definition, request, dbName) {}


        void processWriteCommand(OperationContext* opCtx,
                                 const OpMsgRequest& request,
                                 BSONObjBuilder* builder) final {
            query_analysis::processInsertCommand(opCtx, request, builder, ns());
        }
    };

} insertCmd;


class CryptDUpdateCmd final : public CryptDWriteOp {
public:
    CryptDUpdateCmd() : CryptDWriteOp("update") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) final {
        return std::make_unique<Invocation>(this, opMsgRequest, opMsgRequest.getDatabase());
    }

    class Invocation : public CryptDWriteOp::InvocationBase {
    public:
        Invocation(const CryptDUpdateCmd* definition,
                   const OpMsgRequest& request,
                   const DatabaseName& dbName)
            : InvocationBase(definition, request, dbName) {}


        void processWriteCommand(OperationContext* opCtx,
                                 const OpMsgRequest& request,
                                 BSONObjBuilder* builder) final {
            query_analysis::processUpdateCommand(opCtx, request, builder, ns());
        }
    };

} updateCmd;


class CryptDDeleteCmd final : public CryptDWriteOp {
public:
    CryptDDeleteCmd() : CryptDWriteOp("delete") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) final {
        return std::make_unique<Invocation>(this, opMsgRequest, opMsgRequest.getDatabase());
    }

    class Invocation : public CryptDWriteOp::InvocationBase {
    public:
        Invocation(const CryptDDeleteCmd* definition,
                   const OpMsgRequest& request,
                   const DatabaseName& dbName)
            : InvocationBase(definition, request, dbName) {}


        void processWriteCommand(OperationContext* opCtx,
                                 const OpMsgRequest& request,
                                 BSONObjBuilder* builder) final {
            query_analysis::processDeleteCommand(opCtx, request, builder, ns());
        }
    };

} deleteCmd;


/**
 * The explain command in mongod checks the replication coordinator and so cryptd uses its own
 * version of explain.
 */
class CryptdExplainCmd final : public Command {
public:
    CryptdExplainCmd() : Command("explain") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& request) override;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kOptIn;
    }

    bool maintenanceOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return false;
    }

    std::string help() const override {
        return "explain database reads and writes";
    }

private:
    class Invocation;
} cryptdExplainCmd;

class CryptdExplainCmd::Invocation final : public CommandInvocation {
public:
    Invocation(const CryptdExplainCmd* explainCommand,
               const OpMsgRequest& request,
               ExplainOptions::Verbosity verbosity,
               std::unique_ptr<OpMsgRequest> innerRequest,
               std::unique_ptr<CommandInvocation> innerInvocation)
        : CommandInvocation(explainCommand),
          _outerRequest{&request},
          _dbName{_outerRequest->getDatabase()},
          _ns{CommandHelpers::parseNsFromCommand(_dbName, _outerRequest->body)},
          _verbosity{std::move(verbosity)},
          _innerRequest{std::move(innerRequest)},
          _innerInvocation{std::move(innerInvocation)} {}

private:
    void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* result) override {
        _innerInvocation->explain(opCtx, _verbosity, result);
    }

    void explain(OperationContext* opCtx,
                 ExplainOptions::Verbosity verbosity,
                 rpc::ReplyBuilderInterface* result) override {
        uasserted(ErrorCodes::IllegalOperation, "Explain cannot explain itself.");
    }

    NamespaceString ns() const override {
        return _ns;
    }

    bool supportsWriteConcern() const override {
        return false;
    }

    /**
     * You are authorized to run an explain if you are authorized to run
     * the command that you are explaining. The auth check is performed recursively
     * on the nested command.
     */
    void doCheckAuthorization(OperationContext* opCtx) const override {
        _innerInvocation->checkAuthorization(opCtx, *_innerRequest);
    }

    const CryptdExplainCmd* command() const {
        return static_cast<const CryptdExplainCmd*>(definition());
    }

    const OpMsgRequest* _outerRequest;
    const DatabaseName _dbName;
    NamespaceString _ns;
    ExplainOptions::Verbosity _verbosity;
    std::unique_ptr<OpMsgRequest> _innerRequest;  // Lifespan must enclose that of _innerInvocation.
    std::unique_ptr<CommandInvocation> _innerInvocation;
};

std::unique_ptr<CommandInvocation> CryptdExplainCmd::parse(OperationContext* opCtx,
                                                           const OpMsgRequest& request) {
    CommandHelpers::uassertNoDocumentSequences(getName(), request);

    const BSONObj& cmdObj = request.body;

    auto cleanedCmdObj = cmdObj.removeFields(StringDataSet{query_analysis::kJsonSchema,
                                                           query_analysis::kIsRemoteSchema,
                                                           query_analysis::kEncryptionInformation});
    auto explainCmd = ExplainCommandRequest::parse(
        IDLParserContext(ExplainCommandRequest::kCommandName,
                         APIParameters::get(opCtx).getAPIStrict().value_or(false)),
        cleanedCmdObj);

    // We must remove the FLE meta-data fields before attempting to parse the explain command.
    ExplainOptions::Verbosity verbosity = explainCmd.getVerbosity();

    auto explainedObj = explainCmd.getCommandParameter();
    uassert(30050,
            "In an explain command the jsonSchema field must be top-level and not inside the "
            "command being explained.",
            !explainedObj.hasField(query_analysis::kJsonSchema));
    if (auto cmdSchema = cmdObj[query_analysis::kJsonSchema]) {
        explainedObj = explainedObj.addField(cmdSchema);
    }

    uassert(6365900,
            "In an explain command the encryptionInformation field must be top-level and not "
            "inside the command being explained.",
            !explainedObj.hasField(query_analysis::kEncryptionInformation));
    if (auto cmdEncryptInfo = cmdObj[query_analysis::kEncryptionInformation]) {
        explainedObj = explainedObj.addField(cmdEncryptInfo);
    }

    uassert(31103,
            "In an explain command the isRemoteSchema field must be top-level and not inside the "
            "command being explained.",
            !explainedObj.hasField(query_analysis::kIsRemoteSchema));
    if (auto isRemoteSchema = cmdObj[query_analysis::kIsRemoteSchema]) {
        explainedObj = explainedObj.addField(isRemoteSchema);
    }

    std::string dbname = explainCmd.getDbName().toString();
    if (auto innerDb = explainedObj["$db"]) {
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << "Mismatched $db in explain command. Expected " << dbname
                              << " but got " << innerDb.checkAndGetStringData(),
                innerDb.checkAndGetStringData() == dbname);
    }

    auto explainedCommand = CommandHelpers::findCommand(explainedObj.firstElementFieldName());
    uassert(ErrorCodes::CommandNotFound,
            str::stream() << "Explain failed due to unknown command: "
                          << explainedObj.firstElementFieldName(),
            explainedCommand);

    auto innerRequest =
        std::make_unique<OpMsgRequest>(OpMsgRequest::fromDBAndBody(dbname, explainedObj));

    auto innerInvocation = explainedCommand->parse(opCtx, *innerRequest);

    return std::make_unique<Invocation>(
        this, request, std::move(verbosity), std::move(innerRequest), std::move(innerInvocation));
}

}  // namespace
}  // namespace mongo
