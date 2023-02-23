/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "fle/query_analysis/query_analysis.h"
#include "mongo/base/data_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bson_depth.h"
#include "mongo/client/dbclient_base.h"
#include "mongo/crypto/aead_encryption.h"
#include "mongo/crypto/fle_crypto.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/db/basic_types_gen.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/rpc/object_check.h"
#include "mongo/rpc/op_msg_rpc_impls.h"
#include "mongo/scripting/mozjs/bindata.h"
#include "mongo/scripting/mozjs/implscope.h"
#include "mongo/scripting/mozjs/maxkey.h"
#include "mongo/scripting/mozjs/minkey.h"
#include "mongo/scripting/mozjs/mongo.h"
#include "mongo/scripting/mozjs/objectwrapper.h"
#include "mongo/scripting/mozjs/valuereader.h"
#include "mongo/scripting/mozjs/valuewriter.h"
#include "mongo/shell/encrypted_dbclient_base.h"
#include "mongo/shell/kms.h"
#include "mongo/shell/kms_gen.h"
#include "mongo/shell/shell_options.h"
#include "mongo/util/lru_cache.h"

namespace mongo {

namespace {
constexpr std::size_t kEncryptedDBCacheSize = 50;
constexpr Duration kCacheInvalidationTime = Minutes(1);

class ImplicitEncryptedDBClientBase final : public EncryptedDBClientBase {
    // This struct is used for the LRU Schema cache.
    struct SchemaInfo {
        BSONObj schema;
        Date_t ts;    // Used to mark when the schema was stored in this struct.
        bool remote;  // True if the schema is from the server. Else false.
        enum class SchemaType { none, jsonSchema, encryptedFields } _schemaType;

        bool isFLE2() const {
            return _schemaType == SchemaType::encryptedFields;
        }
    };

public:
    ImplicitEncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                                  ClientSideFLEOptions encryptionOptions,
                                  JS::HandleValue collection,
                                  JSContext* cx)
        : EncryptedDBClientBase(std::move(conn), encryptionOptions, collection, cx) {}

    SchemaInfo getRemoteOrInputSchema(const OpMsgRequest& request, NamespaceString ns) {
        // Check for a client provided schema first
        if (_encryptionOptions.getSchemaMap()) {
            BSONElement schemaElem =
                _encryptionOptions.getSchemaMap().value().getField(ns.toString());
            if (!schemaElem.eoo()) {
                uassert(ErrorCodes::BadValue,
                        "Invalid Schema object in Client Side FLE Options",
                        schemaElem.isABSONObj());

                BSONObj schemaObj = schemaElem.Obj();

                if (schemaObj.hasField("escCollection") && schemaObj.hasField("eccCollection") &&
                    schemaObj.hasField("ecocCollection") && schemaObj.hasField("encryptedFields")) {

                    return SchemaInfo{schemaObj.getOwned(),
                                      Date_t::now(),
                                      false,
                                      SchemaInfo::SchemaType::encryptedFields};
                }
                return SchemaInfo{
                    schemaObj.getOwned(), Date_t::now(), false, SchemaInfo::SchemaType::jsonSchema};
            }
        }

        // Since there is no local schema, try remote
        BSONObj filter = BSON("name" << ns.coll());

        auto dollarTenant = ns.dbName().tenantId();
        auto collectionInfos = _conn->getCollectionInfos(ns.dbName(), filter, dollarTenant);

        invariant(collectionInfos.size() <= 1);
        if (collectionInfos.size() == 1) {
            BSONObj highLevelSchema = collectionInfos.front();

            BSONObj options = highLevelSchema.getObjectField("options");
            if (!options.isEmpty()) {
                if (!options.getObjectField("encryptedFields").isEmpty()) {

                    return SchemaInfo{options.getObjectField("encryptedFields").getOwned(),
                                      Date_t::now(),
                                      true,
                                      SchemaInfo::SchemaType::encryptedFields};
                }
            }

            if (!options.getObjectField("validator").isEmpty() &&
                !options.getObjectField("validator").getObjectField("$jsonSchema").isEmpty()) {
                BSONObj validator = options.getObjectField("validator");
                BSONObj schema = validator.getObjectField("$jsonSchema");
                return SchemaInfo{
                    schema.getOwned(), Date_t::now(), true, SchemaInfo::SchemaType::jsonSchema};
            }
        }

        return SchemaInfo{BSONObj(), Date_t::now(), true, SchemaInfo::SchemaType::none};
    }

    SchemaInfo getSchema(const OpMsgRequest& request, NamespaceString ns) {
        if (_schemaCache.hasKey(ns)) {
            auto schemaInfo = _schemaCache.find(ns)->second;
            auto ts_new = Date_t::now();

            if ((ts_new - schemaInfo.ts) < kCacheInvalidationTime) {
                return schemaInfo;
            }

            _schemaCache.erase(ns);
        }

        auto schemaInfo = getRemoteOrInputSchema(request, ns);
        if (!schemaInfo.schema.isEmpty()) {
            _schemaCache.add(ns, schemaInfo);
        }

        return schemaInfo;
    }

    BSONObj processExplainCommand(OpMsgRequest request,
                                  const SchemaInfo& schemaInfo,
                                  const NamespaceString& ns) {
        // 1. Take an explain command:
        //    explain : {
        //      innerCommand : ns,
        //      ...
        //    }
        //    extract the "inner" command to do query analysis on that.
        // 2. Then re-wrap with "explain" so that it can sent over the wire
        // 3. Finally output the BSON as if query analysis returned it
        //
        auto explainedObj = request.body.firstElement().Obj();
        auto explainedCommand = explainedObj.firstElementFieldName();

        uassert(51242, "Cannot explain the explain command", explainedCommand != kExplain);
        uassert(51243, "Explained command cannot have $db", explainedObj["$db"_sd].eoo());

        OpMsgRequest requestInner;
        {
            BSONObjBuilder builder;
            builder.appendElements(explainedObj);
            builder.append(request.body["$db"_sd]);
            requestInner.body = builder.obj();
        }

        auto obj = runQueryAnalysis(requestInner, schemaInfo, ns, explainedCommand);

        auto placeholder = query_analysis::parsePlaceholderResult(obj);

        {
            BSONObjBuilder explainBuilder;
            explainBuilder.append(kExplain, placeholder.result);
            placeholder.result = explainBuilder.obj();
        }

        BSONObjBuilder builder;
        query_analysis::serializePlaceholderResult(placeholder, &builder);
        return builder.obj();
    }

    BSONObj runQueryAnalysisInt(OpMsgRequest request,
                                const SchemaInfo& schemaInfo,
                                const NamespaceString& ns,
                                const StringData& commandName) {
        if (commandName == kExplain) {
            return processExplainCommand(request, schemaInfo, ns);
        }

        BSONObjBuilder commandBuilder;
        commandBuilder.appendElementsUnique(request.body);

        if (schemaInfo.isFLE2()) {
            BSONObj ei;
            // commands that may delete fields require delete tokens
            if (commandName == "delete" || commandName == "findAndModify" ||
                commandName == "findandmodify" || commandName == "update") {

                EncryptedFieldConfig efc = EncryptedFieldConfig::parse(
                    IDLParserContext("encryptedFields"), schemaInfo.schema);
                ei = EncryptionInformationHelpers::encryptionInformationSerializeForDelete(
                    ns, efc, this);

            } else {

                ei = EncryptionInformationHelpers::encryptionInformationSerialize(
                    ns, schemaInfo.schema);
            }

            commandBuilder.append(query_analysis::kEncryptionInformation, ei);
        } else {
            commandBuilder.append(query_analysis::kJsonSchema, schemaInfo.schema);
            commandBuilder.append(query_analysis::kIsRemoteSchema, schemaInfo.remote);
        }

        BSONObj cmdObj = commandBuilder.obj();
        request.body = cmdObj;
        auto client = &cc();
        auto uniqueOpContext = client->makeOperationContext();
        auto opCtx = uniqueOpContext.get();

        BSONObjBuilder schemaInfoBuilder;
        if (commandName == "find"_sd) {
            query_analysis::processFindCommand(opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "aggregate"_sd) {
            query_analysis::processAggregateCommand(
                opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "findandmodify"_sd || commandName == "findAndModify"_sd) {
            query_analysis::processFindAndModifyCommand(
                opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "count"_sd) {
            query_analysis::processCountCommand(opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "distinct"_sd) {
            query_analysis::processDistinctCommand(
                opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "create"_sd) {
            query_analysis::processCreateCommand(
                opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "collMod"_sd) {
            query_analysis::processCollModCommand(
                opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "createIndexes"_sd) {
            query_analysis::processCreateIndexesCommand(
                opCtx, ns.dbName(), cmdObj, &schemaInfoBuilder, ns);
        } else if (commandName == "update"_sd) {
            query_analysis::processUpdateCommand(opCtx, request, &schemaInfoBuilder, ns);
        } else if (commandName == "insert"_sd) {
            query_analysis::processInsertCommand(opCtx, request, &schemaInfoBuilder, ns);
        } else if (commandName == "delete"_sd) {
            query_analysis::processDeleteCommand(opCtx, request, &schemaInfoBuilder, ns);
        }

        return schemaInfoBuilder.obj();
    }

    BSONObj runQueryAnalysis(OpMsgRequest request,
                             const SchemaInfo& schemaInfo,
                             const NamespaceString& ns,
                             const StringData& commandName) {
        try {
            return runQueryAnalysisInt(request, schemaInfo, ns, commandName);
        } catch (const DBException& e) {
            // Wrap exceptions from query analysis with prefix to make it clear to users that it is
            // coming from the shell
            uassertStatusOK(
                Status(e.code(),
                       str::stream() << "Client Side Field Level Encryption Error:" << e.reason()));
        }

        MONGO_UNREACHABLE;
    }

    using EncryptedDBClientBase::handleEncryptionRequest;
    RunCommandReturn handleEncryptionRequest(RunCommandParams params) final {
        auto& request = params.request;
        DatabaseName dbName;
        if (request.body.hasField("$tenant")) {
            dbName = DatabaseName(TenantId(request.body["$tenant"].OID()), request.getDatabase());
        } else {
            dbName = DatabaseName(request.getValidatedTenantId(), request.getDatabase());
        }
        auto databaseName = dbName.toStringWithTenantId();

        // Check for bypassing auto encryption. If so, always process response.
        if (_encryptionOptions.getBypassAutoEncryption().value_or(false)) {
            auto result = doRunCommand(std::move(params));
            return processResponseFLE1(processResponseFLE2(std::move(result)), databaseName);
        }

        // Check if request is an encrypted command.
        std::string commandName = request.getCommandName().toString();
        if (std::find(kEncryptedCommands.begin(),
                      kEncryptedCommands.end(),
                      StringData(commandName)) == std::end(kEncryptedCommands)) {
            return doRunCommand(std::move(params));
        }

        // getMore has nothing to encrypt in the request but the response may have to be decrypted.
        if (commandName == "getMore"_sd) {
            auto result = doRunCommand(std::move(params));
            return processResponseFLE1(processResponseFLE2(std::move(result)), databaseName);
        }

        // Get namespace for command.
        NamespaceString ns;
        if (commandName == kExplain) {
            uassert(ErrorCodes::BadValue,
                    "explain command requires a nested object",
                    request.body.firstElement().type() == Object);

            ns = CommandHelpers::parseNsCollectionRequired(dbName,
                                                           request.body.firstElement().Obj());
        } else {
            ns = CommandHelpers::parseNsCollectionRequired(dbName, request.body);
        }

        // Attempt to get schema.
        auto schemaInfoObject = [&]() {
            if (commandName == "create"_sd) {
                if (request.body.hasField("encryptedFields")) {
                    return SchemaInfo{request.body.getObjectField("encryptedFields").getOwned(),
                                      Date_t::now(),
                                      true,
                                      SchemaInfo::SchemaType::encryptedFields};
                } else {
                    return SchemaInfo{BSONObj(), Date_t::now(), true, SchemaInfo::SchemaType::none};
                }
            } else {
                return getSchema(request, ns);
            }
        }();


        // collMod commands can modify JSONSchema validators, and so we should invalidate the schema
        // cache entry after a collMod command.
        if (commandName == "collMod"_sd) {
            _schemaCache.erase(ns);
        }

        // Check the schema.
        if (schemaInfoObject.schema.isEmpty()) {
            // Always attempt to decrypt - could have encrypted data
            auto result = doRunCommand(std::move(params));
            if (schemaInfoObject.isFLE2()) {
                return processResponseFLE2(std::move(result));
            }
            return processResponseFLE1(std::move(result), databaseName);
        }

        BSONObj schemaInfo = runQueryAnalysis(params.request, schemaInfoObject, ns, commandName);

        // Check schemaInfo object.
        if (!schemaInfo.getBoolField("hasEncryptionPlaceholders") &&
            !schemaInfo.getBoolField("schemaRequiresEncryption")) {
            BSONElement field = schemaInfo.getField("result"_sd);
            uassert(31115,
                    "Query preprocessing of command yielded error. Result object not found.",
                    field.isABSONObj());
            params.request.body = field.Obj();
            return doRunCommand(params);
        }

        // The passed dbName.toString is to set '$db' which shouldn't contain tenantid. Only use
        // .toString here.
        BSONObj finalRequestObj = preprocessRequest(schemaInfo, dbName.toString());

        OpMsgRequest finalReq(OpMsg{std::move(finalRequestObj), {}});
        if (ns.tenantId()) {
            finalReq.setDollarTenant(ns.tenantId().get());
        }
        RunCommandParams newParam(std::move(finalReq), params);

        auto result = doRunCommand(newParam);

        if (schemaInfoObject.isFLE2()) {
            return processResponseFLE2(std::move(result));
        }
        return processResponseFLE1(std::move(result), databaseName);
    }

    BSONObj preprocessRequest(const BSONObj& schemaInfo, const StringData databaseName) {
        BSONElement field = schemaInfo.getField("result"_sd);
        uassert(31060,
                "Query preprocessing of command yielded error. Result object not found.",
                field.isABSONObj());

        auto obj = encryptDecryptCommand(field.Obj(), true, databaseName);

        return FLEClientCrypto::transformPlaceholders(obj, this);
    }

    void encryptMarking(const BSONObj& obj, BSONObjBuilder* builder, StringData elemName) override {
        EncryptionPlaceholder toEncrypt =
            EncryptionPlaceholder::parse(IDLParserContext("root"), obj);
        if ((toEncrypt.getKeyId() && toEncrypt.getKeyAltName()) ||
            !(toEncrypt.getKeyId() || toEncrypt.getKeyAltName())) {
            uasserted(ErrorCodes::BadValue,
                      "exactly one of either keyId or keyAltName must be specified.");
        }

        IDLAnyType value = toEncrypt.getValue();

        BSONElement valueElem = value.getElement();
        BSONType bsonType = valueElem.type();
        ConstDataRange plaintext(valueElem.value(), valueElem.valuesize());

        FLEEncryptionFrame dataFrame;

        if (toEncrypt.getKeyId()) {
            UUID uuid = toEncrypt.getKeyId().value();
            dataFrame = createEncryptionFrame(
                getDataKey(uuid), toEncrypt.getAlgorithm(), uuid, bsonType, plaintext);
        } else {
            auto keyAltName = toEncrypt.getKeyAltName().value();
            UUID uuid = getUUIDByDataKeyAltName(keyAltName);
            dataFrame = createEncryptionFrame(
                getDataKey(uuid), toEncrypt.getAlgorithm(), uuid, bsonType, plaintext);
        };

        ConstDataRange ciphertextBlob(dataFrame.get());
        builder->appendBinData(elemName,
                               ciphertextBlob.length(),
                               BinDataType::Encrypt,
                               ciphertextBlob.data<uint8_t>());
    }

private:
    UUID getUUIDByDataKeyAltName(StringData altName) {
        NamespaceString fullNameNS = getCollectionNS();
        BSONObjBuilder builder;
        builder.append("keyAltNames"_sd, altName);
        BSONObj altNameObj(builder.obj());
        FindCommandRequest findCmd{fullNameNS};
        findCmd.setFilter(altNameObj);
        findCmd.setReadConcern(
            repl::ReadConcernArgs(repl::ReadConcernLevel::kMajorityReadConcern).toBSONInner());
        BSONObj dataKeyObj = _conn->findOne(std::move(findCmd));
        if (dataKeyObj.isEmpty()) {
            uasserted(ErrorCodes::BadValue, "Invalid keyAltName.");
        }
        BSONElement uuidElem;
        dataKeyObj.getObjectID(uuidElem);
        return uassertStatusOK(UUID::parse(uuidElem));
    }

private:
    LRUCache<NamespaceString, SchemaInfo> _schemaCache{kEncryptedDBCacheSize};
};

// The parameters required to start FLE on the shell. The current connection is passed in as a
// parameter to create the keyvault collection object if one is not provided.
std::unique_ptr<DBClientBase> createImplicitEncryptedDBClientBase(
    std::unique_ptr<DBClientBase> conn,
    ClientSideFLEOptions encryptionOptions,
    JS::HandleValue collection,
    JSContext* cx) {

    std::unique_ptr<ImplicitEncryptedDBClientBase> base =
        std::make_unique<ImplicitEncryptedDBClientBase>(
            std::move(conn), encryptionOptions, collection, cx);
    return std::move(base);
}

MONGO_INITIALIZER(setCallbacksForImplicitEncryptedDBClientBase)(InitializerContext*) {
    setImplicitEncryptedDBClientCallback(createImplicitEncryptedDBClientBase);
}

}  // namespace
}  // namespace mongo
