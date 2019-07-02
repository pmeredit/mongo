/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "fle/query_analysis/query_analysis.h"
#include "mongo/base/data_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bson_depth.h"
#include "mongo/client/dbclient_base.h"
#include "mongo/crypto/aead_encryption.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/namespace_string.h"
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
#include "mongo/shell/encrypted_shell_options.h"
#include "mongo/shell/kms.h"
#include "mongo/shell/kms_gen.h"
#include "mongo/shell/shell_options.h"
#include "mongo/util/lru_cache.h"

namespace mongo {

namespace {
constexpr Duration kCacheInvalidationTime = Minutes(1);

constexpr std::array<StringData, 9> kEncryptedCommands = {"aggregate"_sd,
                                                          "count"_sd,
                                                          "delete"_sd,
                                                          "find"_sd,
                                                          "findandmodify"_sd,
                                                          "findAndModify"_sd,
                                                          "getMore"_sd,
                                                          "insert"_sd,
                                                          "update"_sd};

class ImplicitEncryptedDBClientBase final : public EncryptedDBClientBase {
    // This struct is used for the LRU Schema cache.
    struct SchemaInfo {
        BSONObj schema;
        Date_t ts;    // Used to mark when the schema was stored in this struct.
        bool remote;  // True if the schema is from the server. Else false.
    };

public:
    ImplicitEncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                                  ClientSideFLEOptions encryptionOptions,
                                  JS::HandleValue collection,
                                  JSContext* cx)
        : EncryptedDBClientBase(std::move(conn), encryptionOptions, collection, cx) {}

    using DBClientBase::runCommandWithTarget;
    std::pair<rpc::UniqueReply, DBClientBase*> runCommandWithTarget(OpMsgRequest request) final {
        if (_encryptionOptions.getBypassAutoEncryption().value_or(false)) {
            return _conn->runCommandWithTarget(std::move(request));
        }
        return handleEncryptionRequest(std::move(request));
    }


    SchemaInfo getRemoteOrInputSchema(const OpMsgRequest& request, NamespaceString ns) {
        // Check for a client provided schema first
        if (_encryptionOptions.getSchemaMap()) {
            BSONElement schemaElem =
                _encryptionOptions.getSchemaMap().get().getField(ns.toString());
            if (!schemaElem.eoo()) {
                uassert(ErrorCodes::BadValue,
                        "Invalid Schema object in Client Side FLE Options",
                        schemaElem.isABSONObj());
                return SchemaInfo{schemaElem.Obj().getOwned(), Date_t::now(), false};
            }
        }

        // Since there is no local schema, try remote
        BSONObj filter = BSON("name" << ns.coll());
        auto collectionInfos = _conn->getCollectionInfos(ns.db().toString(), filter);

        invariant(collectionInfos.size() <= 1);
        if (collectionInfos.size() == 1) {
            BSONObj highLevelSchema = collectionInfos.front();

            BSONObj options = highLevelSchema.getObjectField("options");
            if (!options.isEmpty() && !options.getObjectField("validator").isEmpty() &&
                !options.getObjectField("validator").getObjectField("$jsonSchema").isEmpty()) {
                BSONObj validator = options.getObjectField("validator");
                BSONObj schema = validator.getObjectField("$jsonSchema");
                return SchemaInfo{schema.getOwned(), Date_t::now(), true};
            }
        }

        return SchemaInfo{BSONObj(), Date_t::now(), true};
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
        _schemaCache.add(ns, schemaInfo);

        return schemaInfo;
    }

    BSONObj runQueryAnalysis(OpMsgRequest request,
                             const SchemaInfo& schemaInfo,
                             const NamespaceString& ns,
                             const StringData& commandName) {
        BSONObjBuilder schemaInfoBuilder;

        BSONObjBuilder commandBuilder;
        commandBuilder.append(cryptd_query_analysis::kJsonSchema, schemaInfo.schema);
        commandBuilder.append(cryptd_query_analysis::kIsRemoteSchema, schemaInfo.remote);
        commandBuilder.appendElementsUnique(request.body);
        BSONObj cmdObj = commandBuilder.obj();
        request.body = cmdObj;
        auto client = &cc();
        auto uniqueOpContext = client->makeOperationContext();
        auto opCtx = uniqueOpContext.get();

        if (commandName == "find"_sd) {
            cryptd_query_analysis::processFindCommand(
                opCtx, ns.db().toString(), cmdObj, &schemaInfoBuilder);
        } else if (commandName == "aggregate"_sd) {
            cryptd_query_analysis::processAggregateCommand(
                opCtx, ns.db().toString(), cmdObj, &schemaInfoBuilder);
        } else if (commandName == "findandmodify"_sd || commandName == "findAndModify"_sd) {
            cryptd_query_analysis::processFindAndModifyCommand(
                opCtx, ns.db().toString(), cmdObj, &schemaInfoBuilder);
        } else if (commandName == "count"_sd) {
            cryptd_query_analysis::processCountCommand(
                opCtx, ns.db().toString(), cmdObj, &schemaInfoBuilder);
        } else if (commandName == "update"_sd) {
            cryptd_query_analysis::processUpdateCommand(opCtx, request, &schemaInfoBuilder);
        } else if (commandName == "insert"_sd) {
            cryptd_query_analysis::processInsertCommand(opCtx, request, &schemaInfoBuilder);
        } else if (commandName == "delete"_sd) {
            cryptd_query_analysis::processDeleteCommand(opCtx, request, &schemaInfoBuilder);
        }

        return schemaInfoBuilder.obj();
    }

    std::pair<rpc::UniqueReply, DBClientBase*> handleEncryptionRequest(OpMsgRequest request) {
        std::string commandName = request.getCommandName().toString();
        if (std::find(kEncryptedCommands.begin(),
                      kEncryptedCommands.end(),
                      StringData(commandName)) == std::end(kEncryptedCommands)) {
            return _conn->runCommandWithTarget(std::move(request));
        }

        auto databaseName = request.getDatabase().toString();

        // getMore has nothing to encrypt in the request but the response may have to be decrypted.
        if (commandName == "getMore"_sd) {
            auto result = _conn->runCommandWithTarget(request).first;
            return processResponse(std::move(result), databaseName);
        }

        NamespaceString ns = CommandHelpers::parseNsCollectionRequired(databaseName, request.body);
        auto schemaInfoObject = getSchema(request, ns);

        if (schemaInfoObject.schema.isEmpty()) {
            return _conn->runCommandWithTarget(std::move(request));
        }

        BSONObj schemaInfo =
            runQueryAnalysis(std::move(request), schemaInfoObject, ns, commandName);

        if (!schemaInfo.getBoolField("hasEncryptionPlaceholders") &&
            !schemaInfo.getBoolField("schemaRequiresEncryption")) {
            BSONElement field = schemaInfo.getField("result"_sd);
            uassert(31115,
                    "Query preprocessing of command yielded error. Result object not found.",
                    field.isABSONObj());
            request.body = field.Obj();
            return _conn->runCommandWithTarget(request);
        }

        BSONObj finalRequestObj = preprocessRequest(schemaInfo, databaseName);

        OpMsgRequest finalReq(OpMsg{std::move(finalRequestObj), {}});
        auto result = _conn->runCommandWithTarget(finalReq).first;

        return processResponse(std::move(result), databaseName);
    }

    BSONObj preprocessRequest(const BSONObj& schemaInfo, const StringData& databaseName) {
        BSONElement field = schemaInfo.getField("result"_sd);
        uassert(31060,
                "Query preprocessing of command yielded error. Result object not found.",
                field.isABSONObj());

        return buildCommand(field.Obj(), true, databaseName);
    }

    std::pair<rpc::UniqueReply, DBClientBase*> processResponse(rpc::UniqueReply result,
                                                               const StringData& databaseName) {

        auto rawReply = result->getCommandReply();
        BSONObj decryptedDoc = buildCommand(rawReply, false, databaseName);

        rpc::OpMsgReplyBuilder replyBuilder;
        replyBuilder.setCommandReply(StatusWith<BSONObj>(decryptedDoc));
        auto msg = replyBuilder.done();

        auto host = _conn->getServerAddress();
        auto reply = _conn->parseCommandReplyMessage(host, msg);

        return {std::move(reply), this};
    }

    BSONObj buildCommand(const BSONObj& object, bool encrypt, const StringData& databaseName) {
        std::stack<std::pair<BSONObjIterator, BSONObjBuilder>> frameStack;

        // The buildCommand frameStack requires a guard because  if encryptMarking or decrypt
        // payload throw an exception, the stack's destructor will fire. Because a stack's
        // variables are not guaranteed to be destroyed in any order, we need to add a guard
        // to ensure the stack is destroyed in order.
        const auto frameStackGuard = makeGuard([&] {
            while (!frameStack.empty()) {
                frameStack.pop();
            }
        });

        frameStack.emplace(BSONObjIterator(object), BSONObjBuilder());

        while (frameStack.size() > 1 || frameStack.top().first.more()) {
            uassert(31096,
                    "Object too deep to be encrypted. Exceeded stack depth.",
                    frameStack.size() < BSONDepth::kDefaultMaxAllowableDepth);
            auto & [ iterator, builder ] = frameStack.top();
            if (iterator.more()) {
                BSONElement elem = iterator.next();
                if (elem.type() == BSONType::Object) {
                    frameStack.emplace(
                        BSONObjIterator(elem.Obj()),
                        BSONObjBuilder(builder.subobjStart(elem.fieldNameStringData())));
                } else if (elem.type() == BSONType::Array) {
                    frameStack.emplace(
                        BSONObjIterator(elem.Obj()),
                        BSONObjBuilder(builder.subarrayStart(elem.fieldNameStringData())));
                } else if (elem.isBinData(BinDataType::Encrypt)) {
                    int len;
                    const char* data(elem.binData(len));
                    uassert(31101, "Invalid intentToEncrypt object from Query Analyzer", len >= 1);
                    if ((*data == kRandomEncryptionBit || *data == kDeterministicEncryptionBit) &&
                        !encrypt) {
                        ConstDataRange dataCursor(data, len);
                        decryptPayload(dataCursor, &builder, elem.fieldNameStringData());
                    } else if (*data == kIntentToEncryptBit && encrypt) {
                        BSONObj obj = BSONObj(data + 1);
                        encryptMarking(obj, &builder, elem.fieldNameStringData());
                    } else {
                        builder.append(elem);
                    }
                } else {
                    builder.append(elem);
                }
            } else {
                frameStack.pop();
            }
        }
        invariant(frameStack.size() == 1);
        frameStack.top().second.append("$db", databaseName);
        return frameStack.top().second.obj();
    }

    void encryptMarking(const BSONObj& obj, BSONObjBuilder* builder, StringData elemName) {
        EncryptionPlaceholder toEncrypt =
            EncryptionPlaceholder::parse(IDLParserErrorContext("root"), obj);
        if ((toEncrypt.getKeyId() && toEncrypt.getKeyAltName()) ||
            !(toEncrypt.getKeyId() || toEncrypt.getKeyAltName())) {
            uasserted(ErrorCodes::BadValue,
                      "exactly one of either keyId or keyAltName must be specified.");
        }

        EncryptSchemaAnyType value = toEncrypt.getValue();
        BSONElement valueElem = value.getElement();

        BSONType bsonType = valueElem.type();
        ConstDataRange plaintext(valueElem.value(), valueElem.valuesize());
        std::vector<uint8_t> encData;

        if (toEncrypt.getKeyId()) {
            UUID uuid = toEncrypt.getKeyId().get();
            auto key = getDataKey(uuid);
            encData = encryptWithKey(uuid,
                                     key,
                                     plaintext,
                                     bsonType,
                                     FleAlgorithmInt_serializer(toEncrypt.getAlgorithm()));
        } else {
            auto keyAltName = toEncrypt.getKeyAltName().get();
            UUID uuid = getUUIDByDataKeyAltName(keyAltName);
            auto key = getDataKey(uuid);
            encData = encryptWithKey(uuid,
                                     key,
                                     plaintext,
                                     bsonType,
                                     FleAlgorithmInt_serializer(toEncrypt.getAlgorithm()));
        };

        builder->appendBinData(elemName, encData.size(), BinDataType::Encrypt, encData.data());
    }

    void decryptPayload(ConstDataRange data, BSONObjBuilder* builder, StringData elemName) {
        invariant(builder);
        uassert(
            ErrorCodes::BadValue, "Invalid decryption blob", data.length() > kAssociatedDataLength);
        ConstDataRange uuidCdr = ConstDataRange(data.data() + 1, 16);
        UUID uuid = UUID::fromCDR(uuidCdr);

        auto key = getDataKey(uuid);
        std::vector<uint8_t> out(data.length() - kAssociatedDataLength);
        size_t outLen = out.size();

        uassertStatusOK(crypto::aeadDecrypt(
            *key,
            reinterpret_cast<const uint8_t*>(data.data() + kAssociatedDataLength),
            data.length() - kAssociatedDataLength,
            reinterpret_cast<const uint8_t*>(data.data()),
            kAssociatedDataLength,
            out.data(),
            &outLen));

        // extract type byte
        const uint8_t bsonType = static_cast<const uint8_t>(*(data.data() + 17));
        BSONObj decryptedObj = validateBSONElement(ConstDataRange(out.data(), outLen), bsonType);
        if (bsonType == BSONType::Object) {
            builder->append(elemName, decryptedObj);
        } else {
            builder->appendAs(decryptedObj.firstElement(), elemName);
        }
    }


private:
    UUID getUUIDByDataKeyAltName(StringData altName) {
        NamespaceString fullNameNS = getCollectionNS();
        BSONObjBuilder builder;
        builder.append("keyAltNames"_sd, altName);
        BSONObj altNameObj(builder.obj());
        BSONObj dataKeyObj = _conn->findOne(fullNameNS.ns(), Query(altNameObj));
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
    return Status::OK();
}

}  // namespace
}  // namespace mongo
