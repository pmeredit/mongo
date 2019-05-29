/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "encryptdb/symmetric_crypto.h"
#include "fle/encryption/aead_encryption.h"
#include "fle/query_analysis/query_analysis.h"
#include "fle/shell/encrypted_shell_options.h"
#include "fle/shell/kms.h"
#include "fle/shell/kms_gen.h"
#include "mongo/base/data_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bson_depth.h"
#include "mongo/client/dbclient_base.h"
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
#include "mongo/shell/shell_options.h"
#include "mongo/util/lru_cache.h"

namespace mongo {

EncryptedShellGlobalParams encryptedShellGlobalParams;

namespace {
constexpr int kAssociatedDataLength = 18;
constexpr std::size_t kEncryptedDBCacheSize = 50;
constexpr Duration kCacheInvalidationTime = Minutes(1);
constexpr std::array<StringData, 8> kEncryptedCommands = {"aggregate"_sd,
                                                          "count"_sd,
                                                          "delete"_sd,
                                                          "find"_sd,
                                                          "findandmodify"_sd,
                                                          "findAndModify"_sd,
                                                          "insert"_sd,
                                                          "update"_sd};
constexpr uint8_t kIntentToEncryptBit = 0x00;
constexpr uint8_t kDeterministicEncryptionBit = 0x01;
constexpr uint8_t kRandomEncryptionBit = 0x02;

class EncryptedDBClientBase final : public DBClientBase, public mozjs::EncryptionCallbacks {

private:
    // This struct is used for the LRU Schema cache.
    struct SchemaInfo {
        BSONObj schema;
        Date_t ts;    // Used to mark when the schema was stored in this struct.
        bool remote;  // True if the schema is from the server. Else false.
    };

public:
    EncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                          ClientSideFLEOptions encryptionOptions,
                          JS::HandleValue collection,
                          JSContext* cx)
        : _conn(std::move(conn)), _encryptionOptions(std::move(encryptionOptions)), _cx(cx) {
        validateCollection(cx, collection);
        _collection = JS::Heap<JS::Value>(collection);
        uassert(31078,
                "Cannot use WriteMode Legacy with Field Level Encryption",
                shellGlobalParams.writeMode != "legacy");
    };

    std::string getServerAddress() const final {
        return _conn->getServerAddress();
    }

    bool call(Message& toSend, Message& response, bool assertOk, std::string* actualServer) final {
        return _conn->call(toSend, response, assertOk, actualServer);
    }

    void say(Message& toSend, bool isRetry, std::string* actualServer) final {
        MONGO_UNREACHABLE;
    }

    bool lazySupported() const final {
        return _conn->lazySupported();
    }

    using DBClientBase::runCommandWithTarget;
    std::pair<rpc::UniqueReply, DBClientBase*> runCommandWithTarget(OpMsgRequest request) final {
        if (_encryptionOptions.getBypassAutoEncryption().value_or(false)) {
            return _conn->runCommandWithTarget(std::move(request));
        }
        return handleEncryptionRequest(std::move(request));
    }

    SchemaInfo getRemoteOrInputSchema(const OpMsgRequest& request, NamespaceString ns) {
        BSONElement schemaElem = _encryptionOptions.getSchemaMap().getField(ns.toString());
        if (!schemaElem.eoo()) {
            uassert(ErrorCodes::BadValue,
                    "Invalid Schema object in Client Side FLE Options",
                    schemaElem.isABSONObj());
            return SchemaInfo{schemaElem.Obj().getOwned(), Date_t::now(), false};
        } else {
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

    /**
     *
     * This function reads the data from the CDR and returns a copy
     * constructed and owned BSONObject.
     *
     */
    BSONObj validateBSONElement(ConstDataRange out, uint8_t bsonType) {
        if (bsonType == BSONType::Object) {
            ConstDataRangeCursor cdc = ConstDataRangeCursor(out);
            BSONObj valueObj;

            valueObj = cdc.readAndAdvance<Validated<BSONObj>>();
            return valueObj.getOwned();
        } else {
            auto valueString = "value"_sd;

            // The size here is to construct a new BSON document and validate the
            // total size of the object. The first four bytes is for the size of an
            // int32_t, then a space for the type of the first element, then the space
            // for the value string and the the 0x00 terminated field name, then the
            // size of the actual data, then the last byte for the end document character,
            // also 0x00.
            size_t docLength = sizeof(int32_t) + 1 + valueString.size() + 1 + out.length() + 1;
            BufBuilder builder;
            builder.reserveBytes(docLength);

            uassert(ErrorCodes::BadValue,
                    "invalid decryption value",
                    docLength < std::numeric_limits<int32_t>::max());

            builder.appendNum(static_cast<uint32_t>(docLength));
            builder.appendChar(static_cast<uint8_t>(bsonType));
            builder.appendStr(valueString, true);
            builder.appendBuf(out.data(), out.length());
            builder.appendChar('\0');

            ConstDataRangeCursor cdc =
                ConstDataRangeCursor(ConstDataRange(builder.buf(), builder.len()));
            BSONObj elemWrapped = cdc.readAndAdvance<Validated<BSONObj>>();
            return elemWrapped.getOwned();
        }
    }

    std::string toString() const final {
        return _conn->toString();
    }

    int getMinWireVersion() final {
        return _conn->getMinWireVersion();
    }

    int getMaxWireVersion() final {
        return _conn->getMaxWireVersion();
    }

    using EncryptionCallbacks::generateDataKey;
    void generateDataKey(JSContext* cx, JS::CallArgs args) final {
        if (args.length() != 2) {
            uasserted(ErrorCodes::BadValue, "generateDataKey requires 2 arg");
        }

        if (!args.get(0).isString()) {
            uasserted(ErrorCodes::BadValue, "1st param to generateDataKey has to be a string");
        }

        if (!args.get(1).isString()) {
            uasserted(ErrorCodes::BadValue, "2nd param to generateDataKey has to be a string");
        }

        std::string kmsProvider = mozjs::ValueWriter(cx, args.get(0)).toString();
        std::string clientMasterKey = mozjs::ValueWriter(cx, args.get(1)).toString();

        std::unique_ptr<KMSService> kmsService = KMSServiceController::createFromClient(
            kmsProvider, _encryptionOptions.getKmsProviders().toBSON());

        SecureVector<uint8_t> dataKey(crypto::kFieldLevelEncryptionKeySize);
        auto res = crypto::engineRandBytes(dataKey->data(), dataKey->size());
        uassert(31042, "Error generating data key: " + res.codeString(), res.isOK());

        BSONObj obj = kmsService->encryptDataKey(ConstDataRange(dataKey->data(), dataKey->size()),
                                                 clientMasterKey);

        mozjs::ValueReader(cx, args.rval()).fromBSON(obj, nullptr, false);
    }

    using EncryptionCallbacks::getDataKeyCollection;
    void getDataKeyCollection(JSContext* cx, JS::CallArgs args) final {
        if (args.length() != 0) {
            uasserted(ErrorCodes::BadValue, "getDataKeyCollection does not take any params");
        }
        args.rval().set(_collection.get());
    }

    using EncryptionCallbacks::encrypt;
    void encrypt(mozjs::MozJSImplScope* scope, JSContext* cx, JS::CallArgs args) final {
        // Input Validation
        uassert(ErrorCodes::BadValue, "encrypt requires 3 args", args.length() == 3);

        if (!(args.get(1).isObject() || args.get(1).isString() || args.get(1).isNumber() ||
              args.get(1).isBoolean())) {
            uasserted(ErrorCodes::BadValue,
                      "Second parameter must be an object, string, number, or bool");
        }

        uassert(ErrorCodes::BadValue, "Third parameter must be a string", args.get(2).isString());
        auto algorithmStr = mozjs::ValueWriter(cx, args.get(2)).toString();
        FleAlgorithmInt algorithm;

        if (StringData(algorithmStr) == FleAlgorithm_serializer(FleAlgorithmEnum::kRandom)) {
            algorithm = FleAlgorithmInt::kRandom;
        } else if (StringData(algorithmStr) ==
                   FleAlgorithm_serializer(FleAlgorithmEnum::kDeterministic)) {
            algorithm = FleAlgorithmInt::kDeterministic;
        } else {
            uasserted(ErrorCodes::BadValue, "Third parameter must be the FLE Algorithm type");
        }

        // Extract the UUID from the callArgs
        auto binData = getBinDataArg(scope, cx, args, 0, BinDataType::newUUID);
        UUID uuid = UUID::fromCDR(ConstDataRange(binData.data(), binData.size()));
        BSONType bsonType = BSONType::EOO;

        BufBuilder plaintext;
        if (args.get(1).isObject()) {
            JS::RootedObject rootedObj(cx, &args.get(1).toObject());
            auto jsclass = JS_GetClass(rootedObj);

            if (strcmp(jsclass->name, "Object") == 0 || strcmp(jsclass->name, "Array") == 0) {
                uassert(ErrorCodes::BadValue,
                        "Cannot deterministically encrypt object or array types.",
                        algorithm != FleAlgorithmInt::kDeterministic);

                // If it is a JS Object, then we can extract all the information by simply calling
                // ValueWriter.toBSON and setting the type bit, which is what is happening below.
                BSONObj valueObj = mozjs::ValueWriter(cx, args.get(1)).toBSON();
                plaintext.appendBuf(valueObj.objdata(), valueObj.objsize());
                if (strcmp(jsclass->name, "Array") == 0) {
                    bsonType = BSONType::Array;
                } else {
                    bsonType = BSONType::Object;
                }

            } else if (scope->getProto<mozjs::MinKeyInfo>().getJSClass() == jsclass ||
                       scope->getProto<mozjs::MaxKeyInfo>().getJSClass() == jsclass ||
                       scope->getProto<mozjs::DBRefInfo>().getJSClass() == jsclass) {
                uasserted(ErrorCodes::BadValue,
                          "Second parameter cannot be MinKey, MaxKey, or DBRef");
            } else {
                if (scope->getProto<mozjs::NumberDecimalInfo>().getJSClass() == jsclass) {
                    uassert(ErrorCodes::BadValue,
                            "Cannot deterministically encrypt NumberDecimal type objects.",
                            algorithm != FleAlgorithmInt::kDeterministic);
                }

                if (scope->getProto<mozjs::CodeInfo>().getJSClass() == jsclass) {
                    uassert(ErrorCodes::BadValue,
                            "Cannot deterministically encrypt Code type objects.",
                            algorithm != FleAlgorithmInt::kDeterministic);
                }

                // If it is one of our Mongo defined types, then we have to use the ValueWriter
                // writeThis function, which takes in a set of WriteFieldRecursionFrames (setting
                // a limit on how many times we can recursively dig into an object's nested
                // structure)
                // and writes the value out to a BSONObjBuilder. We can then extract that
                // information
                // from the object by building it and pulling out the first element, which is the
                // object we are trying to get.
                mozjs::ObjectWrapper::WriteFieldRecursionFrames frames;
                frames.emplace(cx, rootedObj.get(), nullptr, StringData{});
                BSONObjBuilder builder;
                mozjs::ValueWriter(cx, args.get(1)).writeThis(&builder, "value"_sd, &frames);

                BSONObj object = builder.obj();
                auto elem = object.getField("value"_sd);

                plaintext.appendBuf(elem.value(), elem.valuesize());
                bsonType = elem.type();
            }

        } else if (args.get(1).isString()) {
            std::string valueStr = mozjs::ValueWriter(cx, args.get(1)).toString();
            if (valueStr.size() + 1 > std::numeric_limits<uint32_t>::max()) {
                uasserted(ErrorCodes::BadValue, "Plaintext string to encrypt too long.");
            }

            plaintext.appendNum(static_cast<uint32_t>(valueStr.size() + 1));
            plaintext.appendStr(valueStr, true);
            bsonType = BSONType::String;

        } else if (args.get(1).isNumber()) {
            uassert(ErrorCodes::BadValue,
                    "Cannot deterministically encrypt Floating Point numbers.",
                    algorithm != FleAlgorithmInt::kDeterministic);

            double valueNum = mozjs::ValueWriter(cx, args.get(1)).toNumber();
            plaintext.appendNum(valueNum);
            bsonType = BSONType::NumberDouble;
        } else if (args.get(1).isBoolean()) {
            uassert(ErrorCodes::BadValue,
                    "Cannot deterministically encrypt booleans.",
                    algorithm != FleAlgorithmInt::kDeterministic);

            bool boolean = mozjs::ValueWriter(cx, args.get(1)).toBoolean();
            if (boolean) {
                plaintext.appendChar(0x01);
            } else {
                plaintext.appendChar(0x00);
            }
            bsonType = BSONType::Bool;
        } else {
            uasserted(ErrorCodes::BadValue, "Cannot encrypt valuetype provided.");
        }
        ConstDataRange plaintextRange(plaintext.buf(), plaintext.len());

        auto key = getDataKey(uuid);
        std::vector<uint8_t> fleBlob = encryptWithKey(
            uuid, key, plaintextRange, bsonType, FleAlgorithmInt_serializer(algorithm));

        // Prepare the return value
        std::string blobStr =
            base64::encode(reinterpret_cast<char*>(fleBlob.data()), fleBlob.size());
        JS::AutoValueArray<2> arr(cx);

        arr[0].setInt32(BinDataType::Encrypt);
        mozjs::ValueReader(cx, arr[1]).fromStringData(blobStr);
        scope->getProto<mozjs::BinDataInfo>().newInstance(arr, args.rval());
    }

    using EncryptionCallbacks::decrypt;
    void decrypt(mozjs::MozJSImplScope* scope, JSContext* cx, JS::CallArgs args) final {
        uassert(ErrorCodes::BadValue, "decrypt requires one argument", args.length() == 1);
        uassert(ErrorCodes::BadValue,
                "decrypt argument must be a BinData subtype Encrypt object",
                args.get(0).isObject());

        if (!scope->getProto<mozjs::BinDataInfo>().instanceOf(args.get(0))) {
            uasserted(ErrorCodes::BadValue,
                      "decrypt argument must be a BinData subtype Encrypt object");
        }

        JS::RootedObject obj(cx, &args.get(0).get().toObject());
        std::vector<uint8_t> binData = getBinDataArg(scope, cx, args, 0, BinDataType::Encrypt);

        uassert(ErrorCodes::BadValue,
                "Ciphertext blob too small",
                binData.size() > kAssociatedDataLength);
        uassert(ErrorCodes::BadValue,
                "Ciphertext blob algorithm unknown",
                (FleAlgorithmInt(binData[0]) == FleAlgorithmInt::kDeterministic ||
                 FleAlgorithmInt(binData[0]) == FleAlgorithmInt::kRandom));

        ConstDataRange uuidCdr = ConstDataRange(&binData[1], UUID::kNumBytes);
        UUID uuid = UUID::fromCDR(uuidCdr);

        auto key = getDataKey(uuid);
        std::vector<uint8_t> out(binData.size() - kAssociatedDataLength);
        size_t outLen = out.size();

        auto decryptStatus = crypto::aeadDecrypt(*key,
                                                 &binData[kAssociatedDataLength],
                                                 binData.size() - kAssociatedDataLength,
                                                 &binData[0],
                                                 kAssociatedDataLength,
                                                 out.data(),
                                                 &outLen);
        if (!decryptStatus.isOK()) {
            uasserted(decryptStatus.code(), decryptStatus.reason());
        }

        uint8_t bsonType = binData[17];
        BSONObj parent;
        BSONObj decryptedObj = validateBSONElement(ConstDataRange(out.data(), outLen), bsonType);
        if (bsonType == BSONType::Object) {
            mozjs::ValueReader(cx, args.rval()).fromBSON(decryptedObj, &parent, true);
        } else {
            mozjs::ValueReader(cx, args.rval())
                .fromBSONElement(decryptedObj.firstElement(), parent, true);
        }
    }

    using EncryptionCallbacks::trace;
    void trace(JSTracer* trc) final {
        JS::TraceEdge(trc, &_collection, "collection object");
    }

    JS::Value getCollection() const {
        return _collection.get();
    }

    static void validateCollection(JSContext* cx, JS::HandleValue value) {
        uassert(ErrorCodes::BadValue,
                "Collection object must be provided to ClientSideFLEOptions",
                !(value.isNull() || value.isUndefined()));

        JS::RootedValue coll(cx, value);

        uassert(31043,
                "The collection object in ClientSideFLEOptions is invalid",
                mozjs::getScope(cx)->getProto<mozjs::DBCollectionInfo>().instanceOf(coll));
    }


    using DBClientBase::query;
    std::unique_ptr<DBClientCursor> query(const NamespaceStringOrUUID& nsOrUuid,
                                          Query query,
                                          int nToReturn,
                                          int nToSkip,
                                          const BSONObj* fieldsToReturn,
                                          int queryOptions,
                                          int batchSize) final {
        return _conn->query(
            nsOrUuid, query, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize);
    }

    bool isFailed() const final {
        return _conn->isFailed();
    }

    bool isStillConnected() final {
        return _conn->isStillConnected();
    }

    ConnectionString::ConnectionType type() const final {
        return _conn->type();
    }

    double getSoTimeout() const final {
        return _conn->getSoTimeout();
    }

    bool isReplicaSetMember() const final {
        return _conn->isReplicaSetMember();
    }

    bool isMongos() const final {
        return _conn->isMongos();
    }

private:
    NamespaceString getCollectionNS() {
        JS::RootedValue fullNameRooted(_cx);
        JS::RootedObject collectionRooted(_cx, &_collection.get().toObject());
        JS_GetProperty(_cx, collectionRooted, "_fullName", &fullNameRooted);
        if (!fullNameRooted.isString()) {
            uasserted(ErrorCodes::BadValue, "Collection object is incomplete.");
        }
        std::string fullName = mozjs::ValueWriter(_cx, fullNameRooted).toString();
        NamespaceString fullNameNS = NamespaceString(fullName);
        uassert(ErrorCodes::BadValue,
                str::stream() << "Invalid namespace: " << fullName,
                fullNameNS.isValid());
        return fullNameNS;
    }

    std::vector<uint8_t> getBinDataArg(mozjs::MozJSImplScope* scope,
                                       JSContext* cx,
                                       JS::CallArgs args,
                                       int index,
                                       BinDataType type) {
        if (!args.get(index).isObject() ||
            !scope->getProto<mozjs::BinDataInfo>().instanceOf(args.get(index))) {
            uasserted(ErrorCodes::BadValue, "First parameter must be a BinData object");
        }

        mozjs::ObjectWrapper o(cx, args.get(index));

        auto binType = BinDataType(static_cast<int>(o.getNumber(mozjs::InternedString::type)));
        uassert(ErrorCodes::BadValue,
                str::stream() << "Incorrect bindata type, expected" << typeName(type) << " but got "
                              << typeName(binType),
                binType == type);
        auto str = static_cast<std::string*>(JS_GetPrivate(args.get(index).toObjectOrNull()));
        uassert(ErrorCodes::BadValue, "Cannot call getter on BinData prototype", str);
        std::string string = base64::decode(*str);
        return std::vector<uint8_t>(string.data(), string.data() + string.length());
    }

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

    std::shared_ptr<SymmetricKey> getDataKey(const UUID& uuid) {
        auto ts_new = Date_t::now();

        if (_datakeyCache.hasKey(uuid)) {
            auto[key, ts] = _datakeyCache.find(uuid)->second;
            if (ts_new - ts < kCacheInvalidationTime) {
                return key;
            } else {
                _datakeyCache.erase(uuid);
            }
        }
        auto key = getDataKeyFromDisk(uuid);
        _datakeyCache.add(uuid, std::make_pair(key, ts_new));
        return key;
    }

    std::shared_ptr<SymmetricKey> getDataKeyFromDisk(const UUID& uuid) {
        NamespaceString fullNameNS = getCollectionNS();
        BSONObj dataKeyObj = _conn->findOne(fullNameNS.ns(), QUERY("_id" << uuid));
        if (dataKeyObj.isEmpty()) {
            uasserted(ErrorCodes::BadValue, "Invalid keyID.");
        }

        auto keyStoreRecord = KeyStoreRecord::parse(IDLParserErrorContext("root"), dataKeyObj);
        if (dataKeyObj.hasField("version"_sd)) {
            uassert(ErrorCodes::BadValue,
                    "Invalid version, must be either 0 or undefined",
                    dataKeyObj.getIntField("version"_sd) == 0);
        }

        BSONElement elem = dataKeyObj.getField("keyMaterial"_sd);
        uassert(ErrorCodes::BadValue, "Invalid key.", elem.isBinData(BinDataType::BinDataGeneral));
        uassert(ErrorCodes::BadValue,
                "Invalid version, must be either 0 or undefined",
                keyStoreRecord.getVersion() == 0);

        auto dataKey = keyStoreRecord.getKeyMaterial();
        uassert(ErrorCodes::BadValue, "Invalid data key.", dataKey.length() != 0);

        std::unique_ptr<KMSService> kmsService = KMSServiceController::createFromDisk(
            _encryptionOptions.getKmsProviders().toBSON(), keyStoreRecord.getMasterKey());
        SecureVector<uint8_t> decryptedKey =
            kmsService->decrypt(dataKey, keyStoreRecord.getMasterKey());
        return std::make_shared<SymmetricKey>(
            std::move(decryptedKey), crypto::aesAlgorithm, "kms_encryption");
    }

    std::vector<uint8_t> encryptWithKey(UUID uuid,
                                        const std::shared_ptr<SymmetricKey>& key,
                                        ConstDataRange plaintext,
                                        BSONType bsonType,
                                        int32_t algorithm) {
        // As per the description of the encryption algorithm for FLE, the
        // associated data is constructed of the following -
        // associatedData[0] = the FleAlgorithmEnum
        //      - either a 1 or a 2 depending on whether the iv is provided.
        // associatedData[1-16] = the uuid in bytes
        // associatedData[17] = the bson type

        ConstDataRange uuidCdr = uuid.toCDR();
        uint64_t outputLength = crypto::aeadCipherOutputLength(plaintext.length());
        std::vector<uint8_t> outputBuffer(kAssociatedDataLength + outputLength);
        outputBuffer[0] = static_cast<uint8_t>(algorithm);
        std::memcpy(&outputBuffer[1], uuidCdr.data(), uuidCdr.length());
        outputBuffer[17] = static_cast<uint8_t>(bsonType);
        uassertStatusOK(crypto::aeadEncrypt(*key,
                                            reinterpret_cast<const uint8_t*>(plaintext.data()),
                                            plaintext.length(),
                                            outputBuffer.data(),
                                            18,
                                            // The ciphertext starts 18 bytes into the output
                                            // buffer, as described above.
                                            outputBuffer.data() + 18,
                                            outputLength));
        return outputBuffer;
    }

    LRUCache<NamespaceString, SchemaInfo> _schemaCache{kEncryptedDBCacheSize};
    LRUCache<UUID, std::pair<std::shared_ptr<SymmetricKey>, Date_t>, UUID::Hash> _datakeyCache{
        kEncryptedDBCacheSize};
    std::unique_ptr<DBClientBase> _conn;
    ClientSideFLEOptions _encryptionOptions;
    JS::Heap<JS::Value> _collection;
    JSContext* _cx;
};  // class EncryptedDBClientBase

/**
 * Constructs a collection object from a namespace, passed in to the nsString parameter.
 * The client is the connection to a database in which you want to create the collection.
 * The collection parameter gets set to a javascript collection object.
 */
void createCollectionObject(JSContext* cx,
                            JS::HandleValue client,
                            StringData nsString,
                            JS::MutableHandleValue collection) {
    invariant(!client.isNull() && !client.isUndefined());

    auto ns = NamespaceString(nsString);
    uassert(ErrorCodes::BadValue,
            "Invalid keystore namespace.",
            ns.isValid() && NamespaceString::validCollectionName(ns.coll()));

    auto scope = mozjs::getScope(cx);

    // The collection object requires a database object to be constructed as well.
    JS::RootedValue databaseRV(cx);
    JS::AutoValueArray<2> databaseArgs(cx);

    databaseArgs[0].setObject(client.toObject());
    mozjs::ValueReader(cx, databaseArgs[1]).fromStringData(ns.db());
    scope->getProto<mozjs::DBInfo>().newInstance(databaseArgs, &databaseRV);

    invariant(databaseRV.isObject());
    auto databaseObj = databaseRV.toObjectOrNull();

    JS::AutoValueArray<4> collectionArgs(cx);
    collectionArgs[0].setObject(client.toObject());
    collectionArgs[1].setObject(*databaseObj);
    mozjs::ValueReader(cx, collectionArgs[2]).fromStringData(ns.coll());
    mozjs::ValueReader(cx, collectionArgs[3]).fromStringData(ns.ns());

    scope->getProto<mozjs::DBCollectionInfo>().newInstance(collectionArgs, collection);
}

// The parameters required to start FLE on the shell. The current connection is passed in as a
// parameter to create the keyvault collection object if one is not provided.
std::unique_ptr<DBClientBase> createEncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                                                          JS::HandleValue arg,
                                                          JS::HandleObject mongoConnection,
                                                          JSContext* cx) {

    uassert(
        31038, "Invalid Client Side Encryption parameters.", arg.isObject() || arg.isUndefined());

    static constexpr auto keyVaultClientFieldId = "keyVaultClient";

    if (!arg.isObject() && encryptedShellGlobalParams.awsAccessKeyId.empty()) {
        return conn;
    }

    ClientSideFLEOptions encryptionOptions;
    JS::RootedValue client(cx);
    JS::RootedValue collection(cx);

    if (!arg.isObject()) {
        // If arg is not an object, but one of the required encryptedShellGlobalParams
        // is defined, the user is trying to start an encrypted client with command line
        // parameters.

        AwsKMS awsKms = AwsKMS(encryptedShellGlobalParams.awsAccessKeyId,
                               encryptedShellGlobalParams.awsSecretAccessKey);

        awsKms.setUrl(StringData(encryptedShellGlobalParams.awsKmsURL));

        awsKms.setSessionToken(StringData(encryptedShellGlobalParams.awsSessionToken));

        KmsProviders kmsProviders;
        kmsProviders.setAws(awsKms);

        // The mongoConnection object will never be null.
        // If the encrypted shell is started through command line parameters, then the user must
        // default to the implicit connection for the keyvault collection.
        client.setObjectOrNull(mongoConnection.get());

        // Because we cannot add a schemaMap object through the command line, we set the
        // schemaMap object in ClientSideFLEOptions to be null so we know to always use
        // remote schemas.
        encryptionOptions = ClientSideFLEOptions(
            encryptedShellGlobalParams.keyVaultNamespace, std::move(kmsProviders), BSONObj());
    } else {
        uassert(ErrorCodes::BadValue,
                "Collection object must be passed to Field Level Encryption Options",
                arg.isObject());

        const BSONObj obj = mozjs::ValueWriter(cx, arg).toBSON();
        encryptionOptions = encryptionOptions.parse(IDLParserErrorContext("root"), obj);

        // IDL does not perform a deep copy of BSONObjs when parsing, so we must get an
        // owned copy of the schemaMap.
        encryptionOptions.setSchemaMap(encryptionOptions.getSchemaMap().getOwned());

        // This logic tries to extract the client from the args. If the connection object is defined
        // in the ClientSideFLEOptions struct, then the client will extract it and set itself to be
        // that. Else, the client will default to the implicit connection.
        JS::RootedObject handleObject(cx, &arg.toObject());
        JS_GetProperty(cx, handleObject, keyVaultClientFieldId, &client);
        if (client.isNull() || client.isUndefined()) {
            client.setObjectOrNull(mongoConnection.get());
        }
    }

    createCollectionObject(cx, client, encryptionOptions.getKeyVaultNamespace(), &collection);

    std::unique_ptr<EncryptedDBClientBase> base =
        std::make_unique<EncryptedDBClientBase>(std::move(conn), encryptionOptions, collection, cx);
    return std::move(base);
}

MONGO_INITIALIZER(setCallbacksForEncryptedDBClientBase)(InitializerContext*) {
    mongo::mozjs::setEncryptedDBClientCallback(createEncryptedDBClientBase);
    return Status::OK();
}


}  // namespace
}  // namespace mongo
