/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "encryptdb/symmetric_crypto.h"
#include "fle/encryption/aead_encryption.h"
#include "fle/shell/encrypted_shell_options.h"
#include "fle/shell/kms.h"
#include "fle/shell/kms_gen.h"
#include "mongo/base/data_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/client/dbclient_base.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/rpc/object_check.h"
#include "mongo/scripting/mozjs/bindata.h"
#include "mongo/scripting/mozjs/implscope.h"
#include "mongo/scripting/mozjs/maxkey.h"
#include "mongo/scripting/mozjs/minkey.h"
#include "mongo/scripting/mozjs/mongo.h"
#include "mongo/scripting/mozjs/objectwrapper.h"
#include "mongo/scripting/mozjs/valuereader.h"
#include "mongo/scripting/mozjs/valuewriter.h"

namespace mongo {
EncryptedShellGlobalParams encryptedShellGlobalParams;

namespace {
constexpr int kAssociatedDataLength = 18;

class EncryptedDBClientBase final : public DBClientBase, public mozjs::EncryptionCallbacks {

public:
    EncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                          ClientSideFLEOptions encryptionOptions,
                          JS::HandleValue collection,
                          JSContext* cx)
        : _conn(std::move(conn)), _encryptionOptions(std::move(encryptionOptions)), _cx(cx) {
        if (!collection.isNull() && !collection.isUndefined()) {
            validateCollection(cx, collection);
        }
        _collection = JS::Heap<JS::Value>(collection);
    };

    std::string getServerAddress() const final {
        return _conn->getServerAddress();
    }

    bool call(Message& toSend, Message& response, bool assertOk, std::string* actualServer) final {
        return _conn->call(toSend, response, assertOk, actualServer);
    }

    void say(Message& toSend, bool isRetry, std::string* actualServer) final {
        _conn->say(toSend, isRetry, actualServer);
    }

    bool lazySupported() const final {
        return _conn->lazySupported();
    }

    using DBClientBase::runCommandWithTarget;
    std::pair<rpc::UniqueReply, DBClientBase*> runCommandWithTarget(OpMsgRequest request) final {
        return _conn->runCommandWithTarget(std::move(request));
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
        if (args.length() != 1) {
            uasserted(ErrorCodes::BadValue, "generateDataKey requires 1 arg");
        }

        if (!args.get(0).isString()) {
            uasserted(ErrorCodes::BadValue, "1st param to generateDataKey has to be a string");
        }

        std::string clientMasterKey = mozjs::ValueWriter(cx, args.get(0)).toString();
        std::unique_ptr<KMSService> kmsService =
            KMSServiceController::createFromClient(_encryptionOptions.toBSON());

        SecureVector<uint8_t> dataKey(crypto::kAeadAesHmacKeySize);
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
        if (args.length() < 2 || args.length() > 3) {
            uasserted(ErrorCodes::BadValue, "encrypt requires at least 2 args and at most 3 args");
        }

        if (!(args.get(1).isObject() || args.get(1).isString() || args.get(1).isNumber() ||
              args.get(1).isBoolean())) {
            uasserted(ErrorCodes::BadValue,
                      "Second parameter must be an object, string, number, or bool");
        }

        // Extract the UUID from the callArgs
        auto binData = getBinDataArg(scope, cx, args, 0, BinDataType::newUUID);
        UUID uuid = UUID::fromCDR(ConstDataRange(binData.data(), binData.size()));

        // Extract the IV
        boost::optional<std::vector<uint8_t>> iv;
        if (args.length() == 3) {
            if (!args.get(2).isObject()) {
                uasserted(ErrorCodes::BadValue, "IV parameter must be of BinData type.");
            }
            if (!scope->getProto<mozjs::BinDataInfo>().instanceOf(args.get(2))) {
                uasserted(ErrorCodes::BadValue, "First parameter must be an IV (BinData type)");
            }

            iv = getBinDataArg(scope, cx, args, 2, BinDataType::BinDataGeneral);
            uassert(
                ErrorCodes::BadValue, "IV must be exactly 16 bytes long", iv.get().size() == 16);
        }

        BSONType bsonType = BSONType::EOO;

        BufBuilder plaintext;
        if (args.get(1).isObject()) {
            JS::RootedObject rootedObj(cx, &args.get(1).toObject());
            auto jsclass = JS_GetClass(rootedObj);

            if (strcmp(jsclass->name, "Object") == 0) {
                // If it is a JS Object, then we can extract all the information by simply calling
                // ValueWriter.toBSON and setting the type bit, which is what is happening below.
                BSONObj valueObj = mozjs::ValueWriter(cx, args.get(1)).toBSON();
                plaintext.appendBuf(valueObj.objdata(), valueObj.objsize());
                bsonType = BSONType::Object;

            } else if (scope->getProto<mozjs::MinKeyInfo>().getJSClass() == jsclass ||
                       scope->getProto<mozjs::MaxKeyInfo>().getJSClass() == jsclass ||
                       scope->getProto<mozjs::DBRefInfo>().getJSClass() == jsclass) {
                uasserted(ErrorCodes::BadValue,
                          "Second parameter cannot be MinKey, MaxKey, or DBRef");
            } else {
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
            double valueNum = mozjs::ValueWriter(cx, args.get(1)).toNumber();
            plaintext.appendNum(valueNum);
            bsonType = BSONType::NumberDouble;
        } else if (args.get(1).isBoolean()) {
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
        std::vector<uint8_t> plaintextVec(plaintext.buf(), plaintext.buf() + plaintext.len());

        SymmetricKey key(getDataKey(uuid));
        std::vector<uint8_t> fleBlob =
            encryptWithKey(uuid, std::move(key), plaintextVec, bsonType, iv);

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
        ;

        uassert(ErrorCodes::BadValue,
                "Ciphertext blob too small",
                binData.size() > kAssociatedDataLength);
        uassert(ErrorCodes::BadValue,
                "Ciphertext blob algorithm unknown",
                (FleAlgorithmInt(binData[0]) == FleAlgorithmInt::kDeterministic ||
                 FleAlgorithmInt(binData[0]) == FleAlgorithmInt::kRandom));

        ConstDataRange uuidCdr = ConstDataRange(&binData[1], UUID::kNumBytes);
        UUID uuid = UUID::fromCDR(uuidCdr);

        SymmetricKey key(getDataKey(uuid));
        std::vector<uint8_t> out(binData.size() - kAssociatedDataLength);
        size_t outLen = out.size();

        auto decryptStatus = crypto::aeadDecrypt(key,
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

        if (bsonType == BSONType::Object) {
            ConstDataRangeCursor cdc = ConstDataRangeCursor(ConstDataRange(out.data(), outLen));
            BSONObj valueObj, parent;

            valueObj = cdc.readAndAdvance<Validated<BSONObj>>();
            mozjs::ValueReader(cx, args.rval()).fromBSON(valueObj.getOwned(), &parent, true);
        } else {
            BSONObj parent;
            auto valueString = "value"_sd;

            // The size here is to construct a new BSON document and validate the
            // total size of the object. The first four bytes is for the size of an
            // int32_t, then a space for the type of the first element, then the space
            // for the value string and the the 0x00 terminated field name, then the
            // size of the actual data, then the last byte for the end document character,
            // also 0x00.
            size_t docLength = sizeof(int32_t) + 1 + valueString.size() + 1 + outLen + 1;
            BufBuilder builder;
            builder.reserveBytes(docLength);

            uassert(ErrorCodes::BadValue,
                    "invalid decryption value",
                    docLength < std::numeric_limits<int32_t>::max());

            builder.appendNum(static_cast<uint32_t>(docLength));
            builder.appendChar(static_cast<uint8_t>(bsonType));
            builder.appendStr(valueString, true);
            builder.appendBuf(out.data(), outLen);
            builder.appendChar('\0');

            ConstDataRangeCursor cdc =
                ConstDataRangeCursor(ConstDataRange(builder.buf(), builder.len()));
            BSONObj o = cdc.readAndAdvance<Validated<BSONObj>>();

            auto element = o.firstElement();

            mozjs::ValueReader(cx, args.rval()).fromBSONElement(element, parent, true);
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


    SymmetricKey getDataKey(const UUID& uuid) {
        JS::RootedValue fullNameRooted(_cx);
        JS::RootedObject collectionRooted(_cx, &_collection.get().toObject());
        JS_GetProperty(_cx, collectionRooted, "_fullName", &fullNameRooted);
        if (!fullNameRooted.isString()) {
            uasserted(ErrorCodes::BadValue, "Collection object is the wrong type");
        }
        std::string fullName = mozjs::ValueWriter(_cx, fullNameRooted).toString();
        NamespaceString fullNameNS = NamespaceString(fullName);

        BSONObj dataKeyObj = _conn->findOne(fullNameNS.ns(), QUERY("_id" << uuid));
        if (dataKeyObj.isEmpty()) {
            uasserted(ErrorCodes::BadValue, "Invalid keyID.");
        }

        if (dataKeyObj.hasField("version"_sd)) {
            uassert(ErrorCodes::BadValue,
                    "Invalid version, must be either 0 or undefined",
                    dataKeyObj.getIntField("version"_sd) == 0);
        }

        BSONElement elem = dataKeyObj.getField("keyMaterial"_sd);
        uassert(ErrorCodes::BadValue, "Invalid key.", elem.isBinData(BinDataType::BinDataGeneral));

        int len;
        const char* dataKey = elem.binData(len);
        if (len == 0) {
            uasserted(ErrorCodes::BadValue, "Invalid key.");
        }

        BSONObj masterKey = dataKeyObj.getObjectField("masterKey"_sd);
        uassert(ErrorCodes::BadValue,
                "Key in keystore missing metadata field 'masterKey'",
                !masterKey.isEmpty());

        std::unique_ptr<KMSService> kmsService =
            KMSServiceController::createFromDisk(_encryptionOptions.toBSON(), masterKey);
        SecureVector<uint8_t> decryptedKey = kmsService->decrypt(ConstDataRange(dataKey, len));
        return SymmetricKey(std::move(decryptedKey), crypto::aesAlgorithm, "kms_encryption");
    }

    std::vector<uint8_t> encryptWithKey(UUID uuid,
                                        SymmetricKey key,
                                        std::vector<uint8_t> plaintext,
                                        BSONType bsonType,
                                        boost::optional<std::vector<uint8_t>>& iv) {
        // As per the description of the encryption algorithm for FLE, the
        // associated data is constructed of the following -
        // associatedData[0] = the FleAlgorithmEnum
        //      - either a 1 or a 2 depending on whether the iv is provided.
        // associatedData[1-16] = the uuid in bytes
        // associatedData[17] = the bson type

        ConstDataRange uuidCdr = uuid.toCDR();
        uint64_t outputLength = crypto::aeadCipherOutputLength(plaintext.size());
        std::vector<uint8_t> outputBuffer(kAssociatedDataLength + outputLength);
        std::memcpy(&outputBuffer[1], uuidCdr.data(), uuidCdr.length());
        outputBuffer[17] = static_cast<uint8_t>(bsonType);

        uint8_t FleAlgorithm;
        if (iv) {
            FleAlgorithm = static_cast<uint8_t>(FleAlgorithmInt::kDeterministic);
            outputBuffer[0] = FleAlgorithm;
            uassertStatusOK(crypto::aeadEncrypt(key,
                                                plaintext.data(),
                                                plaintext.size(),
                                                iv->data(),
                                                iv->size(),
                                                outputBuffer.data(),
                                                18,
                                                outputBuffer.data() + 18,
                                                outputLength));
        } else {
            FleAlgorithm = static_cast<uint8_t>(FleAlgorithmInt::kRandom);
            outputBuffer[0] = FleAlgorithm;
            uassertStatusOK(crypto::aeadEncrypt(key,
                                                plaintext.data(),
                                                plaintext.size(),
                                                nullptr,
                                                0,
                                                outputBuffer.data(),
                                                18,
                                                outputBuffer.data() + 18,
                                                outputLength));
        }
        return outputBuffer;
    }


    std::unique_ptr<DBClientBase> _conn;
    ClientSideFLEOptions _encryptionOptions;
    JS::Heap<JS::Value> _collection;
    JSContext* _cx;
};  // class EncryptedDBClientBase

std::unique_ptr<DBClientBase> createEncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                                                          JS::HandleValue arg,
                                                          JSContext* cx) {

    uassert(
        31038, "Invalid Client Side Encryption parameters.", arg.isObject() || arg.isUndefined());

    static constexpr auto keyVaultCollectionFieldId = "keyVaultCollection"_sd;

    if (!arg.isObject() && encryptedShellGlobalParams.awsAccessKeyId.empty()) {
        return conn;
    }

    ClientSideFLEOptions encryptionOptions;
    JS::RootedValue collection(cx);

    if (!arg.isObject()) {
        AwsKMS awsKms = AwsKMS(encryptedShellGlobalParams.awsAccessKeyId,
                               encryptedShellGlobalParams.awsSecretAccessKey);

        boost::optional<StringData> awsSessionToken(encryptedShellGlobalParams.awsSessionToken);
        awsKms.setSessionToken(awsSessionToken);

        encryptionOptions = ClientSideFLEOptions(std::move(awsKms));
    } else {
        const BSONObj obj = mozjs::ValueWriter(cx, arg).toBSON();
        encryptionOptions = encryptionOptions.parse(IDLParserErrorContext("root"), obj);

        // TODO SERVER-39897 Parse and validate that the collection exists.
        JS::RootedObject handleObject(cx, &arg.toObject());
        JS_GetProperty(cx, handleObject, keyVaultCollectionFieldId.rawData(), &collection);
    }
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
