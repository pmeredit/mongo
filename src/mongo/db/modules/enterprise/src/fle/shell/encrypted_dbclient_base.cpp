/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "fle/shell/encrypted_shell_options.h"
#include "fle/shell/kms_gen.h"
#include "mongo/client/dbclient_base.h"
#include "mongo/scripting/mozjs/mongo.h"
#include "mongo/scripting/mozjs/objectwrapper.h"
#include "mongo/scripting/mozjs/valuereader.h"
#include "mongo/scripting/mozjs/valuewriter.h"

namespace mongo {
EncryptedShellGlobalParams encryptedShellGlobalParams;

namespace {

class EncryptedDBClientBase final : public DBClientBase {

public:
    EncryptedDBClientBase(std::unique_ptr<DBClientBase> conn,
                          ClientSideFLEOptions encryptionOptions,
                          JS::HandleValue collection,
                          JSContext* cx)
        : _conn(std::move(conn)),
          _encryptionOptions(std::move(encryptionOptions)),
          _collection(collection),
          _cx(cx){};

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
    std::unique_ptr<DBClientBase> _conn;
    ClientSideFLEOptions _encryptionOptions;
    JS::HandleValue _collection;
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
        const BSONObj obj = mongo::mozjs::ValueWriter(cx, arg).toBSON();
        encryptionOptions = encryptionOptions.parse(IDLParserErrorContext("root"), obj);

        // TODO SERVER-39897 Parse and validate that the collection exists.
        JS::RootedObject handleObject(cx, &arg.toObject());
        JS_GetProperty(cx, handleObject, keyVaultCollectionFieldId.rawData(), &collection);
    }
    return std::make_unique<EncryptedDBClientBase>(
        std::move(conn), encryptionOptions, collection, cx);
}

MONGO_INITIALIZER(setEncryptedDBClientBaseCallback)(InitializerContext*) {
    mongo::mozjs::setEncryptedDBClientCallback(createEncryptedDBClientBase);
    return Status::OK();
}


}  // namespace
}  // namespace mongo
