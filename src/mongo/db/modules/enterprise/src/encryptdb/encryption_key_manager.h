/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <wiredtiger.h>

#include "kmip_service.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_customization_hooks.h"
#include "symmetric_crypto.h"
#include "symmetric_key.h"

namespace mongo {

using namespace mongo::kmip;
struct EncryptionGlobalParams;
class NamespaceString;
class ServiceContext;
class SSLManagerInterface;
struct SSLParams;

/**
 * The EncryptionKeyManager manages the keys for the encrypted storage engine.
 */
class EncryptionKeyManager : public WiredTigerCustomizationHooks {
    MONGO_DISALLOW_COPYING(EncryptionKeyManager);

public:
    /**
     * Initialize the EncryptionKeyManager. The encryptionParams and SSLParams
     * must outlive the EncryptionKeyManager.
     */
    EncryptionKeyManager(const std::string& dbPath,
                         EncryptionGlobalParams* encryptionParams,
                         SSLParams* sslParams);

    ~EncryptionKeyManager() override;

    /**
     * Get the key manager from a service context.
     */
    static EncryptionKeyManager* get(ServiceContext* service);

    /**
     * Methods overriding the customization hooks base class
     */
    void appendUID(BSONObjBuilder* builder) override;

    std::string getOpenConfig(StringData ns) override;

    /**
     * Takes in a key identifier and returns a unique_ptr to the
     * associated encryption key for that keyID.
     */
    virtual StatusWith<std::unique_ptr<SymmetricKey>> getKey(const std::string& keyId);

private:
    /**
     * Acquires the master key from the key management system.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _acquireMasterKey();

    /**
     * Open a connection to the local key database
     */
    Status _initLocalKeyStore();

    /**
     * Acquires the system key from a KMIP server specifically.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getKeyFromKMIPServer();

    /**
     * Acquires the system key from the encryption keyfile.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getKeyFromKeyFile();

    /**
     * Reads the masterKey UID from storage.bson.
     */
    StatusWith<std::string> _getKeyUIDFromMetadata();

    /**
     * Taken from global storage parameters -- the dbpath directory.
     */
    std::string _dbPath;

    /**
     * The master system key, provided via KMIP or a keyfile.
     */
    std::unique_ptr<SymmetricKey> _masterKey;

    /**
     * The id of the master system key.
     */
    std::string _kmipMasterKeyId;

    /**
     * Flag set when the local key store has been initialized.
     */
    bool _localKeyStoreInitialized;

    /**
     * Management of the local WiredTiger key store.
     */
    WT_CONNECTION* _keyStorageConnection;
    WT_EVENT_HANDLER _keyStorageEventHandler;

    /**
     * Pointer to the encryption parameters to use.
     */
    EncryptionGlobalParams* _encryptionParams;

    /**
     * Pointer to the SSLParams struct to use.
     */
    SSLParams* _sslParams;
};
}  // namespace mongo
