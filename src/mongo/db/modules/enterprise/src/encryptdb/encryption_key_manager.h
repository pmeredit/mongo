/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

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

    ~EncryptionKeyManager() override = default;

    /**
     * Get the key manager from a service context.
     */
    static EncryptionKeyManager* get(ServiceContext* service);

    void appendUID(BSONObjBuilder* builder) override;

    std::string getOpenConfig(StringData tableName) override;

    /**
     * Takes in a key identifier and returns a unique_ptr to the
     * associated encryption key for that keyID.
     */
    virtual StatusWith<std::unique_ptr<SymmetricKey>> getKey(const std::string& keyID);

    /**
     * Removes a key for a database that is being dropped. Responsible for removing the
     * relevant data from local.sys.keyids and storage engine metadata.
     * TODO: Implement together with multikey support
     */
    Status removeKey(const std::string& dbname);

private:
    /**
     * Acquires the system key from the key management system.
     */
    Status _acquireSystemKey();

    /**
     * Acquires the system key from a KMIP server specifically.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getKeyFromKMIPServer();

    /**
     * Acquires the system key from the encryption keyfile.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getKeyFromKeyFile();

    /**
     * Reads the systemKey UID from storage.bson.
     */
    std::string _getKeyUIDFromMetadata();

    /**
     * Flag indicating if the key manager has been initialized yet.
     */
    bool _initialized;

    /**
     * Taken from global storage parameters -- the dbpath directory.
     */
    std::string _dbPath;

    /**
     * The master system key, provided via KMIP or a keyfile.
     */
    std::unique_ptr<SymmetricKey> _systemKey;

    /**
     * The id of the master system key.
     */
    std::string _systemKeyId;

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
