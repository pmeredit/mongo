/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <boost/filesystem/path.hpp>
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

const std::string kSystemKeyId = ".system";
const std::string kMasterKeyId = ".master";

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
     * Get the WT table encryption config for a specific namespace
     * or internal WT table.
     */
    std::string getOpenConfig(StringData ns) override;

    /**
     * Take any initialization action that needs to wait until after storage engine initialization
     * including:
     *
     * - Verify that the server has not been started without encryption before.
     * - Rotate the master key.
     *
     * Returns a bool indicating if the server should continue start or not.
     */
    bool restartRequired() override;

    /**
     * Takes in a key identifier and returns a unique_ptr to the
     * associated encryption key for that keyID.
     */
    virtual StatusWith<std::unique_ptr<SymmetricKey>> getKey(const std::string& keyId);

private:
    /**
     * Validate master key config and initiate the local key store.
     */
    Status _initLocalKeystore();

    /**
     * Open a local key store at 'path', create it if it doesn't exist.
     */
    Status _openKeystore(const boost::filesystem::path& path, WT_CONNECTION** conn);

    /**
     * Rotate the master encryption key and create a new key store.
     */
    Status _rotateMasterKey(const std::string& newKeyId);

    /**
     * Internal key management helper methods
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getSystemKey();
    StatusWith<std::unique_ptr<SymmetricKey>> _getMasterKey();
    StatusWith<std::unique_ptr<SymmetricKey>> _readKey(const std::string& keyId);

    /**
     * Acquires the master key 'keyId' from a KMIP server.
     *
     * If 'keyId' is empty a new key will be created and keyId will be assigned the new id and hence
     * as as an out parameter.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getKeyFromKMIPServer(const std::string& keyId);

    /**
     * Acquires the system key from the encryption keyfile.
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getKeyFromKeyFile();

    /**
     * Taken from global storage parameters -- the dbpath directory.
     */
    boost::filesystem::path _dbPath;
    boost::filesystem::path _keystoreBasePath;

    /**
     * The master system key, provided via KMIP or a keyfile.
     */
    std::unique_ptr<SymmetricKey> _masterKey;
    std::string _masterKeyId;

    /**
     * The master system key for the new key store when doing key rotation.
     */
    std::unique_ptr<SymmetricKey> _rotMasterKey;
    bool _keyRotationAllowed;

    /**
     * Management of the local WiredTiger key store.
     */
    WT_CONNECTION* _keystoreConnection;
    WT_EVENT_HANDLER _keystoreEventHandler;

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
