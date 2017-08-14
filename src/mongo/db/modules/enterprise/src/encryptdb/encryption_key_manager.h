/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <boost/filesystem/path.hpp>
#include <vector>
#include <wiredtiger.h>

#include "encryption_options.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_customization_hooks.h"
#include "symmetric_crypto.h"
#include "symmetric_key.h"

namespace mongo {

class DataProtector;
class NamespaceString;
class ServiceContext;
class SSLManagerInterface;
struct SSLParams;

const std::string kSystemKeyId = ".system";
const std::string kMasterKeyId = ".master";
const std::string kTmpDataKeyId = ".tmp";
const std::string kEncryptionEntrypointConfig =
    "local={entry=mongo_addWiredTigerEncryptors,early_load=true},";

/**
 * The EncryptionKeyManager manages the keys for the encrypted storage engine.
 *
 *
 * Note that EncryptionHooks provides additionalBytesForProtectedBuffer. This function
 * returns a constant which defines how many more bytes a ciphertext payload may have than its
 * corresponding cleartext. It is computed by:
 *  max(MaxCBCPadding, MaxGCMPadding) + sizeof(conditional version number)
 *  = max(CBC IV + aes blocksize, GCM IV + GCM tag) + 1 // GCM is a stream cipher, so doesn't need
 *                                                         a padded AES block
 *  = max(16 + 16, 12 + 12) + 1
 *  = 32 + 1 = 33
 */
class EncryptionKeyManager : public EncryptionHooks {
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
     * Indicates that encryption at rest is enabled.
     */
    bool enabled() const override;

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

    std::unique_ptr<DataProtector> getDataProtector() override;

    boost::filesystem::path getProtectedPathSuffix() override;

    /**
     * Encrypt temporary data written to disk outside of the storage engine. The method uses an
     * ephemeral key _tmpDataKey which is re-generated on each server restart.
     */
    Status protectTmpData(
        const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen, size_t* resultLen) override;

    /**
     * Decrypt temporary data previously written to disk outside of the storage engine.
     */
    Status unprotectTmpData(
        const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen, size_t* resultLen) override;

    /**
     * Takes in a key identifier and returns a unique_ptr to the
     * associated encryption key for that keyID.
     */
    virtual StatusWith<std::unique_ptr<SymmetricKey>> getKey(const std::string& keyId);

    std::string getMasterKeyId() {
        return _masterKeyId;
    }

    std::string getCipherMode() {
        return _encryptionParams->encryptionCipherMode;
    }

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
     * Taken from global storage parameters -- the dbpath directory.
     */
    boost::filesystem::path _dbPath;
    boost::filesystem::path _keystoreBasePath;

    /**
     * The master system key, provided via KMIP or a keyfile.
     */
    std::unique_ptr<SymmetricKey> _masterKey;
    std::string _masterKeyId;
    bool _masterKeyRequested;

    /**
     * The master system key for the new key store when doing key rotation.
     */
    std::unique_ptr<SymmetricKey> _rotMasterKey;
    bool _keyRotationAllowed;

    /**
     * Ephemeral key whose life time does not span server restarts.
     */
    const SymmetricKey _tmpDataKey;

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

/**
 * Initialize the global encryption key manager.
 *
 * Note: Not idempotent. It will overwrite the existing global encryption key manager.
 */
void InitializeGlobalEncryptionKeyManager();

class EncryptionWiredTigerCustomizationHooks : public WiredTigerCustomizationHooks {
    MONGO_DISALLOW_COPYING(EncryptionWiredTigerCustomizationHooks);

public:
    /**
     * Initialize the EncryptionWiredTigerCustomizationHooks. The encryptionParams and SSLParams
     * must outlive the configuration customizer.
     */
    EncryptionWiredTigerCustomizationHooks(EncryptionGlobalParams* encryptionParams);

    ~EncryptionWiredTigerCustomizationHooks() override;

    /**
     * Get the WT table encryption config for a specific namespace or internal WT table on a
     * `Session::create` call.
     */
    std::string getTableCreateConfig(StringData ns) override;

private:
    /**
     * Pointer to the encryption parameters to use. The parameters must outlive the object.
     */
    EncryptionGlobalParams* _encryptionParams;
};

}  // namespace mongo
