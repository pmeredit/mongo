/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <boost/filesystem/path.hpp>
#include <vector>
#include <wiredtiger.h>

#include "encryption_options.h"
#include "keystore.h"
#include "keystore_metadata.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/service_context.h"
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
     * Opens a backup cursor on the underlying WT database. Returns the list of files that need to
     * be copied by the application as part of the backup. The file paths may be absolute or
     * relative; it depends on the dbpath passed into MongoDB.
     */
    StatusWith<std::vector<std::string>> beginNonBlockingBackup() override;

    /**
     * Close the backup cursor and release resources opened by `beginNonBlockingBackup`.
     */
    Status endNonBlockingBackup() override;

    /**
     * Takes in a key identifier and returns a unique_ptr to the
     * associated encryption key for that keyID.
     */
    virtual StatusWith<std::unique_ptr<SymmetricKey>> getKey(const SymmetricKeyId& keyId);

    SymmetricKeyId getMasterKeyId() {
        return _masterKey->getKeyId();
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
     * Rotate the master encryption key and create a new key store.
     */
    Status _rotateMasterKey(const std::string& newKeyId);

    /**
     * Internal key management helper methods
     */
    StatusWith<std::unique_ptr<SymmetricKey>> _getSystemKey();
    StatusWith<std::unique_ptr<SymmetricKey>> _getMasterKey();
    StatusWith<std::unique_ptr<SymmetricKey>> _readKey(const SymmetricKeyId& keyId);

    enum class PathMode { kValid, kInvalid, kInitializing };
    boost::filesystem::path _metadataPath(PathMode mode);

    /**
     * Taken from global storage parameters -- the dbpath directory.
     */
    boost::filesystem::path _dbPath;
    boost::filesystem::path _keystoreBasePath;
    boost::filesystem::path _wtConnPath;

    /**
     * The master system key, provided via KMIP or a keyfile.
     */
    UniqueSymmetricKey _masterKey;
    bool _masterKeyRequested;

    /**
     * The master system key for the new key store when doing key rotation.
     */
    std::unique_ptr<SymmetricKey> _rotMasterKey;
    bool _keyRotationAllowed;

    /**
     * Metadata file containing schema version and whether they keystore is dirty.
     */
    stdx::mutex _keystoreMetadataMutex;
    KeystoreMetadataFile _keystoreMetadata;

    /**
     * Ephemeral key whose life time does not span server restarts.
     */
    const SymmetricKey _tmpDataKey;

    /**
     * Management of the local WiredTiger key store.
     */
    std::unique_ptr<Keystore> _keystore;
    std::unique_ptr<Keystore::Session> _backupSession;

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
 * Initialize the encryption key manager attached to the given ServiceContext.
 *
 * Note: Not idempotent. It will overwrite the existing encryption key manager.
 */
void initializeEncryptionKeyManager(ServiceContext* service);

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
