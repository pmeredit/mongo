/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/filesystem/path.hpp>
#include <vector>
#include <wiredtiger.h>

#include "encryption_options.h"
#include "keystore.h"
#include "keystore_metadata.h"
#include "mongo/crypto/symmetric_key.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_customization_hooks.h"
#include "mongo/util/synchronized_value.h"
#include "symmetric_crypto.h"

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
 * Note that EncryptionHooks provides additionalBytesForProtectedBuffer. This function
 * returns a constant which defines how many more bytes a ciphertext payload may have than its
 * corresponding cleartext. It is computed by:
 *  max(AllPaddingModes) + sizeof(conditional version number)
 *  Where max(AllPaddingModes) can be found in crypto::kMaxHeaderSize
 */
class EncryptionKeyManager : public EncryptionHooks {
    EncryptionKeyManager(const EncryptionKeyManager&) = delete;
    EncryptionKeyManager& operator=(const EncryptionKeyManager&) = delete;

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
     * Encrypt temporary data written to disk outside of the storage engine. The method uses the
     * key associated with the database the data comes from, which is persistent across server
     * restarts, if dbName is provided. Otherwise, it uses the generated key _tmpKey, which is
     * subject to change every time the server restarts.
     */
    Status protectTmpData(const uint8_t* in,
                          size_t inLen,
                          uint8_t* out,
                          size_t outLen,
                          size_t* resultLen,
                          boost::optional<DatabaseName> dbName) override;

    /**
     * Decrypt temporary data written to disk outside of the storage engine. The method uses the
     * key associated with the database the data comes from, which is persistent across server
     * restarts.
     */
    Status unprotectTmpData(const uint8_t* in,
                            size_t inLen,
                            uint8_t* out,
                            size_t outLen,
                            size_t* resultLen,
                            boost::optional<DatabaseName> dbName) override;
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
    using FindMode = Keystore::Session::FindMode;
    virtual StatusWith<std::unique_ptr<SymmetricKey>> getKey(
        const SymmetricKeyId& keyId, FindMode mode = FindMode::kIdOrCurrent);

    SymmetricKeyId getMasterKeyId() {
        return _masterKey->getKeyId();
    }

    std::string getCipherMode() {
        return _encryptionParams->encryptionCipherMode;
    }

    /**
     * Returns the current version of the keystore.
     */
    std::int32_t getKeystoreVersion() const;

    /**
     * Access the current rollover ID for testing purposes.
     */
    std::uint32_t getRolloverId() const {
        return _keystore->getRolloverId();
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
    StatusWith<std::unique_ptr<SymmetricKey>> _getSystemKey(const SymmetricKeyId& keyId, FindMode);
    StatusWith<std::unique_ptr<SymmetricKey>> _getMasterKey();
    StatusWith<std::unique_ptr<SymmetricKey>> _readKey(const SymmetricKeyId& keyId, FindMode);

    enum class PathMode { kValid, kInvalid, kInitializing };
    boost::filesystem::path _metadataPath(PathMode mode);

    /**
     * Taken from global storage parameters -- the dbpath directory.
     */
    boost::filesystem::path _dbPath;
    boost::filesystem::path _keystoreBasePath;
    boost::filesystem::path _keystorePath;

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
    synchronized_value<KeystoreMetadataFile> _keystoreMetadata;

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
    EncryptionWiredTigerCustomizationHooks(const EncryptionWiredTigerCustomizationHooks&) = delete;
    EncryptionWiredTigerCustomizationHooks& operator=(
        const EncryptionWiredTigerCustomizationHooks&) = delete;

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
