/**
 * Copyright (C) 2015 MongoDB Inc.
 * The main KMIP API that is exposed to the rest of the codebase.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "encryption_key_manager.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <tuple>

#include "encrypted_data_protector.h"
#include "encryption_key_acquisition.h"
#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_customization_hooks.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace fs = boost::filesystem;

namespace {
const std::string kInvalidatedKeyword = "-invalidated-";
const std::string kInitializingKeyword = "-initializing";
const std::string kKeystoreMetadataFilename = "keystore.metadata";

Status createDirectoryIfNeeded(const fs::path& path) {
    try {
        if (!fs::exists(path)) {
            fs::create_directory(path);
        }
    } catch (const std::exception& e) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Error creating path " << path.string() << ' ' << e.what());
    }
    return Status::OK();
}

bool hasExistingDatafiles(const fs::path& path) {
    fs::path metadataPath = path / "storage.bson";
    fs::path wtDatafilePath = path / "WiredTiger.wt";

    bool hasDatafiles = true;  // default to the error condition
    try {
        hasDatafiles = fs::exists(metadataPath) || fs::exists(wtDatafilePath);
    } catch (const std::exception& e) {
        severe() << "Caught exception when checking for existence of data files: " << e.what();
    }
    return hasDatafiles;
}

}  // namespace

EncryptionKeyManager::EncryptionKeyManager(const std::string& dbPath,
                                           EncryptionGlobalParams* encryptionParams,
                                           SSLParams* sslParams)
    : _dbPath(fs::path(dbPath)),
      _keystoreBasePath(fs::path(dbPath) / "key.store"),
      _masterKeyRequested(false),
      _keyRotationAllowed(false),
      _tmpDataKey(crypto::aesGenerate(crypto::sym256KeySize, kTmpDataKeyId)),
      _encryptionParams(encryptionParams),
      _sslParams(sslParams) {}

EncryptionKeyManager::~EncryptionKeyManager() {}

EncryptionKeyManager* EncryptionKeyManager::get(ServiceContext* service) {
    // TODO: Does this need to change now that there are different hooks implemented?
    EncryptionKeyManager* globalKeyManager =
        checked_cast<EncryptionKeyManager*>(EncryptionHooks::get(service));
    fassert(4036, globalKeyManager != nullptr);
    return globalKeyManager;
}

bool EncryptionKeyManager::enabled() const {
    return true;
}

bool EncryptionKeyManager::restartRequired() {
    if (_encryptionParams->rotateMasterKey) {
        Status status = _rotateMasterKey(_encryptionParams->kmipKeyIdentifierRot);
        if (!status.isOK()) {
            severe() << "Failed to rotate master key: " << status.reason();
        }
        // The server should always exit after a key rotation.
        return true;
    }

    if (_encryptionParams->encryptionKeyFile.empty()) {
        log() << "Encryption key manager initialized using KMIP key with id: "
              << _masterKey->getKeyId() << ".";
    } else {
        log() << "Encryption key manager initialized with key file: "
              << _encryptionParams->encryptionKeyFile;
    }

    return false;
}

Status EncryptionKeyManager::protectTmpData(
    const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen, size_t* resultLen) {
    return crypto::aesEncrypt(
        _tmpDataKey,
        crypto::getCipherModeFromString(_encryptionParams->encryptionCipherMode),
        crypto::PageSchema::k0,
        in,
        inLen,
        out,
        outLen,
        resultLen);
}

std::unique_ptr<DataProtector> EncryptionKeyManager::getDataProtector() {
    return stdx::make_unique<EncryptedDataProtector>(_masterKey.get(), crypto::aesMode::cbc);
}

boost::filesystem::path EncryptionKeyManager::getProtectedPathSuffix() {
    return ".enc";
}

Status EncryptionKeyManager::unprotectTmpData(
    const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen, size_t* resultLen) {
    return crypto::aesDecrypt(
        _tmpDataKey,
        crypto::getCipherModeFromString(_encryptionParams->encryptionCipherMode),
        crypto::PageSchema::k0,
        in,
        inLen,
        out,
        outLen,
        resultLen);
}

StatusWith<std::vector<std::string>> EncryptionKeyManager::beginNonBlockingBackup() try {
    if (_backupSession) {
        return {ErrorCodes::CannotBackup,
                "A backup cursor is already open on the encryption database."};
    }

    auto backupSession = _keystore->makeSession();
    auto dataStoreSession = backupSession->dataStoreSession();
    auto cursor = dataStoreSession->beginBackup();

    stdx::lock_guard<stdx::mutex> lk(_keystoreMetadataMutex);
    _keystoreMetadata.setDirty(true);
    auto status =
        _keystoreMetadata.store(_metadataPath(PathMode::kValid), _masterKey, *_encryptionParams);
    if (!status.isOK()) {
        return status;
    }

    std::vector<std::string> filesToCopy;
    filesToCopy.push_back(_metadataPath(PathMode::kValid).string());

    const auto keystorePath = boost::filesystem::path(_wtConnPath);
    for (; cursor != dataStoreSession->end(); ++cursor) {
        StringData filename = cursor.getKey<const char*>();
        const auto filePath = keystorePath / filename.toString();
        filesToCopy.push_back(filePath.string());
    }

    // Closing a WT session closes all of its corresponding cursors. Instead of explicitly
    // managing this cursor's lifetime 1-1 with the session, let it be closed when the session
    // closes. Additionally, this cursor must not be closed unless this method returns an error,
    // or until `endNonBlockingBackup` is called.
    cursor.release();
    _backupSession = std::move(backupSession);
    return filesToCopy;
} catch (const DBException& e) {
    return e.toStatus();
}

Status EncryptionKeyManager::endNonBlockingBackup() {
    // The API dictates `EncryptionHooks::endNonBlockingBackup` must allow calls regardless of
    // whether it was preceded by a successful `EncryptionHooks::beginNonBlockingBackup` call.
    if (_backupSession) {
        _backupSession.reset();
    }

    stdx::lock_guard<stdx::mutex> lk(_keystoreMetadataMutex);
    _keystoreMetadata.setDirty(false);
    auto status =
        _keystoreMetadata.store(_metadataPath(PathMode::kValid), _masterKey, *_encryptionParams);
    if (!status.isOK()) {
        return status;
    }

    return Status::OK();
}

// The local key store database and the corresponding keys are created lazily as they are requested,
// but are persisted to disk. All keys except the master key is stored in the local key store. The
// master key is acquired from the configured external source.
//
// The only current caller of getKey is WT_ENCRYPTOR::customize with the keyId provided in the
// callback.
StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::getKey(
    const SymmetricKeyId& keyId) {
    if (keyId.name() == kSystemKeyId) {
        return _getSystemKey(keyId);
    } else if (keyId.name() == kMasterKeyId) {
        return _getMasterKey();
    }
    return _readKey(keyId);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getSystemKey(
    const SymmetricKeyId& keyId) {
    if (_keystore) {
        return _readKey(keyId);
    }

    Status status = _initLocalKeystore();
    if (!status.isOK()) {
        return status;
    }

    return _readKey(keyId);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getMasterKey() {
    // WT will ask for the .master key twice when rotating keys since there are two databases.
    // The first request will be for the original master key and the second request for the new
    // master key.
    if (!_masterKeyRequested) {
        // Make a single copy of the master key for the storage engine layer.
        _masterKeyRequested = true;
        auto symmetricKey =
            stdx::make_unique<SymmetricKey>(reinterpret_cast<const uint8_t*>(_masterKey->getKey()),
                                            _masterKey->getKeySize(),
                                            crypto::aesAlgorithm,
                                            _masterKey->getKeyId(),
                                            _masterKey->getInitializationCount());
        return std::move(symmetricKey);
    }
    // Check that key rotation is enabled
    else if (_rotMasterKey) {
        return std::move(_rotMasterKey);
    }
    MONGO_UNREACHABLE;
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_readKey(
    const SymmetricKeyId& keyId) try {
    // Look for the requested key in the local key store. Assume that the local key store has been
    // created and that open_session and open_cursor will succeed.
    auto session = _keystore->makeSession();

    auto cursor = session->find(keyId);
    if (cursor != session->end()) {
        // NIST guidelines in SP-800-38D allow us to deterministically construct GCM IVs. We use
        // this construction for database writes, because it allows us a large number of invocations
        // of the Authenticated Encryption Function. However, we must take steps to ensure that we
        // never ever reuse an IV.
        //
        // The IV is divided into two fields, a 32 bit "fixed field" representing the context the
        // function operates inside of, and a 64 bit "invocation field". Every time the AEF is
        // called, the invocation field must be incremented. On every server startup, the fixed
        // field must be incremented and written back to the keystore, to indicate the creation of
        // a new context.
        //
        // When the two fields are concatenated together, we have a unique value.
        // This scheme ensures that we only need to perform one disk write on startup to ensure this
        // property.

        // Keep the server from starting if all possible fixed fields have been used, rather than
        // disobey NIST's guidelines.
        if (cursor->incrementAndGetInitializationCount() >=
            std::numeric_limits<uint32_t>::max() - 10) {
            return Status(
                ErrorCodes::Overflow,
                "Unable to allocate an IV prefix, as the server has been restarted 2^32 times.");
        }

        auto symmetricKey = std::move(*cursor);

        if (!_encryptionParams->readOnlyMode) {
            session->update(std::move(cursor), symmetricKey);
        }

        return std::move(symmetricKey);
    }

    // There is no key corresponding to keyId yet so create one
    auto symmetricKey =
        stdx::make_unique<SymmetricKey>(crypto::aesGenerate(crypto::sym256KeySize, keyId.name()));

    session->insert(symmetricKey);

    return std::move(symmetricKey);
} catch (const DBException& e) {
    return e.toStatus();
}

boost::filesystem::path EncryptionKeyManager::_metadataPath(PathMode mode) {
    switch (mode) {
        case PathMode::kValid:
            return _keystoreBasePath / kKeystoreMetadataFilename;
        case PathMode::kInvalid: {
            std::string filename = str::stream() << kKeystoreMetadataFilename << kInvalidatedKeyword
                                                 << terseCurrentTime(false);
            return _keystoreBasePath / filename;
        }
        case PathMode::kInitializing:
            return _keystoreBasePath / "keystore.metadata-initializing";
    }

    // We have covered all the enum values above. This just makes that explicit to the compiler.
    MONGO_UNREACHABLE;
}

Status EncryptionKeyManager::_initLocalKeystore() {
    // Create the local key store directory, key.store/masterKeyId with the special case
    // masterKeyId = local for local key files.
    Status status = createDirectoryIfNeeded(_keystoreBasePath);
    if (!status.isOK()) {
        return status;
    }

    std::string existingKeyId;
    // Use the first key store we find that has not been invalidated.
    for (fs::directory_iterator it(_keystoreBasePath);
         it != boost::filesystem::directory_iterator();
         ++it) {
        std::string fileName = fs::path(*it).filename().string();
        if (fileName.find(kInvalidatedKeyword) == std::string::npos &&
            fileName.find(kInitializingKeyword) == std::string::npos &&
            fileName.find(kKeystoreMetadataFilename) == std::string::npos) {
            existingKeyId = fileName;
            break;
        }
    }

    int defaultKeystoreSchemaVersion =
        _encryptionParams->encryptionCipherMode == crypto::aes256GCMName ? 1 : 0;
    if (existingKeyId.empty()) {
        // Initializing the key store for the first time.
        if (hasExistingDatafiles(_dbPath)) {
            return Status(ErrorCodes::BadValue,
                          "There are existing data files, but no valid keystore could be located.");
        }
    } else {
        // The key store exists
        if (!_encryptionParams->encryptionKeyFile.empty() && existingKeyId != "local") {
            // Warn if the the server has been started with a KMIP key before.
            warning() << "It looks like the data files were previously encrypted using an "
                      << "external key with id " << existingKeyId
                      << ". Attempting to use the provided key file.";
        }
        _keyRotationAllowed = true;
        defaultKeystoreSchemaVersion = 0;
    }

    if (!existingKeyId.empty() && !_encryptionParams->kmipParams.kmipKeyIdentifier.empty() &&
        existingKeyId != _encryptionParams->kmipParams.kmipKeyIdentifier) {
        return Status(
            ErrorCodes::BadValue,
            str::stream() << "The KMIP key id " << _encryptionParams->kmipParams.kmipKeyIdentifier
                          << " was provided, but the system is already configured with key id "
                          << existingKeyId
                          << ".");
    }

    StatusWith<std::unique_ptr<SymmetricKey>> swMasterKey =
        Status(ErrorCodes::InternalError, "Failed to find a valid source of keys");
    if (!_encryptionParams->encryptionKeyFile.empty()) {
        swMasterKey = getKeyFromKeyFile(_encryptionParams->encryptionKeyFile);
    } else {
        std::string kmipKeyId{};

        if (!existingKeyId.empty()) {
            kmipKeyId = existingKeyId;
        } else if (!_encryptionParams->kmipParams.kmipKeyIdentifier.empty()) {
            kmipKeyId = _encryptionParams->kmipParams.kmipKeyIdentifier;
        }

        // Otherwise, keyId must be empty to request KMIP generate a new key
        swMasterKey = getKeyFromKMIPServer(_encryptionParams->kmipParams, *_sslParams, kmipKeyId);
    }

    if (!swMasterKey.isOK()) {
        return swMasterKey.getStatus();
    }
    _masterKey = std::move(swMasterKey.getValue());
    fs::path keystorePath = _keystoreBasePath / _masterKey->getKeyId().name();

    auto swMetadata =
        KeystoreMetadataFile::load(_metadataPath(PathMode::kValid), _masterKey, *_encryptionParams);
    if (!swMetadata.isOK()) {
        if (swMetadata.getStatus() == ErrorCodes::NonExistentPath) {
            KeystoreMetadataFile newMetadata(defaultKeystoreSchemaVersion);
            auto storeStatus =
                newMetadata.store(_metadataPath(PathMode::kValid), _masterKey, *_encryptionParams);
            if (!storeStatus.isOK()) {
                return storeStatus.withContext("Error creating metadata file");
            }
            swMetadata = std::move(newMetadata);
        } else {
            return swMetadata.getStatus();
        }
    }

    stdx::lock_guard<stdx::mutex> lk(_keystoreMetadataMutex);
    _keystoreMetadata = std::move(swMetadata.getValue());

    // Open the local WT key store, create it if it doesn't exist.
    try {
        _keystore = Keystore::makeKeystore(keystorePath, _keystoreMetadata, _encryptionParams);
        if (_keystoreMetadata.getDirty() || _encryptionParams->rotateDatabaseKeys) {
            if (_keystoreMetadata.getDirty()) {
                log() << "Detected an unclean shutdown of the encrypted storage engine - "
                      << "rolling over all database keys.";
            } else {
                log() << "Database key rollover for encrypted storage engine was requested.";
            }
            _keystore->rollOverKeys();
            _keystoreMetadata.setDirty(false);
            // Make sure that we can get the new system key before persisting the metadata file
            // so that we know the rollover was successful.
            uassertStatusOK(_getSystemKey(kSystemKeyId));
            uassertStatusOK(_keystoreMetadata.store(
                _metadataPath(PathMode::kValid), _masterKey, *_encryptionParams));
        }
    } catch (const DBException& e) {
        _keystore.reset();
        return e.toStatus();
    }
    return Status::OK();
}

Status EncryptionKeyManager::_rotateMasterKey(const std::string& newKeyId) try {
    if (!_keyRotationAllowed) {
        return Status(ErrorCodes::BadValue,
                      "It is not possible to rotate the master key on a system that is being "
                      "started for the first time.");
    }

    const auto oldMasterKeyId = _masterKey->getKeyId();
    if (oldMasterKeyId.name() == newKeyId) {
        return Status(
            ErrorCodes::BadValue,
            "Key rotation requires that the new master key id be different from the old.");
    }

    auto swRotMasterKey =
        getKeyFromKMIPServer(_encryptionParams->kmipParams, *_sslParams, newKeyId);
    if (!swRotMasterKey.isOK()) {
        return swRotMasterKey.getStatus();
    }
    _rotMasterKey = std::move(swRotMasterKey.getValue());

    stdx::lock_guard<stdx::mutex> lk(_keystoreMetadataMutex);
    auto status = _keystoreMetadata.store(
        _metadataPath(PathMode::kInitializing), _rotMasterKey, *_encryptionParams);
    if (!status.isOK()) {
        return status;
    }

    SymmetricKeyId rotMasterKeyId = _rotMasterKey->getKeyId();
    fs::path rotKeystorePath = _keystoreBasePath / rotMasterKeyId.name();
    fs::path initRotKeystorePath = rotKeystorePath;
    initRotKeystorePath += "-initializing-keystore";

    auto rotKeyStore =
        Keystore::makeKeystore(initRotKeystorePath, _keystoreMetadata, _encryptionParams);
    auto writeSession = rotKeyStore->makeSession();

    auto keystore = std::move(_keystore);
    auto readSession = keystore->makeSession();
    for (auto&& key : *readSession) {
        writeSession->insert(key);
    }

    readSession.reset();
    writeSession.reset();

    keystore.reset();
    rotKeyStore.reset();

    // Remove the -initializing post fix to the from the new keystore.
    // Rename the old keystore path to keyid-invalidated-date.
    fs::path oldKeystorePath = _keystoreBasePath / oldMasterKeyId.name();
    fs::path invalidatedKeystorePath = oldKeystorePath;
    invalidatedKeystorePath += kInvalidatedKeyword + terseCurrentTime(false);
    try {
        // If the first of these renames succeed and the second fails, we will end up in a state
        // where the server is without a valid keystore and cannot start. This can be resolved by
        // manually renaming either the old or the newly created keystore to 'keyid'.
        fs::rename(oldKeystorePath, invalidatedKeystorePath);
        fs::rename(_metadataPath(PathMode::kValid), _metadataPath(PathMode::kInvalid));
        fs::rename(initRotKeystorePath, rotKeystorePath);
        fs::rename(_metadataPath(PathMode::kInitializing), _metadataPath(PathMode::kValid));

        // Delete old invalidated keystores to clean up. It used to be that the key store last
        // rotated was kept around for backup purposes. This behavior is no longer retained.
        for (fs::directory_iterator it(_keystoreBasePath);
             it != boost::filesystem::directory_iterator();
             ++it) {
            std::string fileName = fs::path(*it).filename().string();
            std::string filePath = fs::path(*it).string();
            if (fileName.find(kInvalidatedKeyword) != std::string::npos &&
                filePath != rotKeystorePath) {
                if (fs::remove_all(filePath) > 0) {
                    LOG(1) << "Removing old keystore " << fileName << ".";
                } else {
                    warning() << "Failed to remove the invalidated keystore " << filePath << ".";
                }
            }
        }
    } catch (const std::exception& e) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Error occured when performing key "
                                       "rotation directory rename operation. "
                                       "Verify the contents of your key "
                                       "store. "
                                    << e.what());
    }

    log() << "Rotated master encryption key from id " << oldMasterKeyId << " to id "
          << rotMasterKeyId << ".";

    return Status::OK();
} catch (const DBException& e) {
    return e.toStatus();
}

std::int32_t EncryptionKeyManager::getKeystoreVersion() const {
    stdx::lock_guard<stdx::mutex> lk(_keystoreMetadataMutex);
    return _keystoreMetadata.getVersion();
}

void initializeEncryptionKeyManager(ServiceContext* service) {
    auto keyManager = stdx::make_unique<EncryptionKeyManager>(
        storageGlobalParams.dbpath, &encryptionGlobalParams, &sslGlobalParams);
    EncryptionHooks::set(service, std::move(keyManager));
}

EncryptionWiredTigerCustomizationHooks::EncryptionWiredTigerCustomizationHooks(
    EncryptionGlobalParams* encryptionParams)
    : _encryptionParams(encryptionParams) {}

EncryptionWiredTigerCustomizationHooks::~EncryptionWiredTigerCustomizationHooks() {}

std::string EncryptionWiredTigerCustomizationHooks::getTableCreateConfig(StringData ns) {
    // Internal metadata WT tables such as sizeStorer and _mdb_catalog are identified by not having
    // a '.' separated name that distinguishes a "normal namespace during collection or index
    // creation.
    //
    // Canonicalize the tables names into a conflict free set of "database ids" by prefixing special
    // WT table names with '.' and use ".system" for the internal WT tables.
    std::size_t dotIndex = ns.find(".");
    std::string keyId;
    if (dotIndex == std::string::npos) {
        keyId = kSystemKeyId;
    } else {
        keyId = ns.substr(0, dotIndex).toString();
    }

    return "encryption=(name=" + _encryptionParams->encryptionCipherMode + ",keyid=\"" + keyId +
        "\"),";
}
}  // namespace mongo
