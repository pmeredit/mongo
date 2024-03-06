/**
 * Copyright (C) 2015 MongoDB Inc.
 * The main KMIP API that is exposed to the rest of the codebase.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "encryption_key_manager.h"

#include <boost/filesystem.hpp>
#include <fstream>

#include "encryption_options.h"
#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/auth/security_file.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_customization_hooks.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/time_support.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace {

const std::string kKeystoreName = "key.store";
const std::string kInvalidatedKeyword = "-invalidated-";
const std::string kKeystoreTableName = "table:keystore";
const std::string kEncryptionEntrypointConfig =
    "extensions=[local=(entry=mongo_addWiredTigerEncryptors)],";

void closeWTCursorAndSession(WT_CURSOR* cursor) {
    WT_SESSION* session = cursor->session;
    invariantWTOK(cursor->close(cursor));
    invariantWTOK(session->close(session, nullptr));
}

int keystore_handle_error(WT_EVENT_HANDLER* handler,
                          WT_SESSION* session,
                          int errorCode,
                          const char* message) {
    try {
        error() << "WiredTiger keystore (" << errorCode << ") " << message;
        fassert(4051, errorCode != WT_PANIC);
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int keystore_handle_message(WT_EVENT_HANDLER* handler, WT_SESSION* session, const char* message) {
    try {
        log() << "WiredTiger keystore " << message;
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int keystore_handle_progress(WT_EVENT_HANDLER* handler,
                             WT_SESSION* session,
                             const char* operation,
                             uint64_t progress) {
    try {
        log() << "WiredTiger keystore progress " << operation << " " << progress;
    } catch (...) {
        std::terminate();
    }
    return 0;
}

WT_EVENT_HANDLER keystoreEventHandlers() {
    WT_EVENT_HANDLER handlers = {};
    handlers.handle_error = keystore_handle_error;
    handlers.handle_message = keystore_handle_message;
    handlers.handle_progress = keystore_handle_progress;
    return handlers;
}

namespace fs = boost::filesystem;

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

}  // namespace

EncryptionKeyManager::EncryptionKeyManager(const std::string& dbPath,
                                           EncryptionGlobalParams* encryptionParams,
                                           SSLParams* sslParams)
    : _dbPath(fs::path(dbPath)),
      _keystoreBasePath(fs::path(dbPath) / "key.store"),
      _masterKeyId(""),
      _keyRotationAllowed(false),
      _keystoreConnection(nullptr),
      _keystoreEventHandler(keystoreEventHandlers()),
      _encryptionParams(encryptionParams),
      _sslParams(sslParams) {}

EncryptionKeyManager::~EncryptionKeyManager() {
    if (_keystoreConnection != nullptr) {
        _keystoreConnection->close(_keystoreConnection, nullptr);
    }
}

EncryptionKeyManager* EncryptionKeyManager::get(ServiceContext* service) {
    EncryptionKeyManager* globalKeyManager =
        checked_cast<EncryptionKeyManager*>(WiredTigerCustomizationHooks::get(service));
    fassert(4036, globalKeyManager != nullptr);
    return globalKeyManager;
}

bool EncryptionKeyManager::restartRequired() {
    // It is sufficient to check for 'mmapv1' since it's the only non-wiredWiger storage engine
    // we detect dynamically for backwards compatibility.
    if (getGlobalServiceContext()->getGlobalStorageEngine()->isMmapV1()) {
        log() << "Encryption at rest requires the 'wiredTiger' storage engine, aborting.";
        return true;
    }

    if (_encryptionParams->rotateMasterKey) {
        Status status = _rotateMasterKey(_encryptionParams->kmipKeyIdentifierRot);
        if (!status.isOK()) {
            severe() << "Failed to rotate master key: " << status.reason();
        }
        // The server should always exit after a key rotation.
        return true;
    }

    if (_encryptionParams->encryptionKeyFile.empty()) {
        log() << "Encryption key manager initialized using KMIP key with id: " << _masterKeyId
              << ".";
    } else {
        log() << "Encryption key manager initialized with key file: "
              << _encryptionParams->encryptionKeyFile;
    }

    return false;
}

std::string EncryptionKeyManager::getOpenConfig(StringData ns) {
    std::string config;

    // The keyId system implies that the call is to configure wiredtiger_open for the regular data
    // store.
    if (ns == "system") {
        config += kEncryptionEntrypointConfig;
    }

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

    config += "encryption=(name=" + _encryptionParams->encryptionCipherMode + ",keyid=\"" + keyId +
        "\"),";

    return config;
}

// The local key store database and the corresponding keys are created lazily as they are requested,
// but are persisted to disk. All keys except the master key is stored in the local key store. The
// master key is acquired from the configured external source.
//
// The only current caller of getKey is WT_ENCRYPTOR::customize with the keyId provided in the
// callback.
StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::getKey(const std::string& keyId) {
    if (keyId == kSystemKeyId) {
        return _getSystemKey();
    } else if (keyId == kMasterKeyId) {
        return _getMasterKey();
    }
    return _readKey(keyId);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getSystemKey() {
    Status status = _initLocalKeystore();
    if (!status.isOK()) {
        return status;
    }

    return _readKey(kSystemKeyId);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getMasterKey() {
    // WT will ask for the .master key twice when rotating keys since there are two databases.
    // The first request will be for the original master key and the second request for the new
    // master key.
    if (_masterKey) {
        return std::move(_masterKey);
    }
    // Check that key rotation is enabled
    else if (_rotMasterKey) {
        return std::move(_rotMasterKey);
    }
    MONGO_UNREACHABLE;
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_readKey(const std::string& keyId) {
    // Look for the requested key in the local key store. Assume that the local key store has been
    // created and that open_session and open_cursor will succeed.
    WT_SESSION* session;
    invariantWTOK(
        _keystoreConnection->open_session(_keystoreConnection, nullptr, nullptr, &session));

    WT_CURSOR* cursor;
    invariantWTOK(
        session->open_cursor(session, kKeystoreTableName.c_str(), nullptr, nullptr, &cursor));

    ON_BLOCK_EXIT(closeWTCursorAndSession, cursor);

    WT_ITEM key;
    cursor->set_key(cursor, keyId.c_str());
    int ret = cursor->search(cursor);
    if (ret == 0) {
        // The key already exists so return it.
        uint32_t keyStatus, initializationCount;
        invariantWTOK(cursor->get_value(cursor, &key, &keyStatus, &initializationCount));
        auto symmetricKey =
            stdx::make_unique<SymmetricKey>(reinterpret_cast<const uint8_t*>(key.data),
                                            key.size,
                                            crypto::aesAlgorithm,
                                            keyId,
                                            ++initializationCount);
        cursor->set_value(cursor, &key, keyStatus, initializationCount);
        invariantWTOK(cursor->update(cursor));
        return std::move(symmetricKey);
    }

    // There is no key corresponding to keyId yet so create one
    auto symmetricKey =
        stdx::make_unique<SymmetricKey>(crypto::aesGenerate(crypto::sym256KeySize, keyId));
    key.data = symmetricKey.get()->getKey();
    key.size = symmetricKey.get()->getKeySize();
    cursor->set_value(cursor, &key, 0, symmetricKey.get()->getInitializationCount());
    invariantWTOK(cursor->insert(cursor));

    return std::move(symmetricKey);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getKeyFromKMIPServer(
    const std::string& keyId) {
    if (_encryptionParams->kmipServerName.empty()) {
        return Status(ErrorCodes::BadValue,
                      "Encryption at rest enabled but no key server specified");
    }

    SSLParams sslKMIPParams;

    // KMIP specific parameters.
    sslKMIPParams.sslPEMKeyFile = _encryptionParams->kmipClientCertificateFile;
    sslKMIPParams.sslPEMKeyPassword = _encryptionParams->kmipClientCertificatePassword;
    sslKMIPParams.sslClusterFile = "";
    sslKMIPParams.sslClusterPassword = "";
    sslKMIPParams.sslCAFile = _encryptionParams->kmipServerCAFile;

    // Copy the rest from the global SSL manager options.
    sslKMIPParams.sslFIPSMode = _sslParams->sslFIPSMode;

    // KMIP servers never should have invalid certificates
    sslKMIPParams.sslAllowInvalidCertificates = false;
    sslKMIPParams.sslAllowInvalidHostnames = false;
    sslKMIPParams.sslCRLFile = "";

    KMIPService kmipService(
        HostAndPort(_encryptionParams->kmipServerName, _encryptionParams->kmipPort), sslKMIPParams);

    if (!kmipService.isValid()) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Failed to open connection to KMIP server "
                                    << _encryptionParams->kmipServerName << ".");
    }

    // Try to retrieve an existing key.
    if (!keyId.empty()) {
        return kmipService.getExternalKey(keyId);
    }

    // Create and retrieve new key.
    StatusWith<std::string> swCreateKey = kmipService.createExternalKey();
    if (!swCreateKey.isOK()) {
        return swCreateKey.getStatus();
    }
    std::string newKeyId = swCreateKey.getValue();
    log() << "Created KMIP key with id: " << newKeyId;
    return kmipService.getExternalKey(newKeyId);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getKeyFromKeyFile() {
    StatusWith<std::string> keyString = readSecurityFile(_encryptionParams->encryptionKeyFile);
    if (!keyString.isOK()) {
        return keyString.getStatus();
    }

    std::string decodedKey;
    try {
        decodedKey = base64::decode(keyString.getValue());
    } catch (const DBException& ex) {
        return ex.toStatus();
    }

    const size_t keyLength = decodedKey.size();
    if (keyLength != crypto::minKeySize && keyLength != crypto::maxKeySize) {
        return StatusWith<std::unique_ptr<SymmetricKey>>(
            ErrorCodes::BadValue,
            str::stream() << "Encryption key in " << _encryptionParams->encryptionKeyFile
                          << " has length " << keyLength << ", must be either "
                          << crypto::minKeySize << " or " << crypto::maxKeySize << " characters.");
    }

    std::vector<uint8_t> keyVector(decodedKey.begin(), decodedKey.end());
    const uint8_t* keyData = keyVector.data();

    return stdx::make_unique<SymmetricKey>(keyData, keyLength, crypto::aesAlgorithm, "local", 0);
}

Status EncryptionKeyManager::_openKeystore(const fs::path& path, WT_CONNECTION** conn) {
    Status status = createDirectoryIfNeeded(path);
    if (!status.isOK()) {
        return status;
    }

    // FIXME: While using GCM, needs to always use AES-GCM with RBG derived IVs
    std::string keystoreConfig = "encryption=(name=" + _encryptionParams->encryptionCipherMode +
        ",keyid=" + kMasterKeyId + "),";

    StringBuilder wtConfig;
    wtConfig << "create,";
    wtConfig << "log=(enabled,file_max=3MB),transaction_sync=(enabled=true,method=fsync),";
    wtConfig << kEncryptionEntrypointConfig;
    wtConfig << keystoreConfig;

    // _localKeystoreInitialized needs to be set before calling wiredtiger_open since that
    // call eventually will result in a call to getKey for the ".system" key at which point we don't
    // want to call _initLocalKeystore recursively.
    int ret = wiredtiger_open(
        path.string().c_str(), &_keystoreEventHandler, wtConfig.str().c_str(), conn);
    if (ret != 0) {
        return wtRCToStatus(ret);
    }

    WT_SESSION* session;
    invariantWTOK((*conn)->open_session(*conn, nullptr, nullptr, &session));

    // Check if the key store table:encryptionkeys already exists.
    WT_CURSOR* cursor;
    if ((ret = session->open_cursor(
             session, kKeystoreTableName.c_str(), nullptr, nullptr, &cursor)) == 0) {
        closeWTCursorAndSession(cursor);
        return Status::OK();
    }

    // Create a new WT key storage with the following schema:
    //
    // S: database name, null-terminated string.
    // u: key material, raw byte array stored as a WT_ITEM.
    // l: key status, currently not used but set to 0.
    std::string sessionConfig = keystoreConfig +
        "key_format=S,value_format=uLL,columns=(keyid,key,keystatus,initializationCount)";

    invariantWTOK(session->create(session, kKeystoreTableName.c_str(), sessionConfig.c_str()));
    invariantWTOK(session->checkpoint(session, nullptr));
    invariantWTOK(session->close(session, nullptr));

    return Status::OK();
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
            fileName.find("-initializing") == std::string::npos) {
            existingKeyId = fileName;
            break;
        }
    }

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
    }

    StatusWith<std::unique_ptr<SymmetricKey>> swMasterKey =
        Status(ErrorCodes::InternalError, "Master key never acquired");
    if (!_encryptionParams->encryptionKeyFile.empty()) {
        // Retrieve the master key from a key file.
        swMasterKey = _getKeyFromKeyFile();
    } else {
        // Retrieve the master key from a KMIP server.
        if (!existingKeyId.empty() && !_encryptionParams->kmipKeyIdentifier.empty() &&
            existingKeyId != _encryptionParams->kmipKeyIdentifier) {
            return Status(ErrorCodes::BadValue,
                          str::stream()
                              << "The KMIP key id " << _encryptionParams->kmipKeyIdentifier
                              << " was provided, but the system is already configured with key id "
                              << existingKeyId << ".");
        }

        if (!existingKeyId.empty()) {
            // Use the existing key id derived from the key store name.
            swMasterKey = _getKeyFromKMIPServer(existingKeyId);
        } else {
            // Create a new key or use a provided key id.
            swMasterKey = _getKeyFromKMIPServer(_encryptionParams->kmipKeyIdentifier);
        }
    }

    if (!swMasterKey.isOK()) {
        return swMasterKey.getStatus();
    }
    _masterKey = std::move(swMasterKey.getValue());
    _masterKeyId = _masterKey->getKeyId();
    fs::path keystorePath = _keystoreBasePath / _masterKeyId;

    // Open the local WT key store, create it if it doesn't exist.
    return _openKeystore(keystorePath, &_keystoreConnection);
}

Status EncryptionKeyManager::_rotateMasterKey(const std::string& newKeyId) {
    if (!_keyRotationAllowed) {
        return Status(ErrorCodes::BadValue,
                      "It is not possible to rotate the master key on a system that is being "
                      "started for the first time.");
    }
    if (_masterKeyId == newKeyId) {
        return Status(
            ErrorCodes::BadValue,
            "Key rotation requires that the new master key id be different from the old.");
    }

    auto swRotMasterKey = _getKeyFromKMIPServer(newKeyId);
    if (!swRotMasterKey.isOK()) {
        return swRotMasterKey.getStatus();
    }
    _rotMasterKey = std::move(swRotMasterKey.getValue());

    WT_CONNECTION* rotKeystoreConnection;
    std::string rotMasterKeyId = _rotMasterKey->getKeyId();
    fs::path rotKeystorePath = _keystoreBasePath / rotMasterKeyId;
    fs::path initRotKeystorePath = rotKeystorePath;
    initRotKeystorePath += "-initializing-keystore";

    Status status = _openKeystore(initRotKeystorePath, &rotKeystoreConnection);
    if (!status.isOK()) {
        return status;
    }

    // Open sessions and cursor for the old and new key store.
    WT_SESSION* readSession;
    WT_CURSOR* readCursor;
    invariantWTOK(
        _keystoreConnection->open_session(_keystoreConnection, nullptr, nullptr, &readSession));
    invariantWTOK(readSession->open_cursor(
        readSession, kKeystoreTableName.c_str(), nullptr, nullptr, &readCursor));

    WT_SESSION* writeSession;
    WT_CURSOR* writeCursor;
    invariantWTOK(rotKeystoreConnection->open_session(
        rotKeystoreConnection, nullptr, nullptr, &writeSession));
    invariantWTOK(writeSession->open_cursor(
        writeSession, kKeystoreTableName.c_str(), nullptr, nullptr, &writeCursor));

    // Read all the keys from the original key store and write them to the new key store.
    char* keyId;
    WT_ITEM key;
    uint32_t keyStatus, initializationCount;
    while (readCursor->next(readCursor) == 0) {
        invariantWTOK(readCursor->get_key(readCursor, &keyId));
        invariantWTOK(readCursor->get_value(readCursor, &key, &keyStatus, &initializationCount));
        writeCursor->set_key(writeCursor, keyId);
        writeCursor->set_value(writeCursor, &key, keyStatus, initializationCount);
        invariantWTOK(writeCursor->insert(writeCursor));
    }

    closeWTCursorAndSession(readCursor);
    invariantWTOK(_keystoreConnection->close(_keystoreConnection, nullptr));

    closeWTCursorAndSession(writeCursor);
    invariantWTOK(rotKeystoreConnection->close(rotKeystoreConnection, nullptr));

    // Remove the -initializing post fix to the from the new keystore.
    // Rename the old keystore path to keyid-invalidated-date.
    fs::path oldKeystorePath = _keystoreBasePath / _masterKeyId;
    fs::path invalidatedKeystorePath = oldKeystorePath;
    invalidatedKeystorePath += kInvalidatedKeyword + Date_t::now().toString();
    try {
        // If the first of these renames succeed and the second fails, we will end up in a state
        // where the server is without a valid keystore and cannot start. This can be resolved by
        // manually renaming either the old or the newly created keystore to 'keyid'.
        fs::rename(oldKeystorePath, invalidatedKeystorePath);
        fs::rename(initRotKeystorePath, rotKeystorePath);

        // Delete any old invalidated keystores to clean up.
        for (fs::directory_iterator it(_keystoreBasePath);
             it != boost::filesystem::directory_iterator();
             ++it) {
            std::string fileName = fs::path(*it).filename().string();
            std::string filePath = fs::path(*it).string();
            if (fileName.find(kInvalidatedKeyword) != std::string::npos &&
                filePath != invalidatedKeystorePath) {
                if (fs::remove_all(filePath) > 0) {
                    log() << "Removing outdated invalid keystore " << fileName << ".";
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
                                       "store. " << e.what());
    }

    log() << "Rotated master encryption key from id " << _masterKeyId << " to id " << rotMasterKeyId
          << ".";
    _masterKeyId = rotMasterKeyId;

    return Status::OK();
}

}  // namespace mongo
