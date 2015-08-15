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
#include "symmetric_crypto.h"

namespace mongo {
namespace {

const std::string kMetadataBasename = "storage.bson";
const std::string kKeyStorageTableName = "table:keystore";
const std::string kEncryptionEntrypointConfig =
    "extensions=[local=(entry=mongo_addWiredTigerEncryptors)],";
const std::string kSystemKeyId = ".system";
const std::string kMasterKeyId = ".master";

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
}  // namespace

EncryptionKeyManager::EncryptionKeyManager(const std::string& dbPath,
                                           EncryptionGlobalParams* encryptionParams,
                                           SSLParams* sslParams)
    : _dbPath(dbPath),
      _localKeyStoreInitialized(false),
      _keyStorageConnection(nullptr),
      _keyStorageEventHandler(keystoreEventHandlers()),
      _encryptionParams(encryptionParams),
      _sslParams(sslParams) {}

EncryptionKeyManager::~EncryptionKeyManager() {
    if (_keyStorageConnection != nullptr) {
        _keyStorageConnection->close(_keyStorageConnection, nullptr);
    }
}

EncryptionKeyManager* EncryptionKeyManager::get(ServiceContext* service) {
    EncryptionKeyManager* globalKeyManager =
        checked_cast<EncryptionKeyManager*>(WiredTigerCustomizationHooks::get(service));
    fassert(4036, globalKeyManager != nullptr);
    return globalKeyManager;
}

void EncryptionKeyManager::appendUID(BSONObjBuilder* builder) {
    BSONObjBuilder sub(builder->subobjStart("encryption"));
    sub.append("keyUID", _kmipMasterKeyId);
    sub.done();
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
    if (!_localKeyStoreInitialized) {
        Status status = _initLocalKeyStore();
        if (!status.isOK()) {
            return status;
        }
    }
    if (keyId == kMasterKeyId) {
        return _acquireMasterKey();
    }

    // Look for the requested key in the local key store. Assume that the local key store has been
    // created and that open_session and open_cursor will succeed.
    WT_SESSION* session;
    invariantWTOK(
        _keyStorageConnection->open_session(_keyStorageConnection, nullptr, nullptr, &session));

    WT_CURSOR* cursor;
    invariantWTOK(
        session->open_cursor(session, kKeyStorageTableName.c_str(), nullptr, nullptr, &cursor));

    ON_BLOCK_EXIT(closeWTCursorAndSession, cursor);

    WT_ITEM key;
    cursor->set_key(cursor, keyId.c_str());
    int ret = cursor->search(cursor);
    if (ret == 0) {
        // The key already exists so return it.
        uint32_t keyStatus;
        invariantWTOK(cursor->get_value(cursor, &key, &keyStatus));
        return stdx::make_unique<SymmetricKey>(
            reinterpret_cast<const uint8_t*>(key.data), key.size, crypto::aesAlgorithm);
    }

    // There is no key corresponding to keyId yet so create one
    auto symmetricKey = stdx::make_unique<SymmetricKey>(crypto::aesGenerate(crypto::sym256KeySize));
    key.data = symmetricKey.get()->getKey();
    key.size = symmetricKey.get()->getKeySize();
    cursor->set_value(cursor, &key, 0);
    invariantWTOK(cursor->insert(cursor));

    return std::move(symmetricKey);
}

StatusWith<std::string> EncryptionKeyManager::_getKeyUIDFromMetadata() {
    boost::filesystem::path metadataPath = boost::filesystem::path(_dbPath) / kMetadataBasename;
    if (!boost::filesystem::exists(metadataPath)) {
        return Status(ErrorCodes::PathNotViable,
                      "Storage engine metadata file has not been created yet");
    }

    auto fileSize = boost::filesystem::file_size(metadataPath);
    uassert(
        4038, "metadata filesize > maximum BSON object size", fileSize <= BSONObjMaxInternalSize);

    std::vector<char> buffer(fileSize);
    std::string filename = metadataPath.string();
    std::ifstream ifs(filename.c_str(), std::ios_base::in | std::ios_base::binary);
    uassert(4039, "failed to read metadata from file", ifs);
    ifs.read(&buffer[0], buffer.size());
    uassert(4040, "failed to read metadata from file", ifs);

    BSONObj obj = BSONObj(buffer.data());
    uassert(4041,
            "read invalid BSON from metadata file",
            validateBSON(obj.objdata(), obj.objsize()).isOK());
    BSONElement encryptionKeyElement = obj.getFieldDotted("storage.options.encryption.keyUID");

    if (encryptionKeyElement.eoo()) {
        return Status(ErrorCodes::NoMatchingDocument,
                      "No external encryption key identifier found in metadata");
    }

    uassert(4042,
            "metadata encryption key element must be a string",
            encryptionKeyElement.type() == mongo::String);
    return encryptionKeyElement.String();
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_getKeyFromKMIPServer() {
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

    // Try to retrieve an existing key.
    if (!_kmipMasterKeyId.empty()) {
        return kmipService.getExternalKey(_kmipMasterKeyId);
    }

    // Create and retrieve new key.
    StatusWith<std::string> swCreateKey = kmipService.createExternalKey();
    if (!swCreateKey.isOK()) {
        return swCreateKey.getStatus();
    }
    _kmipMasterKeyId = swCreateKey.getValue();
    log() << "Created KMIP key with id: " << _kmipMasterKeyId;
    return kmipService.getExternalKey(_kmipMasterKeyId);
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

    return stdx::make_unique<SymmetricKey>(keyData, keyLength, crypto::aesAlgorithm);
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::_acquireMasterKey() {
    // Get key id from metadata. If storage.bson exists and does not
    // contain a key id element return error.
    auto swStoredKeyId = _getKeyUIDFromMetadata();
    std::string storedKeyId;
    if (!swStoredKeyId.isOK()) {
        if (swStoredKeyId.getStatus().code() == ErrorCodes::NoMatchingDocument) {
            return Status(ErrorCodes::BadValue,
                          "The system has previously been started without encryption enabled.");
        }
    } else {
        storedKeyId = swStoredKeyId.getValue();
    }

    if (!_encryptionParams->encryptionKeyFile.empty()) {
        if (!storedKeyId.empty()) {
            // Warn if the the server has been started with a KMIP key before.
            log() << "It looks like the data files were previously encrypted using an "
                  << "external key with id " << storedKeyId
                  << ". Attempting to use the provided key file.";
        }
        StatusWith<std::unique_ptr<SymmetricKey>> keyFileSystemKey = _getKeyFromKeyFile();
        if (!keyFileSystemKey.isOK()) {
            return keyFileSystemKey.getStatus();
        }

        log() << "Encryption key manager initialized with key file: "
              << _encryptionParams->encryptionKeyFile;
        return std::move(keyFileSystemKey.getValue());
    }

    if (swStoredKeyId.isOK()) {
        // Warn if the the server has not been started with a key file before.
        if (storedKeyId.empty()) {
            log() << "It looks like the data files were previously encrypted using a key file."
                  << " Attempting to use the provided KMIP key.";
        } else if (!_encryptionParams->kmipKeyIdentifier.empty() &&
                   storedKeyId != _encryptionParams->kmipKeyIdentifier) {
            // Return error if a new KMIP key identifier was provided that's different from the
            // stored one.
            return Status(ErrorCodes::BadValue,
                          str::stream()
                              << "The KMIP key id " << _encryptionParams->kmipKeyIdentifier
                              << " was provided, but the system is already configured with key id "
                              << storedKeyId << ".");
        }
    }

    // The _kmipMasterKeyId will be written to the metadata by the general WT metadata management.
    if (storedKeyId.empty()) {
        _kmipMasterKeyId = _encryptionParams->kmipKeyIdentifier;
    } else {
        _kmipMasterKeyId = storedKeyId;
    }

    // Retrieve the key from the KMIP server. The key is created if it does not already exist.
    StatusWith<std::unique_ptr<SymmetricKey>> swMasterKey = _getKeyFromKMIPServer();
    if (!swMasterKey.isOK()) {
        return swMasterKey.getStatus();
    }

    log() << "Encryption key manager initialized using KMIP key with id: " << _kmipMasterKeyId;
    return std::move(swMasterKey.getValue());
}

Status EncryptionKeyManager::_initLocalKeyStore() {
    // Create the local key store directory, named .local for key file master keys and
    // '.' || masterKeyId for KMIP keys.
    std::string keyStoragePath = _dbPath + "/";
    if (_kmipMasterKeyId.empty()) {
        keyStoragePath += ".local";
    } else {
        keyStoragePath += "." + _kmipMasterKeyId;
    }

    if (!boost::filesystem::exists(keyStoragePath)) {
        try {
            boost::filesystem::create_directory(keyStoragePath);
        } catch (const std::exception& e) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "error creating path " << keyStoragePath << ' '
                                        << e.what());
        }
    }

    // Create the local WT key store
    // FIXME: While using GCM, needs to always use AES-GCM with RBG derived IVs
    std::string keyStorageEncryptionConfig = "encryption=(name=" +
        _encryptionParams->encryptionCipherMode + ",keyid=" + kMasterKeyId + "),";
    std::string wtConfig = "create,";
    wtConfig += "log=(enabled,file_max=3MB),transaction_sync=(enabled=true,method=fsync),";
    wtConfig += kEncryptionEntrypointConfig;
    wtConfig += keyStorageEncryptionConfig;

    // The _localKeyStoreInitialized needs to be set before calling wiredtiger_open since that call
    // eventually will result in a call to getKey for the ".system" key at which point we don't want
    // to call _initLocalKeyStore recursively.
    _localKeyStoreInitialized = true;
    int ret = wiredtiger_open(
        keyStoragePath.c_str(), &_keyStorageEventHandler, wtConfig.c_str(), &_keyStorageConnection);
    if (ret != 0) {
        return wtRCToStatus(ret);
    }

    WT_SESSION* session;
    invariantWTOK(
        _keyStorageConnection->open_session(_keyStorageConnection, nullptr, nullptr, &session));

    // Check if the key store table:encryptionkeys already exists.
    WT_CURSOR* cursor;
    if ((ret = session->open_cursor(
             session, kKeyStorageTableName.c_str(), nullptr, nullptr, &cursor)) == 0) {
        closeWTCursorAndSession(cursor);
        return Status::OK();
    }

    // Create a new WT key storage with the following schema:
    //
    // S: database name, null-terminated string.
    // u: key material, raw byte array stored as a WT_ITEM.
    // l: key status, currently not used but set to 0.
    std::string sessionConfig =
        keyStorageEncryptionConfig + "key_format=S,value_format=uL,columns=(keyid,key,keystatus)";

    invariantWTOK(session->create(session, kKeyStorageTableName.c_str(), sessionConfig.c_str()));
    invariantWTOK(session->checkpoint(session, nullptr));
    invariantWTOK(session->close(session, nullptr));

    return Status::OK();
}
}  // namespace mongo
