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
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"

namespace mongo {

EncryptionKeyManager::EncryptionKeyManager(const std::string& dbPath,
                                           EncryptionGlobalParams* encryptionParams,
                                           SSLParams* sslParams)
    : _initialized(false),
      _dbPath(dbPath),
      _encryptionParams(encryptionParams),
      _sslParams(sslParams) {}

static const std::string keyIdNamespaceName = "local.system.keyids";
static const std::string kMetadataBasename = "storage.bson";

EncryptionKeyManager* EncryptionKeyManager::get(ServiceContext* service) {
    EncryptionKeyManager* globalKeyManager =
        checked_cast<EncryptionKeyManager*>(WiredTigerCustomizationHooks::get(service));
    fassert(4036, globalKeyManager != nullptr);
    return globalKeyManager;
}

void EncryptionKeyManager::appendUID(BSONObjBuilder* builder) {
    BSONObjBuilder sub(builder->subobjStart("encryption"));
    sub.append("keyUID", _systemKeyId);
    sub.done();
}

std::string EncryptionKeyManager::getOpenConfig(StringData tableName) {
    std::string config;

    // Metadata implies that the call is to configure wiredtiger_open.
    if (tableName == "metadata") {
        config += "extensions=[local=(entry=mongo_addWiredTigerEncryptors)],";
    }

    /**
     * TODO: Currently using the system key for all tables.
     * It looks like we'll be able to feed the actual collection name
     * to this function so we can use it to determine the keyid straight off
     * for a multikey scenario.
     */
    config += "encryption=(name=aes,keyid=system),";
    return config;
}

StatusWith<std::unique_ptr<SymmetricKey>> EncryptionKeyManager::getKey(const std::string& keyID) {
    /**
     * TODO: This method is currently hardcoded to return the system key regardless
     * of which key id was asked for. In the multikey scenario we will retrieve the
     * correct key from the key database instead of copying the system key.
     */
    if (!_initialized) {
        Status status = _acquireSystemKey();
        if (!status.isOK()) {
            return status;
        }

        _initialized = true;
        if (!_systemKeyId.empty()) {
            log() << "Encryption key manager initialized using system key with id: "
                  << _systemKeyId;
        } else {
            log() << "Encryption key manager initialized with key file: "
                  << _encryptionParams->encryptionKeyFile;
        }
    }

    // TODO: This is an intentional hack to avoid implementing an undesired
    // copy constructor for SymmetricKey. This will be removed when the proper
    // keys are returned from this method.
    return stdx::make_unique<SymmetricKey>(_systemKey.get()->getKey(),
                                           _systemKey.get()->getKeySize(),
                                           _systemKey.get()->getAlgorithm());
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
    if (!_systemKeyId.empty()) {
        return kmipService.getExternalKey(_systemKeyId);
    }

    // Create and retrieve new key.
    StatusWith<std::string> swCreateKey = kmipService.createExternalKey();
    if (!swCreateKey.isOK()) {
        return swCreateKey.getStatus();
    }
    _systemKeyId = swCreateKey.getValue();
    log() << "Created KMIP key with id: " << _systemKeyId;
    return kmipService.getExternalKey(_systemKeyId);
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

Status EncryptionKeyManager::_acquireSystemKey() {
    // TODO: Implement support for multiple key provision mechanism.


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
        _systemKey = std::move(keyFileSystemKey.getValue());
        return Status::OK();
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

    // The _systemKeyId will be written to the metadata by the general WT metadata management.
    if (storedKeyId.empty()) {
        _systemKeyId = _encryptionParams->kmipKeyIdentifier;
    } else {
        _systemKeyId = storedKeyId;
    }

    // Retrieve the key from the KMIP server. The key is created if it does not already exist.
    StatusWith<std::unique_ptr<SymmetricKey>> swSystemKey = _getKeyFromKMIPServer();
    if (!swSystemKey.isOK()) {
        return swSystemKey.getStatus();
    }

    _systemKey = std::move(swSystemKey.getValue());
    return Status::OK();
}
}  // namespace mongo
