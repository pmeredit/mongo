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
#include "mongo/db/auth/security_key.h"
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
        log() << "Encryption key manager initialized using system key with id: " << _systemKeyId;
    }

    // TODO: This is an intentional hack to avoid implementing an undesired
    // copy constructor for SymmetricKey. This will be removed when the proper
    // keys are returned from this method.
    return stdx::make_unique<SymmetricKey>(_systemKey.get()->getKey(),
                                           _systemKey.get()->getKeySize(),
                                           _systemKey.get()->getAlgorithm());
}

std::string EncryptionKeyManager::_getKeyUIDFromMetadata() {
    boost::filesystem::path metadataPath = boost::filesystem::path(_dbPath) / kMetadataBasename;
    if (!boost::filesystem::exists(metadataPath)) {
        return "";
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

    std::string keyUID = "";

    if (!encryptionKeyElement.eoo()) {
        uassert(4042,
                "metadata encryption key element must be a string",
                encryptionKeyElement.type() == mongo::String);
        keyUID = encryptionKeyElement.String();
        uassert(4043, "metadata encryption key uuid string cannot be empty", !keyUID.empty());
    }

    return keyUID;
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

    // Copy the rest from the global SSL manager options.
    sslKMIPParams.sslCAFile = _sslParams->sslCAFile;
    sslKMIPParams.sslCRLFile = _sslParams->sslCRLFile;
    sslKMIPParams.sslAllowInvalidCertificates = _sslParams->sslAllowInvalidCertificates;
    sslKMIPParams.sslAllowInvalidHostnames = _sslParams->sslAllowInvalidHostnames;
    sslKMIPParams.sslFIPSMode = _sslParams->sslFIPSMode;

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
                          << crypto::minKeySize << " or " << crypto::maxKeySize);
    }

    std::vector<uint8_t> keyVector(decodedKey.begin(), decodedKey.end());
    const uint8_t* keyData = keyVector.data();

    return stdx::make_unique<SymmetricKey>(keyData, keyLength, crypto::aesAlgorithm);
}

Status EncryptionKeyManager::_acquireSystemKey() {
    // TODO: Implement support for multiple key provision mechanism.

    if (!_encryptionParams->encryptionKeyFile.empty()) {
        StatusWith<std::unique_ptr<SymmetricKey>> keyFileSystemKey = _getKeyFromKeyFile();
        if (!keyFileSystemKey.isOK()) {
            return keyFileSystemKey.getStatus();
        }
        _systemKey = std::move(keyFileSystemKey.getValue());
        return Status::OK();
    }

    /**
     * 1. Get key from metadata
     * 2. If we got one check that we don't have a keyIdentifier specified too
     * 3. If we don't have a key in metadata create one on the KMIP server
     * 4. Retrieve it from the KMIP server
     * 5. If a new key id was created, write it to the storage metadata
     */
    std::string storedKeyId = _getKeyUIDFromMetadata();
    if (!_encryptionParams->kmipKeyIdentifier.empty() && !storedKeyId.empty()) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "A KMIP key identifier was provided but the system is already "
                             "configured with a key with id " << storedKeyId);
    }

    if (storedKeyId.empty()) {
        _systemKeyId = _encryptionParams->kmipKeyIdentifier;
    } else {
        _systemKeyId = storedKeyId;
    }

    StatusWith<std::unique_ptr<SymmetricKey>> swSystemKey = _getKeyFromKMIPServer();
    if (!swSystemKey.isOK()) {
        return swSystemKey.getStatus();
    }

    _systemKey = std::move(swSystemKey.getValue());
    return Status::OK();
}
}  // namespace mongo
