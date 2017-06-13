/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "encryption_key_acquisition.h"

#include "encryption_options.h"
#include "kmip_service.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/security_file.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_options.h"
#include "symmetric_crypto.h"
#include "symmetric_crypto.h"
#include "symmetric_key.h"

using namespace mongo::kmip;

namespace mongo {
StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKeyFile(StringData encryptionKeyFile) {
    StatusWith<std::string> keyString = readSecurityFile(encryptionKeyFile.toString());
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
    if (keyLength != crypto::sym256KeySize) {
        return {ErrorCodes::BadValue,
                str::stream() << "Encryption key in " << encryptionKeyFile << " is "
                              << keyLength * 8
                              << " bit, must be "
                              << crypto::sym256KeySize * 8
                              << " bit."};
    }

    return stdx::make_unique<SymmetricKey>(reinterpret_cast<const uint8_t*>(decodedKey.data()),
                                           keyLength,
                                           crypto::aesAlgorithm,
                                           "local",
                                           0);
}

StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKMIPServer(const KMIPParams& kmipParams,
                                                               const SSLParams& sslParams,
                                                               StringData keyId) {
    if (kmipParams.kmipServerName.empty()) {
        return Status(ErrorCodes::BadValue,
                      "Encryption at rest enabled but no key server specified");
    }

    SSLParams sslKMIPParams;

    // KMIP specific parameters.
    sslKMIPParams.sslPEMKeyFile = kmipParams.kmipClientCertificateFile;
    sslKMIPParams.sslPEMKeyPassword = kmipParams.kmipClientCertificatePassword;
    sslKMIPParams.sslClusterFile = "";
    sslKMIPParams.sslClusterPassword = "";
    sslKMIPParams.sslCAFile = kmipParams.kmipServerCAFile;

    // Copy the rest from the global SSL manager options.
    sslKMIPParams.sslFIPSMode = sslParams.sslFIPSMode;

    // KMIP servers never should have invalid certificates
    sslKMIPParams.sslAllowInvalidCertificates = false;
    sslKMIPParams.sslAllowInvalidHostnames = false;
    sslKMIPParams.sslCRLFile = "";

    StatusWith<KMIPService> swKMIPService = KMIPService::createKMIPService(
        HostAndPort(kmipParams.kmipServerName, kmipParams.kmipPort), sslKMIPParams);

    if (!swKMIPService.isOK()) {
        return swKMIPService.getStatus();
    }
    auto kmipService = std::move(swKMIPService.getValue());

    // Try to retrieve an existing key.
    if (!keyId.empty()) {
        return kmipService.getExternalKey(keyId.toString());
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

}  // namespace mongo
