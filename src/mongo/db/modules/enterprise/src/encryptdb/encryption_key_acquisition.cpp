/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "encryption_key_acquisition.h"

#include <memory>

#include "encryption_options.h"
#include "kmip/kmip_service.h"
#include "mongo/base/string_data.h"
#include "mongo/config.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/crypto/symmetric_key.h"
#include "mongo/db/auth/security_file.h"
#include "mongo/logv2/log.h"
#include "mongo/util/base64.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/str.h"

using namespace mongo::kmip;

namespace mongo {
StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKeyFile(StringData encryptionKeyFile) {
    auto keyStrings = readSecurityFile(encryptionKeyFile.toString());
    if (!keyStrings.isOK()) {
        return keyStrings.getStatus();
    }

    if (keyStrings.getValue().size() > 1) {
        return {ErrorCodes::BadValue,
                "The encrypted storage engine only supports storing one key in the key file"};
    }

    const auto& keyString = keyStrings.getValue().front();
    std::string decodedKey;
    try {
        decodedKey = base64::decode(keyString);
    } catch (const DBException& ex) {
        return ex.toStatus();
    }

    const size_t keyLength = decodedKey.size();
    if (keyLength != crypto::sym256KeySize) {
        return {ErrorCodes::BadValue,
                str::stream() << "Encryption key in " << encryptionKeyFile << " is "
                              << keyLength * 8 << " bit, must be " << crypto::sym256KeySize * 8
                              << " bit."};
    }

    return std::make_unique<SymmetricKey>(reinterpret_cast<const uint8_t*>(decodedKey.data()),
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

    auto kmipService = getGlobalKMIPService();

    // Try to retrieve an existing key.
    if (!keyId.empty()) {
        return kmipService->getExternalKey(keyId.toString());
    }

    // Create and retrieve new key.
    StatusWith<std::string> swCreateKey = kmipService->createExternalKey();
    if (!swCreateKey.isOK()) {
        return swCreateKey.getStatus();
    }

    std::string newKeyId = swCreateKey.getValue();
    LOGV2(24199, "Created KMIP key", "keyId"_attr = newKeyId);
    return kmipService->getExternalKey(newKeyId);
}

}  // namespace mongo
