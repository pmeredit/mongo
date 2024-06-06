/*
 * Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "encryption_key_acquisition.h"

#include <memory>

#include "encryptdb/encryption_options_gen.h"
#include "encryption_options.h"
#include "kmip/kmip_key_active_periodic_job.h"
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


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

Status _startPolling(KMIPService& kmipService, const KMIPParams& kmipParams, StringData keyId) {
    auto status =
        KMIPIsActivePollingJob::get(getGlobalServiceContext())
            ->createJob(kmipService, keyId.toString(), kmipParams.kmipKeyStatePollingSeconds);

    if (status.isOK()) {
        KMIPIsActivePollingJob::get(getGlobalServiceContext())->startJob();
    }
    return status;
}

StatusWith<std::unique_ptr<SymmetricKey>> _fetchKey(KMIPService& kmipService,
                                                    const KMIPParams& kmipParams,
                                                    StringData keyId,
                                                    bool activateKeyAndStartJob) {
    auto swKey = kmipService.getExternalKey(keyId.toString());
    if (!swKey.isOK() || !activateKeyAndStartJob) {
        return swKey;
    }

    auto status = _startPolling(kmipService, kmipParams, keyId);
    if (!status.isOK()) {
        return status;
    }

    return swKey;
}

StatusWith<std::unique_ptr<SymmetricKey>> _createKey(KMIPService& kmipService,
                                                     const KMIPParams& kmipParams,
                                                     bool activateKeyAndStartJob) {
    // Create and retrieve new key.
    StatusWith<std::string> swCreateKey = kmipService.createExternalKey();
    if (!swCreateKey.isOK()) {
        return swCreateKey.getStatus();
    }

    std::string newKeyId = swCreateKey.getValue();
    LOGV2(24199, "Created KMIP key", "keyId"_attr = newKeyId);

    auto swKey = kmipService.getExternalKey(newKeyId);
    if (!swKey.isOK() || !activateKeyAndStartJob) {
        return swKey;
    }

    StatusWith<std::string> swActivatedUid = kmipService.activate(newKeyId);
    if (!swActivatedUid.isOK()) {
        return swActivatedUid.getStatus();
    }

    LOGV2(2360700, "Activated KMIP key", "uid"_attr = swActivatedUid.getValue());

    auto status = _startPolling(kmipService, kmipParams, newKeyId);
    if (!status.isOK()) {
        return status;
    }

    return swKey;
}

StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKMIPServer(const KMIPParams& kmipParams,
                                                               StringData keyId,
                                                               bool ignoreStateAttribute) {
    if (kmipParams.kmipServerName.empty()) {
        return Status(ErrorCodes::BadValue,
                      "Encryption at rest enabled but no key server specified");
    }

    auto swKmipService = KMIPService::createKMIPService();
    if (!swKmipService.isOK()) {
        return swKmipService.getStatus();
    }

    if (ignoreStateAttribute) {
        invariant(!keyId.empty());
    }

    bool activateKeyAndStartJob = (kmipParams.activateKeys && !ignoreStateAttribute);

    auto& kmipService = swKmipService.getValue();

    if (!keyId.empty()) {
        return _fetchKey(kmipService, kmipParams, keyId, activateKeyAndStartJob);
    } else {
        return _createKey(kmipService, kmipParams, activateKeyAndStartJob);
    }
}

}  // namespace mongo
