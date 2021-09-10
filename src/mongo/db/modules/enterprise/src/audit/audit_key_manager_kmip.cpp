/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "audit_key_manager_kmip.h"

#include "encryptdb/encryption_key_acquisition.h"
#include "encryptdb/symmetric_crypto.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

namespace crypto = ::mongo::crypto;

namespace mongo {
namespace audit {

namespace {
constexpr auto kKMIPKeyId = "kmipLogEncryptKey"_sd;
constexpr auto kProviderField = "provider"_sd;
constexpr auto kProviderValue = "kmip"_sd;
constexpr auto kUIDField = "uid"_sd;
}  // namespace

AuditKeyManagerKMIP::AuditKeyManagerKMIP(kmip::KMIPService ks, std::string uid)
    : _kmipService(std::move(ks)),
      _keyEncryptKeyUID(std::move(uid)),
      _keyStoreID(BSON(kProviderField << kProviderValue << kUIDField << _keyEncryptKeyUID)) {}

AuditKeyManager::KeyGenerationResult AuditKeyManagerKMIP::generateWrappedKey() {
    // create the ephemeral log encryption key
    SymmetricKey logEncryptKey(crypto::aesGenerate(crypto::sym256KeySize, kKMIPKeyId));
    const uint8_t* key = logEncryptKey.getKey();
    std::vector<uint8_t> keyCopy(
        key,
        key +
            logEncryptKey.getKeySize());  // since uint8_t is a byte, this pointer arithmetic is OK

    // encrypt with the provided key encryption key's UID through KMIP
    auto swEncrypted = _kmipService.encrypt(_keyEncryptKeyUID, keyCopy);

    if (!swEncrypted.getStatus().isOK()) {
        LOGV2_FATAL(5884600,
                    "Failed to encrypt key with KMIP",
                    "reason"_attr = swEncrypted.getStatus().reason());
    }

    return AuditKeyManager::KeyGenerationResult{std::move(logEncryptKey),
                                                std::move(swEncrypted.getValue())};
}

SymmetricKey AuditKeyManagerKMIP::unwrapKey(WrappedKey wrappedKey) {
    // decrypt with the provided key encryption key's UID through KMIP
    auto swDecrypted = _kmipService.decrypt(_keyEncryptKeyUID, wrappedKey);

    if (!swDecrypted.getStatus().isOK()) {
        LOGV2_FATAL(5884601,
                    "Failed to decrypt key with KMIP",
                    "reason"_attr = swDecrypted.getStatus().reason());
    }

    SecureVector<std::uint8_t> secureKey(swDecrypted.getValue().begin(),
                                         swDecrypted.getValue().end());

    uassert(ErrorCodes::BadValue,
            "Failed decrypting audit key, wrong size",
            secureKey->size() == crypto::sym256KeySize);

    return SymmetricKey(secureKey, crypto::aesAlgorithm, kKMIPKeyId);
}

BSONObj AuditKeyManagerKMIP::getKeyStoreID() {
    return _keyStoreID;
}

}  // namespace audit
}  // namespace mongo
