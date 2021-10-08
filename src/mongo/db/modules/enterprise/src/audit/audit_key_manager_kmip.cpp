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
    SecureVector<uint8_t> keyCopy(
        key,
        key +
            logEncryptKey.getKeySize());  // since uint8_t is a byte, this pointer arithmetic is OK

    // encrypt with the provided key encryption key's UID through KMIP
    auto swResult = _kmipService.encrypt(_keyEncryptKeyUID, keyCopy);

    if (!swResult.getStatus().isOK()) {
        LOGV2_FATAL(5884600,
                    "Failed to encrypt key with KMIP",
                    "reason"_attr = swResult.getStatus().reason());
    }

    // Create BSONObj incorporating both key and IV, and pretend it's the wrapped key
    BSONObjBuilder builder;
    auto iv = std::move(swResult.getValue().iv);
    auto enckey = std::move(swResult.getValue().data);
    builder.appendBinData("iv", iv.size(), BinDataGeneral, iv.data());
    builder.appendBinData("key", enckey.size(), BinDataGeneral, enckey.data());
    BSONObj keyObj = builder.obj();

    return AuditKeyManager::KeyGenerationResult{
        std::move(logEncryptKey),
        std::vector<uint8_t>(keyObj.objdata(), keyObj.objdata() + keyObj.objsize())};
}

SymmetricKey AuditKeyManagerKMIP::unwrapKey(WrappedKey wrappedKey) {
    // unwrap "Wrapped Key" which is actually IV and key
    BSONObj keyObj = BSONObj(reinterpret_cast<char*>(wrappedKey.data()));
    int keylen, ivlen;
    auto ivPtr = keyObj.getField("iv").binData(ivlen);
    auto keyPtr = keyObj.getField("key").binData(keylen);
    std::vector<uint8_t> key(keyPtr, keyPtr + keylen);
    std::vector<uint8_t> iv(ivPtr, ivPtr + ivlen);
    // decrypt with the provided key encryption key's UID and IV through KMIP
    auto swDecrypted = _kmipService.decrypt(_keyEncryptKeyUID, iv, key);

    if (!swDecrypted.getStatus().isOK()) {
        LOGV2_FATAL(5884601,
                    "Failed to decrypt key with KMIP",
                    "reason"_attr = swDecrypted.getStatus().reason());
    }

    auto secureKey = std::move(swDecrypted.getValue());

    uassert(ErrorCodes::BadValue,
            "Failed decrypting audit key, wrong size",
            secureKey->size() == crypto::sym256KeySize);

    return SymmetricKey(std::move(secureKey), crypto::aesAlgorithm, kKMIPKeyId);
}

BSONObj AuditKeyManagerKMIP::getKeyStoreID() {
    return _keyStoreID;
}

}  // namespace audit
}  // namespace mongo
