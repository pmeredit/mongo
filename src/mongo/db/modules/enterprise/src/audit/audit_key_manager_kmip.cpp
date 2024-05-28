/**
 *    Copyright (C) 2021 MongoDB Inc.
 */


#include "audit_key_manager_kmip.h"

#include "audit/audit_config_gen.h"
#include "audit_enc_comp_manager.h"
#include "encryptdb/encryption_options.h"
#include "kmip/kmip_options.h"
#include "mongo/bson/json.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {
namespace audit {

namespace {
constexpr auto kKMIPKeyId = "kmipLogEncryptKey"_sd;
constexpr auto kProviderField = "provider"_sd;
constexpr auto kServerField = "kmipServerName"_sd;
constexpr auto kPortField = "kmipPort"_sd;
constexpr auto kProviderValue = "kmip"_sd;
constexpr auto kKeyWrapMethodField = "keyWrapMethod"_sd;
constexpr auto kUIDField = "uid"_sd;
}  // namespace

AuditKeyManagerKMIP::AuditKeyManagerKMIP(std::string uid) : _keyEncryptKeyUID(std::move(uid)) {}

AuditKeyManagerKMIPEncrypt::AuditKeyManagerKMIPEncrypt(std::string uid, KeyStoreIDFormat format)
    : AuditKeyManagerKMIP(std::move(uid)) {
    // KMIP encrypt is only supported when using KMIP protocol 1.2+.
    uassert(ErrorCodes::BadValue,
            "By default, audit log encryption uses KMIP protocol version 1.2+, but "
            "security.kmip.useLegacyProtocol is set to true, forcing the use of the KMIP 1.0 "
            "protocol. To use the KMIP 1.0 protocol with "
            "audit log encryption, the auditEncryptKeyWithKMIPGet setParameter must be enabled.",
            encryptionGlobalParams.kmipParams.version[0] >= 1 &&
                encryptionGlobalParams.kmipParams.version[1] >= 2);
    constexpr auto kKeyWrapMethodName = "encrypt"_sd;

    if (format == KeyStoreIDFormat::kmsConfigStruct) {
        _keyStoreID = fromjson(_keyEncryptKeyUID);
    } else {
        _keyStoreID =
            BSON(kProviderField << kProviderValue << kUIDField << _keyEncryptKeyUID << kServerField
                                << encryptionGlobalParams.kmipParams.kmipServerName << kPortField
                                << encryptionGlobalParams.kmipParams.kmipPort << kKeyWrapMethodField
                                << kKeyWrapMethodName);
    }
}

AuditKeyManagerKMIPGet::AuditKeyManagerKMIPGet(std::string uid)
    : AuditKeyManagerKMIP(std::move(uid)) {

    constexpr auto kKeyWrapMethodName = "get"_sd;
    _keyStoreID =
        BSON(kProviderField << kProviderValue << kUIDField << _keyEncryptKeyUID << kServerField
                            << encryptionGlobalParams.kmipParams.kmipServerName << kPortField
                            << encryptionGlobalParams.kmipParams.kmipPort << kKeyWrapMethodField
                            << kKeyWrapMethodName);
}

AuditKeyManager::KeyGenerationResult AuditKeyManagerKMIPEncrypt::generateWrappedKey() {
    // create the ephemeral log encryption key
    SymmetricKey logEncryptKey(crypto::aesGenerate(crypto::sym256KeySize, kKMIPKeyId));

    const uint8_t* key = logEncryptKey.getKey();
    SecureVector<uint8_t> keyCopy(
        key,
        key +
            logEncryptKey.getKeySize());  // since uint8_t is a byte, this pointer arithmetic is OK

    // encrypt with the provided key encryption key's UID through KMIP
    auto swKmipService = kmip::KMIPService::createKMIPService();
    if (!swKmipService.isOK()) {
        LOGV2_FATAL(4250506,
                    "Failed to retrieve KMIP Service for encryption",
                    "reason"_attr = swKmipService.getStatus());
    }

    auto swResult = swKmipService.getValue().encrypt(_keyEncryptKeyUID, keyCopy);

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

AuditKeyManager::KeyGenerationResult AuditKeyManagerKMIPGet::generateWrappedKey() {
    // create the ephemeral log encryption key
    SymmetricKey logEncryptKey(crypto::aesGenerate(crypto::sym256KeySize, kKMIPKeyId));

    // get the key encryption key from KMIP server
    auto keyEncryptKey = fetchKeyEncryptKey();

    // generate random IV
    // per the NIST recommendation on usage of RBG-constructed IVs, the
    // security of this encryption rests on the assumption that the key
    // encrypt key will:
    //  - not be used more than 2^32 times with random IVs. Encryption of
    //    the log encrypt key shall only be performed once during process,
    //    startup, making it highly unlikely to exceed the usage limit
    //  - be rotated as often as needed to satisfy the usage limit
    //  - not be used for any other encryption operation
    //
    auto iv = std::vector<uint8_t>(aesGetIVSize(crypto::aesMode::gcm));
    uassertStatusOK(crypto::engineRandBytes(DataRange(iv)));

    // prepare the input and output buffers for GCM encrypt:
    // input: we use the KMIP key ID as authenticated data (AAD), a random
    //        IV, and the log encrypt key as plaintext
    // output: the tag and ciphertext are concatenated together in contiguous
    //         memory, but are passed as two separate DataRanges
    auto aad = ConstDataRange(_keyEncryptKeyUID.data(), _keyEncryptKeyUID.size());
    auto plaintext = ConstDataRange(logEncryptKey.getKey(), logEncryptKey.getKeySize());
    std::vector<uint8_t> tagAndCiphertext(kWrappedKeyLen);
    DataRange tag(tagAndCiphertext.data(), crypto::aesGCMTagSize);
    DataRange ciphertext(tagAndCiphertext.data() + crypto::aesGCMTagSize, plaintext.length());

    // encrypt the log encryption key with the key encryption key
    auto ctLen = AuditEncryptionCompressionManager::aesEncryptGCM(
        *keyEncryptKey, iv, aad, plaintext, ciphertext, tag);
    invariant(ctLen == plaintext.length());

    // Create BSONObj incorporating both key and IV, and pretend it's the wrapped key
    BSONObjBuilder builder;
    builder.appendBinData("iv", iv.size(), BinDataGeneral, iv.data());
    builder.appendBinData("key", tagAndCiphertext.size(), BinDataGeneral, tagAndCiphertext.data());
    BSONObj keyObj = builder.obj();

    return AuditKeyManager::KeyGenerationResult{
        std::move(logEncryptKey),
        std::vector<uint8_t>(keyObj.objdata(), keyObj.objdata() + keyObj.objsize())};
}

SymmetricKey AuditKeyManagerKMIPEncrypt::unwrapKey(WrappedKey wrappedKey) {
    // unwrap "Wrapped Key" which is actually IV and key
    BSONObj keyObj = BSONObj(reinterpret_cast<char*>(wrappedKey.data()));
    int keyLen, ivLen;
    auto ivPtr = keyObj.getField("iv").binData(ivLen);
    auto keyPtr = keyObj.getField("key").binData(keyLen);
    std::vector<uint8_t> key(keyPtr, keyPtr + keyLen);
    std::vector<uint8_t> iv(ivPtr, ivPtr + ivLen);

    // decrypt with the provided key encryption key's UID and IV through KMIP

    auto swKMIPService = kmip::KMIPService::createKMIPService();
    if (!swKMIPService.isOK()) {
        LOGV2_FATAL(4250507,
                    "Unable to create KMIP Service for decryption",
                    "reason"_attr = swKMIPService.getStatus());
    }

    auto swDecrypted = swKMIPService.getValue().decrypt(_keyEncryptKeyUID, iv, key);

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

SymmetricKey AuditKeyManagerKMIPGet::unwrapKey(WrappedKey wrappedKey) {
    // unwrap "Wrapped Key" which is actually IV and key
    BSONObj keyObj = BSONObj(reinterpret_cast<char*>(wrappedKey.data()));
    int keyLen, ivLen;
    auto ivPtr = keyObj.getField("iv").binData(ivLen);
    auto keyPtr = keyObj.getField("key").binData(keyLen);
    uassert(ErrorCodes::OperationFailed,
            "Wrapped key is not the expected size",
            keyLen == kWrappedKeyLen);

    // get the key encryption key from KMIP server
    auto keyEncryptKey = fetchKeyEncryptKey();

    // prepare the input and output buffers for GCM decrypt:
    // input: KMIP key ID as authenticated data (AAD), the IV from
    //        the "iv" field, the first 12 bytes of the "key" field
    //        as tag, and the rest of the "key" field as ciphertext
    // output: DataRange backed by a SecureVector that can fit a 256-bit key
    auto iv = ConstDataRange(ivPtr, ivLen);
    auto aad = ConstDataRange(_keyEncryptKeyUID.data(), _keyEncryptKeyUID.size());
    auto ciphertext = ConstDataRange(keyPtr + crypto::aesGCMTagSize, crypto::sym256KeySize);
    auto tag = ConstDataRange(keyPtr, crypto::aesGCMTagSize);
    SecureVector<std::uint8_t> keyBuf(crypto::sym256KeySize);
    auto plaintext = DataRange(keyBuf->data(), keyBuf->capacity());

    // decrypt the wrapped key with GCM
    AuditEncryptionCompressionManager::aesDecryptGCM(
        *keyEncryptKey, iv, aad, ciphertext, tag, plaintext);

    return SymmetricKey(std::move(keyBuf), crypto::aesAlgorithm, kKMIPKeyId);
}

UniqueSymmetricKey AuditKeyManagerKMIPGet::fetchKeyEncryptKey() {
    uassert(ErrorCodes::BadValue,
            "Cannot get key from KMIP server with empty key UID",
            !_keyEncryptKeyUID.empty());

    // get the key encryption key from KMIP server
    auto kmipService =
        uassertStatusOKWithContext(kmip::KMIPService::createKMIPService(),
                                   "Failed to set up KMIP Service for Audit Key encryption");

    auto keyEncryptKey =
        uassertStatusOKWithContext(kmipService.getExternalKey(_keyEncryptKeyUID),
                                   "Failed to get external key for audit key encryption");

    // the retrieved key must be 256 bits in size
    uassert(ErrorCodes::BadValue,
            str::stream() << "Retrieved key encryption key from KMIP is "
                          << keyEncryptKey->getKeySize() * 8 << " bit; must be "
                          << crypto::sym256KeySize * 8 << " bit",
            keyEncryptKey->getKeySize() == crypto::sym256KeySize);
    return keyEncryptKey;
}

BSONObj AuditKeyManagerKMIP::getKeyStoreID() const {
    return _keyStoreID;
}

}  // namespace audit
}  // namespace mongo
