/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit_key_manager_local.h"

#include "encryptdb/encryption_key_acquisition.h"
#include "encryptdb/symmetric_crypto.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace audit {

namespace {
// Expected wrapped key size is the ciphertext (block aligned key size plus a full PKCS#7 padding
// block) plus the auto-generated IV prepended to the wrapped key by aesEncrypt.
constexpr std::size_t kWrappedKeyLen =
    crypto::HeaderCBCV0::kIVSize + crypto::sym256KeySize + crypto::aesBlockSize;
constexpr auto kLocalKeyId = "localLogEncryptKey"_sd;
constexpr auto kProviderField = "provider"_sd;
constexpr auto kFilenameField = "filename"_sd;
constexpr auto kProviderValue = "local"_sd;

}  // namespace

AuditKeyManagerLocal::AuditKeyManagerLocal(StringData keyPath) : _keyPath(keyPath) {
    // retrieve the key encryption key from the local file
    _keyEncryptKey = uassertStatusOK(mongo::getKeyFromKeyFile(_keyPath));

    // build the key store ID BSON object
    _keyStoreID = BSON(kProviderField << kProviderValue << kFilenameField << _keyPath);
}

AuditKeyManager::KeyGenerationResult AuditKeyManagerLocal::generateWrappedKey() {
    // create the ephemeral log encryption key
    SymmetricKey logEncryptKey(crypto::aesGenerate(crypto::sym256KeySize, kLocalKeyId));

    // encrypt the log encryption key with the key encryption key
    WrappedKey outBuf(kWrappedKeyLen);
    size_t outLen = 0;
    uassertStatusOK(crypto::aesEncrypt(*_keyEncryptKey,
                                       crypto::aesMode::cbc,
                                       crypto::PageSchema::k0,
                                       logEncryptKey.getKey(),
                                       logEncryptKey.getKeySize(),
                                       outBuf.data(),
                                       outBuf.size(),
                                       &outLen,
                                       false /* ivProvided */));
    invariant(outLen == kWrappedKeyLen);

    return AuditKeyManager::KeyGenerationResult{std::move(logEncryptKey), std::move(outBuf)};
}

SymmetricKey AuditKeyManagerLocal::unwrapKey(WrappedKey wrappedKey) {
    uassert(ErrorCodes::OperationFailed,
            "Wrapped key is not the expected size",
            wrappedKey.size() == kWrappedKeyLen);

    // We over-allocate this by an extra block size so that aesDecrypt() does not
    // complain about the buffer size being smaller than the max decrypt size.
    SecureVector<std::uint8_t> outBuf(crypto::sym256KeySize + crypto::aesBlockSize);
    size_t outLen = 0;
    uassertStatusOK(crypto::aesDecrypt(*_keyEncryptKey,
                                       crypto::aesMode::cbc,
                                       crypto::PageSchema::k0,
                                       wrappedKey.data(),
                                       wrappedKey.size(),
                                       outBuf->data(),
                                       outBuf->capacity(),
                                       &outLen));
    uassert(ErrorCodes::BadValue,
            "Failed decrypting audit key, wrong size",
            outLen == crypto::sym256KeySize);
    outBuf->resize(outLen);
    return SymmetricKey(std::move(outBuf), crypto::aesAlgorithm, kLocalKeyId);
}

BSONObj AuditKeyManagerLocal::getKeyStoreID() const {
    return _keyStoreID;
}

}  // namespace audit
}  // namespace mongo
