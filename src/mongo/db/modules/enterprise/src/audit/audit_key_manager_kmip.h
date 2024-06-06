/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "audit_key_manager.h"
#include "kmip/kmip_service.h"
#include "mongo/crypto/symmetric_crypto.h"

namespace mongo {
namespace audit {

enum class KeyStoreIDFormat { kmsConfigStruct, kmipKeyIdentifier };

class AuditKeyManagerKMIP : public AuditKeyManager {
public:
    explicit AuditKeyManagerKMIP(std::string uid);
    BSONObj getKeyStoreID() const final;

protected:
    BSONObj _keyStoreID;
    std::string _keyEncryptKeyUID;
};

class AuditKeyManagerKMIPEncrypt final : public AuditKeyManagerKMIP {
public:
    AuditKeyManagerKMIPEncrypt(std::string uid, KeyStoreIDFormat format);
    KeyGenerationResult generateWrappedKey() final;
    SymmetricKey unwrapKey(WrappedKey wrappedKey) final;
};

class AuditKeyManagerKMIPGet final : public AuditKeyManagerKMIP {
public:
    AuditKeyManagerKMIPGet(std::string uid);
    KeyGenerationResult generateWrappedKey() final;
    SymmetricKey unwrapKey(WrappedKey wrappedKey) final;

private:
    UniqueSymmetricKey fetchKeyEncryptKey();

    // Expected wrapped key size is the ciphertext length (same as the plaintext key size)
    // plus the length of the GCM tag prepended to the key ciphertext.
    static constexpr std::size_t kWrappedKeyLen = crypto::aesGCMTagSize + crypto::sym256KeySize;
};

}  // namespace audit
}  // namespace mongo
