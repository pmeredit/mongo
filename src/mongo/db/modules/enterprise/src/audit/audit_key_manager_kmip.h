/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include "audit_key_manager.h"
#include "kmip/kmip_service.h"
#include "mongo/crypto/symmetric_crypto.h"

namespace mongo {
namespace audit {

class AuditKeyManagerKMIP : public AuditKeyManager {
public:
    AuditKeyManagerKMIP(kmip::KMIPService ks, std::string uid);
    BSONObj getKeyStoreID() final;

protected:
    kmip::KMIPService _kmipService;
    std::string _keyEncryptKeyUID;
    BSONObj _keyStoreID;
};

class AuditKeyManagerKMIPEncrypt final : public AuditKeyManagerKMIP {
public:
    AuditKeyManagerKMIPEncrypt(kmip::KMIPService ks, std::string uid);
    KeyGenerationResult generateWrappedKey() final;
    SymmetricKey unwrapKey(WrappedKey wrappedKey) final;
};

class AuditKeyManagerKMIPGet final : public AuditKeyManagerKMIP {
public:
    AuditKeyManagerKMIPGet(kmip::KMIPService ks, std::string uid);
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
