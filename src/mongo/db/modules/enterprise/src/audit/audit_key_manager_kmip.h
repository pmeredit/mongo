/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include "audit_key_manager.h"
#include "kmip/kmip_service.h"

namespace mongo {
namespace audit {

class AuditKeyManagerKMIP final : public AuditKeyManager {
public:
    explicit AuditKeyManagerKMIP(kmip::KMIPService ks, std::string uid);

    KeyGenerationResult generateWrappedKey() final;
    SymmetricKey unwrapKey(WrappedKey wrappedKey) final;
    BSONObj getKeyStoreID() final;

private:
    kmip::KMIPService _kmipService;
    std::string _keyEncryptKeyUID;
    BSONObj _keyStoreID;
};

}  // namespace audit
}  // namespace mongo
