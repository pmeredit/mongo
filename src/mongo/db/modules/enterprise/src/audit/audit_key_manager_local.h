/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include "audit_key_manager.h"

namespace mongo {
namespace audit {

class AuditKeyManagerLocal final : public AuditKeyManager {
public:
    explicit AuditKeyManagerLocal(StringData keyPath);

    KeyGenerationResult generateWrappedKey() final;
    SymmetricKey unwrapKey(WrappedKey wrappedKey) final;
    BSONObj getKeyStoreID() final;

private:
    UniqueSymmetricKey _keyEncryptKey;
    std::string _keyPath;
    BSONObj _keyStoreID;
};

}  // namespace audit
}  // namespace mongo
