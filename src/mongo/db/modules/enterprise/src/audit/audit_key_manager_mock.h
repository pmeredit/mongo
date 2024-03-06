/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include "audit_key_manager.h"

namespace mongo {
namespace audit {

class AuditKeyManagerMock final : public AuditKeyManager {
public:
    AuditKeyManagerMock();

    KeyGenerationResult generateWrappedKey() final;
    SymmetricKey unwrapKey(WrappedKey wrappedKey) final;
    BSONObj getKeyStoreID() const final;

private:
    SymmetricKey _defaultKey;
    WrappedKey _defaultWrappedKey;
};

}  // namespace audit
}  // namespace mongo
