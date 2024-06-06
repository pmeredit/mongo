/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_key_manager_mock.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace audit {

namespace {
constexpr auto kProviderField = "provider"_sd;
constexpr auto kProviderValue = "mock"_sd;
}  // namespace

AuditKeyManagerMock::AuditKeyManagerMock()
    : _defaultKey(crypto::aesGenerate(crypto::sym256KeySize, "mockKey")),
      _defaultWrappedKey(crypto::sym256KeySize + 16, 0xEF) {}

AuditKeyManager::KeyGenerationResult AuditKeyManagerMock::generateWrappedKey() {
    return AuditKeyManager::KeyGenerationResult{SymmetricKey(_defaultKey.getKey(),
                                                             _defaultKey.getKeySize(),
                                                             _defaultKey.getAlgorithm(),
                                                             _defaultKey.getKeyId(),
                                                             _defaultKey.getInitializationCount()),
                                                _defaultWrappedKey};
}

SymmetricKey AuditKeyManagerMock::unwrapKey(WrappedKey wrappedKey) {
    uassert(ErrorCodes::OperationFailed,
            "Argument to unwrapKey must have been produced by generateWrappedKey",
            _defaultWrappedKey == wrappedKey);
    return SymmetricKey(_defaultKey.getKey(),
                        _defaultKey.getKeySize(),
                        _defaultKey.getAlgorithm(),
                        _defaultKey.getKeyId(),
                        _defaultKey.getInitializationCount());
}

BSONObj AuditKeyManagerMock::getKeyStoreID() const {
    BSONObjBuilder builder;
    builder.append(kProviderField, kProviderValue);
    return builder.obj();
}

}  // namespace audit
}  // namespace mongo
