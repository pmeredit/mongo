/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "encrypted_data_protector.h"
#include "encryption_key_manager.h"

#include <memory>

#include "mongo/base/status.h"
#include "mongo/crypto/symmetric_key.h"

namespace mongo {
class SymmetricKey;

class EncryptionKeyManagerNoop : public EncryptionKeyManager {
public:
    EncryptionKeyManagerNoop()
        : EncryptionKeyManager("", nullptr),
          _testMasterKey(std::move(getKey("test", FindMode::kCurrent).getValue())) {}

    StatusWith<std::unique_ptr<SymmetricKey>> getKey(const SymmetricKeyId& keyID,
                                                     FindMode) override {
        uint8_t key[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5,
                         0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5};
        return std::make_unique<SymmetricKey>(key, sizeof(key), crypto::aesAlgorithm, "test", 0);
    }

    std::unique_ptr<DataProtector> getDataProtector() final {
        return std::make_unique<EncryptedDataProtector>(_testMasterKey.get(), crypto::aesMode::cbc);
    }

private:
    UniqueSymmetricKey _testMasterKey;
};

}  // namespace mongo
