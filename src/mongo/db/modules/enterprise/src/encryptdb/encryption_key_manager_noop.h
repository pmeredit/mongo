/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include "encryption_key_manager.h"
#include "mongo/base/status.h"
#include "mongo/stdx/memory.h"
#include "symmetric_key.h"

namespace mongo {
class SymmetricKey;

class EncryptionKeyManagerNoop : public EncryptionKeyManager {
public:
    EncryptionKeyManagerNoop() : EncryptionKeyManager("", nullptr, nullptr) {}

    StatusWith<std::unique_ptr<SymmetricKey>> getKey(const std::string& keyID) override {
        uint8_t key[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5,
                         0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5};
        return stdx::make_unique<SymmetricKey>(key, sizeof(key), crypto::aesAlgorithm, "test", 0);
    }
};

}  // mongo
