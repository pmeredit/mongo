/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <cstdint>
#include <memory>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/secure_allocator.h"
#include "symmetric_crypto.h"

namespace mongo {
class Status;

/**
 * Class representing a symmetric key
 */
class SymmetricKey {
    MONGO_DISALLOW_COPYING(SymmetricKey);

public:
    SymmetricKey(const uint8_t* key,
                 size_t keySize,
                 uint32_t algorithm,
                 std::string keyId,
                 uint32_t initializationCount);
    SymmetricKey(SecureVector<uint8_t> key, uint32_t algorithm, std::string keyId);

    SymmetricKey(SymmetricKey&&);
    SymmetricKey& operator=(SymmetricKey&&);

    ~SymmetricKey() = default;

    int getAlgorithm() const {
        return _algorithm;
    }

    size_t getKeySize() const {
        return _keySize;
    }

    // Return the number of times the key has been retrieved from the key store
    uint32_t getInitializationCount() const {
        return _initializationCount;
    }

    const uint8_t* getKey() const {
        return _key.data();
    }

    const std::string& getKeyId() const {
        return _keyId;
    }

private:
    int _algorithm;

    size_t _keySize;

    SecureVector<uint8_t> _key;

    std::string _keyId;

    uint32_t _initializationCount;
};
}  // namespace mongo
