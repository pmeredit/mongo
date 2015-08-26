/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <cstdint>
#include <memory>

#include "mongo/base/disallow_copying.h"
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
    SymmetricKey(std::unique_ptr<uint8_t[]> key,
                 size_t keySize,
                 uint32_t algorithm,
                 std::string keyId);

    SymmetricKey(SymmetricKey&&);
    SymmetricKey& operator=(SymmetricKey&&);

    ~SymmetricKey();

    int getAlgorithm() const {
        return _algorithm;
    }

    size_t getKeySize() const {
        return _keySize;
    }

    // Return the number of times the key has been retrieved from the key store
    const uint32_t getInitializationCount() const {
        return _initializationCount;
    }

    const uint8_t* getKey() const {
        return _key.get();
    }

    const std::string& getKeyId() const {
        return _keyId;
    }

private:
    int _algorithm;

    size_t _keySize;

    std::unique_ptr<uint8_t[]> _key;

    std::string _keyId;

    uint32_t _initializationCount;
};
}  // namespace mongo
