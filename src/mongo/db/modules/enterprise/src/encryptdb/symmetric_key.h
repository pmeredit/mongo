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
    SymmetricKey(const uint8_t* key, size_t keySize, uint32_t algorithm);
    SymmetricKey(std::unique_ptr<uint8_t[]> key, size_t keySize, uint32_t algorithm);

    SymmetricKey(SymmetricKey&&);
    SymmetricKey& operator=(SymmetricKey&&);

    ~SymmetricKey();

    const int getAlgorithm() const {
        return _algorithm;
    }

    const size_t getKeySize() const {
        return _keySize;
    }

    const uint8_t* getKey() const {
        return _key.get();
    }

private:
    int _algorithm;

    size_t _keySize;

    std::unique_ptr<uint8_t[]> _key;
};
}  // namespace mongo
