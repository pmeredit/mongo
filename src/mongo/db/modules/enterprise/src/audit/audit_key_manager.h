
/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/crypto/symmetric_key.h"

namespace mongo {
namespace audit {

/**
 *  Manages the lifecycle of cryptographic material used to protect the audit log.
 *  Instances generate Log Encryption Keys, used to directly encrypt audit events, and
 *  persistable copies wrapped using a remote Key Management System.
 *  Wrapped Log Encryption Keys may be safely written to disk, and can be unwrapped
 *  to produce the original Log Encryption Key for use in decryption.
 */
class AuditKeyManager {
public:
    AuditKeyManager() = default;
    virtual ~AuditKeyManager() = default;

    using WrappedKey = std::vector<std::uint8_t>;
    struct KeyGenerationResult {
        // A cryptographic Log Encryption Key which may be used to encrypt audit entries.
        SymmetricKey key;
        // An encrypted or otherwise protected copy of the Log Encryption Key.
        WrappedKey wrappedKey;
    };
    /**
     * Generate a Log Encryption Key, and a wrapped copy with a remote KMS.
     */
    virtual KeyGenerationResult generateWrappedKey() = 0;
    /**
     * Using a remote KMS, unwrap a Log Encryption Key, producing the raw key.
     */
    virtual SymmetricKey unwrapKey(WrappedKey wrappedKey) = 0;
    /**
     * Get details about the managed key store in BSON format
     */
    virtual BSONObj getKeyStoreID() const = 0;
};

}  // namespace audit
}  // namespace mongo
