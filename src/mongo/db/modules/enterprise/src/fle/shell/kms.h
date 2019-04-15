/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "fle/shell/kms_gen.h"
#include "mongo/base/data_range.h"
#include "mongo/base/secure_allocator.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

/**
 * KMSService
 *
 * Represents a Key Management Service. May be a local file KMS or remote.
 *
 * Responsible for securely encrypting and decrypting data. The encrypted data is treated as a
 * blockbox by callers.
 */
class KMSService {
public:
    virtual ~KMSService() = default;

    /**
     * Encrypt a plaintext with the specified key and return a encrypted blob.
     */
    virtual std::vector<uint8_t> encrypt(ConstDataRange cdr, StringData keyId) = 0;

    /**
     * Decrypt an encrypted blob and return the plaintext.
     */
    virtual SecureVector<uint8_t> decrypt(ConstDataRange cdr) = 0;

    /**
     * Encrypt a data key with the specified key and return a BSONObj that describes what needs to
     * be store in the key vault.
     *
     * {
     *   keyMaterial : "<ciphertext>""
     *   masterKey : {
     *     provider : "<provider_name>"
     *     ... <provider specific fields>
     *   }
     * }
     */
    virtual BSONObj encryptDataKey(ConstDataRange cdr, StringData keyId) = 0;
};

/**
 * KMSService Factory
 *
 * Provides static registration of KMSService.
 */
class KMSServiceFactory {
public:
    virtual ~KMSServiceFactory() = default;

    /**
     * Create an instance of the KMS service
     */
    virtual std::unique_ptr<KMSService> create(const BSONObj& config) = 0;
};

/**
 * KMSService Controller
 *
 * Provides static registration of KMSServiceFactory
 */
class KMSServiceController {
public:
    /**
     * Create an instance of the KMS service
     */
    static void registerFactory(KMSProviderEnum provider,
                                std::unique_ptr<KMSServiceFactory> factory);


    /**
     * Iterates over the factories and chooses the correct KMS Factory that parses the config
     * properly. Creates a KMS Service with the config and customer master key.
     */
    static std::unique_ptr<KMSService> createFromClient(const BSONObj& config);


    /**
     * Creates a KMS Service with the given config.
     */
    static std::unique_ptr<KMSService> createFromDisk(const BSONObj& config,
                                                      const BSONObj& kmsProvider);

private:
    static stdx::unordered_map<KMSProviderEnum, std::unique_ptr<KMSServiceFactory>> _factories;
};

/**
 * Parse a basic url of "https://host:port" to a HostAndPort.
 *
 * Does not support URL encoding or anything else.
 */
HostAndPort parseUrl(StringData url);

}  // namespace mongo
