/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <cstring>
#include <sstream>
#include <vector>

#include "encryptdb/encryption_options.h"
#include "kmip_consts.h"
#include "mongo/base/secure_allocator.h"

namespace mongo {
namespace kmip {

/**
 * KMIP Request Parameters are based off of the KMIP 1.2 Spec.
 */

struct KMIPRequestParameters {
    explicit KMIPRequestParameters(OperationType opType) {
        memset(operationType, 0, sizeof(operationType));
        operationType[3] = static_cast<uint8_t>(opType);
        protocolVersion[0] = encryptionGlobalParams.kmipParams.version[0];
        protocolVersion[1] = encryptionGlobalParams.kmipParams.version[1];
    }

    uint8_t operationType[4];
    uint32_t protocolVersion[2];
};

struct ActivateKMIPRequestParameters : KMIPRequestParameters {
    ActivateKMIPRequestParameters(std::vector<uint8_t> uid)
        : KMIPRequestParameters(OperationType::activate), uid(std::move(uid)) {}

    std::vector<uint8_t> uid;
};

struct CreateKMIPRequestParameters : KMIPRequestParameters {
    CreateKMIPRequestParameters(std::vector<uint8_t> algo,
                                std::vector<uint8_t> len,
                                std::vector<uint8_t> mask)
        : KMIPRequestParameters(OperationType::create),
          cryptoAlgorithm(algo),
          cryptoLength(len),
          cryptoUsageMask(mask) {}

    std::vector<uint8_t> cryptoAlgorithm;
    std::vector<uint8_t> cryptoLength;
    std::vector<uint8_t> cryptoUsageMask;
};

struct DiscoverVersionsKMIPRequestParameters : KMIPRequestParameters {
    DiscoverVersionsKMIPRequestParameters()
        : KMIPRequestParameters(OperationType::discoverVersions) {}
};

struct GetKMIPRequestParameters : KMIPRequestParameters {
    explicit GetKMIPRequestParameters(std::vector<uint8_t> uid)
        : KMIPRequestParameters(OperationType::get), uid(uid) {}

    std::vector<uint8_t> uid;
};

struct EncryptKMIPRequestParameters : KMIPRequestParameters {
    EncryptKMIPRequestParameters(std::vector<uint8_t> uid, SecureVector<uint8_t> data)
        : KMIPRequestParameters(OperationType::encrypt),
          uid(std::move(uid)),
          data(std::move(data)) {}

    std::vector<uint8_t> uid;
    SecureVector<uint8_t> data;
};

struct DecryptKMIPRequestParameters : KMIPRequestParameters {
    DecryptKMIPRequestParameters(std::vector<uint8_t> uid,
                                 std::vector<uint8_t> iv,
                                 std::vector<uint8_t> data)
        : KMIPRequestParameters(OperationType::decrypt),
          uid(std::move(uid)),
          iv(std::move(iv)),
          data(std::move(data)) {}

    std::vector<uint8_t> uid;
    std::vector<uint8_t> iv;
    std::vector<uint8_t> data;
};

struct GetAttributesKMIPRequestParameters : KMIPRequestParameters {
    GetAttributesKMIPRequestParameters(std::vector<uint8_t> uid, std::vector<uint8_t> attributeName)
        : KMIPRequestParameters(OperationType::getAttributes),
          uid(std::move(uid)),
          attributeName(std::move(attributeName)) {}

    std::vector<uint8_t> uid;
    std::vector<uint8_t> attributeName;
};

/**
 * Converts a 32 bit positive integer to vector of uint8_t.
 * This function should only be called with 32 bit positive integers.
 * Though KMIP supports 2C, this function would have to be refactored to do so.
 */
std::vector<uint8_t> convertIntToBigEndianArray(uint32_t input);

SecureVector<uint8_t> encodeKMIPRequest(const KMIPRequestParameters& requestParams);

}  // namespace kmip
}  // namespace mongo
