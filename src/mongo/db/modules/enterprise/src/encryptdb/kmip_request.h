/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <sstream>
#include <cstring>
#include <vector>

#include "kmip_consts.h"

namespace mongo {
namespace kmip {

/**
 * KMIP Request Parameters are based off of the KMIP 1.2 Spec.
 */

struct KMIPRequestParameters {
    explicit KMIPRequestParameters(OperationType opType) {
        memset(operationType, 0, sizeof(operationType));
        operationType[3] = static_cast<uint8_t>(opType);
        protocolVersion[0] = MongoKMIPVersion[0];
        protocolVersion[1] = MongoKMIPVersion[1];
    }

    uint8_t operationType[4];
    uint32_t protocolVersion[2];
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
    GetKMIPRequestParameters(std::vector<uint8_t> u)
        : KMIPRequestParameters(OperationType::get), uid(u) {}

    std::vector<uint8_t> uid;
};

/**
 * Converts a 32 bit positive integer to vector of uint8_t.
 * This function should only be called with 32 bit positive integers.
 * Though KMIP supports 2C, this function would have to be refactored to do so.
 */
std::vector<uint8_t> convertIntToBigEndianArray(uint32_t input);

std::vector<uint8_t> encodeKMIPRequest(const KMIPRequestParameters& requestParams);

}  // namespace kmip
}  // namespace mongo
