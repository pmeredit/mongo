/**
 *    Copyright (C) 2015 MongoDB Inc.
 *    Declaration of a subset of the KMIP constants
 *    in the KMIP 1.2 specification.
 */

#pragma once

#include "mongo/platform/cstdint.h"

namespace mongo {
namespace kmip {
    /**
     * The KMIP Protocol Version that we implement for requests.
     */
    const int32_t MongoKMIPVersion[] = {1, 0};

    /**
     * Padding.
     */
    const uint8_t fourBytePadding[]   = {0x00, 0x00, 0x00, 0x00};

    /**
     * Message Tags.
     */
    const uint8_t attributeTag[]              = {0x42, 0x00, 0x08};
    const uint8_t attributeNameTag[]          = {0x42, 0x00, 0x0A};
    const uint8_t attributeValueTag[]         = {0x42, 0x00, 0x0B};
    const uint8_t batchCountTag[]             = {0x42, 0x00, 0x0D};
    const uint8_t batchItemTag[]              = {0x42, 0x00, 0x0F};
    const uint8_t cryptographicAlgorithmTag[] = {0x42, 0x00, 0x28};
    const uint8_t cryptographicLengthTag[]    = {0x42, 0x00, 0x2A};
    const uint8_t keyBlockTag[]               = {0x42, 0x00, 0x40};
    const uint8_t keyFormatTypeTag[]          = {0x42, 0x00, 0x42};
    const uint8_t keyMaterialTag[]            = {0x42, 0x00, 0x43};
    const uint8_t keyValueTag[]               = {0x42, 0x00, 0x45};
    const uint8_t objectTypeTag[]             = {0x42, 0x00, 0x57};
    const uint8_t operationTag[]              = {0x42, 0x00, 0x5C};
    const uint8_t protocolVersionTag[]        = {0x42, 0x00, 0x69};
    const uint8_t protocolVersionMajorTag[]   = {0x42, 0x00, 0x6A};
    const uint8_t protocolVersionMinorTag[]   = {0x42, 0x00, 0x6B};
    const uint8_t requestHeaderTag[]          = {0x42, 0x00, 0x77};
    const uint8_t requestMessageTag[]         = {0x42, 0x00, 0x78};
    const uint8_t requestPayloadTag[]         = {0x42, 0x00, 0x79};
    const uint8_t responseHeaderTag[]         = {0x42, 0x00, 0x7A};
    const uint8_t responseMessageTag[]        = {0x42, 0x00, 0x7B};
    const uint8_t responsePayloadTag[]        = {0x42, 0x00, 0x7C};
    const uint8_t resultMessageTag[]          = {0x42, 0x00, 0x7D};
    const uint8_t resultReasonTag[]           = {0x42, 0x00, 0x7E};
    const uint8_t resultStatusTag[]           = {0x42, 0x00, 0x7F};
    const uint8_t symmetricKeyTag[]           = {0x42, 0x00, 0x8F};
    const uint8_t templateAttributeTag[]      = {0x42, 0x00, 0x91};
    const uint8_t timeStampTag[]              = {0x42, 0x00, 0x92};
    const uint8_t uniqueBatchItemIDTag[]      = {0x42, 0x00, 0x93};
    const uint8_t uniqueIdentifierTag[]       = {0x42, 0x00, 0x94};

    /**
     * Item Types
     */
    enum class ItemType : uint8_t {
        structure   = 0x01,
        integer     = 0x02,
        longInteger = 0x03,
        bigInteger  = 0x04,
        enumeration = 0x05,
        boolean     = 0x06,
        textString  = 0x07,
        byteString  = 0x08,
        dateTime    = 0x09,
        interval    = 0x0A
    };

    /**
     * Operation Types
     */
    enum class OperationType : uint8_t {
        create             = 0x01,
        discoverVersions   = 0x1E,
        get                = 0x0A
    };

    const uint8_t createOperationTypeArray[]           = {0x00, 0x00, 0x00, 0x01};
    const uint8_t discoverVersionsOperationTypeArray[] = {0x00, 0x00, 0x00, 0x1E};
    const uint8_t getOperationTypeArray[]              = {0x00, 0x00, 0x00, 0x0A};

    /**
     * Item Type Byte Lengths
     */
    const uint8_t integerByteLength[]     = {0x00, 0x00, 0x00, 0x04};
    const uint8_t enumerationByteLength[] = {0x00, 0x00, 0x00, 0x04};

    /**
     * Object Types
     */
    const uint8_t symmetricKeyObjectType = 0x02;
    const uint8_t symmetricKeyObjectTypeArray[] = {0x00, 0x00, 0x00, 0x02};

    /**
     * Cryptographic Algorithms
     */
    const uint8_t aesCryptoAlgorithm[]    = {0x00, 0x00, 0x00, 0x03};

    /**
     * Key format types
     */
    const uint8_t keyFormatRaw = 0x01;
    const uint8_t keyFormatSymmetricKey = 0x07;

    /**
     * Text String Hex Encodings
     *
     * "Cryptographic Algorithm"
     */
    const uint8_t cryptoAlgorithmTextString[] = {0x43, 0x72, 0x79, 0x70, 0x74, 0x6F,
                                                 0x67, 0x72, 0x61, 0x70, 0x68, 0x69,
                                                 0x63, 0x20, 0x41, 0x6C, 0x67, 0x6F,
                                                 0x72, 0x69, 0x74, 0x68, 0x6D};
    /**
     * "Cryptographic Length"
     */
    const uint8_t cryptoLengthTextString[] = {0x43, 0x72, 0x79, 0x70, 0x74, 0x6F, 0x67, 0x72,
                                              0x61, 0x70, 0x68, 0x69, 0x63, 0x20, 0x4C, 0x65,
                                              0x6E, 0x67, 0x74, 0x68};

    /**
     * "Cryptographic Usage Mask"
     */
    const uint8_t cryptoUsageMaskTextString[] = {0x43, 0x72, 0x79, 0x70, 0x74, 0x6F,
                                                 0x67, 0x72, 0x61, 0x70, 0x68, 0x69,
                                                 0x63, 0x20, 0x55, 0x73, 0x61, 0x67,
                                                 0x65, 0x20, 0x4D, 0x61, 0x73, 0x6B};

    const uint8_t cryptoUsageMaskEncrypt = 0x04;
    const uint8_t cryptoUsageMaskDecrypt = 0x08;

    /**
     * Return status codes
     */
    const uint32_t statusSuccess             = 0x00;
    const uint32_t statusOperationFailed     = 0x01;
    const uint32_t statusOperationPending    = 0x02;
    const uint32_t operationUndone           = 0x03;
} // namespace kmip
} // namespace mongo
