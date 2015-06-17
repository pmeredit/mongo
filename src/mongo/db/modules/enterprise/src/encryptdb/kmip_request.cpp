/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#include "kmip_request.h"

#include <iostream>
#include <stdio.h>
#include <string>
#include <vector>

#include "kmip_consts.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace kmip {

    namespace {
        std::vector<uint8_t> padToEightByteMultiple(const std::vector<uint8_t>& input) {
            std::vector<uint8_t> output(input);
            size_t uidSize = input.size();
            int paddingRequired = (uidSize % 8);
            if (paddingRequired != 0) {
                for (int i = 0; i < 8 - paddingRequired; i++) {
                    output.push_back(0x00);
                }
            }
            return output;
        }

        std::vector<uint8_t> encodeAttribute(std::vector<uint8_t>* attributeName,
                                             ItemType attributeType,
                                             const std::vector<uint8_t>& attributeData) {

            // The starting value corresponds to the size of the various tags that
            // are always present.
            size_t attributeSizeInt = 16;
            std::vector<uint8_t> attribute;

            // For non-structures, the Item Length is the number of bytes excluding
            // the padding bytes. However, the overall structure's size includes the padding.
            std::vector<uint8_t> nameSize = convertIntToBigEndianArray(attributeName->size());
            std::vector<uint8_t> paddedName = padToEightByteMultiple(*attributeName);
            attributeSizeInt += paddedName.size();

            std::vector<uint8_t> dataSize = convertIntToBigEndianArray(attributeData.size());
            std::vector<uint8_t> processedAttributeData;
            // Figure out whether we need to do any extra processing to the attributeData,
            // then do it, and update attributeSizeInt for overall structure size.
            if (attributeType == ItemType::integer ||
                attributeType == ItemType::enumeration) {
                // integers and enumerations must be padded with 4 bytes.
                processedAttributeData = padToEightByteMultiple(attributeData);
                attributeSizeInt += processedAttributeData.size();
            } else {
                // not supported
                fassertFailed(4042);
            }
            std::vector<uint8_t> attributeSize = convertIntToBigEndianArray(attributeSizeInt);

            //  Tag: Attribute (0x420008), Type: Structure (0x01), Data: Size
            attribute.insert(attribute.end(), std::begin(attributeTag), std::end(attributeTag));
            attribute.push_back(static_cast<uint8_t>(ItemType::structure));
            attribute.insert(attribute.end(), std::begin(attributeSize), std::end(attributeSize));

            //  Tag: Attribute Name (0x42000A), Type: Text String (0x07), Size, Data: attributeName
            attribute.insert(attribute.end(),
                             std::begin(attributeNameTag), std::end(attributeNameTag));
            attribute.push_back(static_cast<uint8_t>(ItemType::textString));
            attribute.insert(attribute.end(), std::begin(nameSize), std::end(nameSize));
            attribute.insert(attribute.end(), std::begin(paddedName), std::end(paddedName));

            //  Tag: Attribute Value (0x42000B), Type: attributeType, Size, Data: attributeData
            attribute.insert(attribute.end(),
                             std::begin(attributeValueTag), std::end(attributeValueTag));
            attribute.push_back(static_cast<uint8_t>(attributeType));
            attribute.insert(attribute.end(), std::begin(dataSize), std::end(dataSize));
            attribute.insert(attribute.end(),
                             std::begin(processedAttributeData), std::end(processedAttributeData));

            return attribute;
        }

        std::vector<uint8_t> encodeCreatePayload(const CreateKMIPRequestParameters& requestParams) {
            std::vector<uint8_t> payload;

            // Tag: Object Type, Type: Enumeration, Enumeration Size, Data: Symmetric Key
            payload.insert(payload.end(), std::begin(objectTypeTag), std::end(objectTypeTag));
            payload.push_back(static_cast<uint8_t>(ItemType::enumeration));
            payload.insert(payload.end(),
                           std::begin(enumerationByteLength), std::end(enumerationByteLength));
            payload.insert(payload.end(),
                           std::begin(symmetricKeyObjectTypeArray),
                           std::end(symmetricKeyObjectTypeArray));
            payload.insert(payload.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

            size_t templateSizeInt = 0;

            std::vector<uint8_t> cryptoAlgorithmTextStringVector(
                    std::begin(cryptoAlgorithmTextString), std::end(cryptoAlgorithmTextString));
            std::vector<uint8_t> cryptoAlgorithmAttr =
                encodeAttribute(&cryptoAlgorithmTextStringVector,
                                ItemType::enumeration,
                                requestParams.cryptoAlgorithm);
            templateSizeInt += cryptoAlgorithmAttr.size();

            std::vector<uint8_t> cryptoLengthTextStringVector(
                    std::begin(cryptoLengthTextString), std::end(cryptoLengthTextString));
            std::vector<uint8_t> cryptoLengthAttr =
                encodeAttribute(&cryptoLengthTextStringVector,
                                ItemType::integer,
                                requestParams.cryptoLength);
            templateSizeInt += cryptoLengthAttr.size();

            std::vector<uint8_t> cryptoUsageMaskTextStringVector(
                    std::begin(cryptoUsageMaskTextString), std::end(cryptoUsageMaskTextString));
            std::vector<uint8_t> cryptoUsageMaskAttr =
                encodeAttribute(&cryptoUsageMaskTextStringVector,
                                ItemType::integer,
                                requestParams.cryptoUsageMask);
            templateSizeInt += cryptoUsageMaskAttr.size();

            std::vector<uint8_t> templateSize = convertIntToBigEndianArray(templateSizeInt);

            // Tag: Template-Attribute, Type: Structure, Data: Size
            payload.insert(payload.end(),
                           std::begin(templateAttributeTag), std::end(templateAttributeTag));
            payload.push_back(static_cast<uint8_t>(ItemType::structure));
            payload.insert(payload.end(), std::begin(templateSize), std::end(templateSize));

            payload.insert(payload.end(),
                           std::begin(cryptoAlgorithmAttr), std::end(cryptoAlgorithmAttr));
            payload.insert(payload.end(),
                           std::begin(cryptoLengthAttr), std::end(cryptoLengthAttr));
            payload.insert(payload.end(),
                           std::begin(cryptoUsageMaskAttr), std::end(cryptoUsageMaskAttr));

            return payload;
        }

        std::vector<uint8_t> encodeGetPayload(const GetKMIPRequestParameters& requestParams) {
            std::vector<uint8_t> payload;

            std::vector<uint8_t> paddedUUID = padToEightByteMultiple(requestParams.uid);
            // For Text Strings, the Item Length is the number of bytes excluding the padding bytes.
            std::vector<uint8_t> uidSize = convertIntToBigEndianArray(requestParams.uid.size());

            // Tag: Unique Identifier, Type: Text String, Size of UUID, UUID
            payload.insert(payload.end(),
                           std::begin(uniqueIdentifierTag), std::end(uniqueIdentifierTag));
            payload.push_back(static_cast<uint8_t>(ItemType::textString));

            payload.insert(payload.end(), std::begin(uidSize), std::end(uidSize));
            payload.insert(payload.end(), std::begin(paddedUUID), std::end(paddedUUID));

            return payload;
        }

        std::vector<uint8_t> encodeOpAndRequestPayload(const KMIPRequestParameters& requestParams) {
            std::vector<uint8_t> request;

            // Tag: Operation, Type: Enumeration, Enumeration Size, Data: OperationType
            request.insert(request.end(), std::begin(operationTag), std::end(operationTag));
            request.push_back(static_cast<uint8_t>(ItemType::enumeration));
            request.insert(request.end(),
                           std::begin(enumerationByteLength), std::end(enumerationByteLength));
            request.insert(request.end(),
                           std::begin(requestParams.operationType),
                           std::end(requestParams.operationType));
            request.insert(request.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

            std::vector<uint8_t> requestPayload;
            if (std::memcmp(requestParams.operationType, createOperationTypeArray,
                            sizeof(createOperationTypeArray)) == 0) {
                requestPayload = encodeCreatePayload(
                        static_cast<const CreateKMIPRequestParameters&>(requestParams));
            } else if (std::memcmp(requestParams.operationType, getOperationTypeArray,
                                sizeof(getOperationTypeArray)) == 0) {
                requestPayload = encodeGetPayload(
                        static_cast<const GetKMIPRequestParameters&>(requestParams));
            } else if (std::memcmp(requestParams.operationType, discoverVersionsOperationTypeArray,
                                sizeof(getOperationTypeArray)) == 0) {
                // Discover Versions has no request payload.
            } else {
                MONGO_UNREACHABLE;
            }
            std::vector<uint8_t> payloadSize = convertIntToBigEndianArray(requestPayload.size());

            // Tag: RequestPayload, Type: Structure, Data: Size
            request.insert(request.end(),
                           std::begin(requestPayloadTag), std::end(requestPayloadTag));
            request.push_back(static_cast<uint8_t>(ItemType::structure));
            request.insert(request.end(), std::begin(payloadSize), std::end(payloadSize));

            // Request payload contents.
            request.insert(request.end(), std::begin(requestPayload), std::end(requestPayload));

            return request;
        }

        std::vector<uint8_t> encodeBatchItem(const KMIPRequestParameters& requestParams) {
            std::vector<uint8_t> item;
            std::vector<uint8_t> request = encodeOpAndRequestPayload(requestParams);
            std::vector<uint8_t> requestSize = convertIntToBigEndianArray(request.size());

            // Tag: Batch Item, Type: Structure, Data: Size
            item.insert(item.end(), std::begin(batchItemTag), std::end(batchItemTag));
            item.push_back(static_cast<uint8_t>(ItemType::structure));
            item.insert(item.end(), std::begin(requestSize), std::end(requestSize));

            // Operation Type information and Request Payload.
            item.insert(item.end(), std::begin(request), std::end(request));

            return item;
        }

        std::vector<uint8_t> encodeProtocolVersion(const KMIPRequestParameters& requestParams) {
            std::vector<uint8_t> version;

            std::vector<uint8_t> majorVersion =
                convertIntToBigEndianArray(requestParams.protocolVersion[0]);
            std::vector<uint8_t> minorVersion =
                convertIntToBigEndianArray(requestParams.protocolVersion[1]);

            // Tag: Protocol Version Major, Type: Integer, Integer Size, Data: ...
            version.insert(version.end(),
                           std::begin(protocolVersionMajorTag), std::end(protocolVersionMajorTag));
            version.push_back(static_cast<uint8_t>(ItemType::integer));
            version.insert(version.end(),
                           std::begin(integerByteLength), std::end(integerByteLength));
            version.insert(version.end(), std::begin(majorVersion), std::end(majorVersion));
            version.insert(version.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

            // Tag: Protocol Version Minor, Type: Integer, Integer Size, Data: ...
            version.insert(version.end(),
                           std::begin(protocolVersionMinorTag), std::end(protocolVersionMinorTag));
            version.push_back(static_cast<uint8_t>(ItemType::integer));
            version.insert(version.end(),
                           std::begin(integerByteLength), std::end(integerByteLength));
            version.insert(version.end(), std::begin(minorVersion), std::end(minorVersion));
            version.insert(version.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

            return version;
        }

        std::vector<uint8_t> encodeRequestHeader(const KMIPRequestParameters& requestParams) {
            std::vector<uint8_t> header;
            std::vector<uint8_t> version = encodeProtocolVersion(requestParams);
            std::vector<uint8_t> versionSize = convertIntToBigEndianArray(version.size());

            // Tag: Protocol Version, Type: Structure, Data: Size
            header.insert(header.end(),
                          std::begin(protocolVersionTag),
                          std::end(protocolVersionTag));
            header.push_back(static_cast<uint8_t>(ItemType::structure));
            header.insert(header.end(), std::begin(versionSize), std::end(versionSize));

            // Insert the version information.
            header.insert(header.end(), std::begin(version), std::end(version));

            // Tag: Batch Count, Type: Integer, Integer Size, Data: Count (1)
            std::vector<uint8_t> batchCount = convertIntToBigEndianArray(1);
            header.insert(header.end(), std::begin(batchCountTag), std::end(batchCountTag));
            header.push_back(static_cast<uint8_t>(ItemType::integer));
            header.insert(header.end(), std::begin(integerByteLength), std::end(integerByteLength));
            header.insert(header.end(), std::begin(batchCount), std::end(batchCount));
            header.insert(header.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

            return header;
        }
    } // namespace

    std::vector<uint8_t> convertIntToBigEndianArray(uint32_t input) {
        std::vector<uint8_t> output;
        output.push_back((input >> 24) & 0xff);
        output.push_back((input >> 16) & 0xff);
        output.push_back((input >> 8) & 0xff);
        output.push_back(input & 0xff);
        return output;
    }

    std::vector<uint8_t> encodeKMIPRequest(const KMIPRequestParameters& requestParams) {
        std::vector<uint8_t> msg;

        std::vector<uint8_t> header = encodeRequestHeader(requestParams);
        std::vector<uint8_t> headerSize = convertIntToBigEndianArray(header.size());

        // Request size is the size of the header and the batch items.
        // The +8 is because the requestHeaderTag is not included in the header.size().
        std::vector<uint8_t> item = encodeBatchItem(requestParams);
        size_t requestSizeInt = header.size() + 8 + item.size();
        std::vector<uint8_t> requestSize = convertIntToBigEndianArray(requestSizeInt);

        // Tag: Request Message, Type: Structure, Data: Size
        msg.insert(msg.end(), std::begin(requestMessageTag), std::end(requestMessageTag));
        msg.push_back(static_cast<uint8_t>(ItemType::structure));
        msg.insert(msg.end(), std::begin(requestSize), std::end(requestSize));

        // Tag: Request Header, Type: Structure, Data: Size
        msg.insert(msg.end(), std::begin(requestHeaderTag), std::end(requestHeaderTag));
        msg.push_back(static_cast<uint8_t>(ItemType::structure));
        msg.insert(msg.end(), std::begin(headerSize), std::end(headerSize));

        // The request header.
        msg.insert(msg.end(), std::begin(header), std::end(header));

        // The batch item.
        msg.insert(msg.end(), std::begin(item), std::end(item));

        return msg;
    }

} // namespace kmip
} // namespace mongo

