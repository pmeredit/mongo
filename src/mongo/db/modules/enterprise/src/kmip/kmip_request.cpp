/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "kmip_request.h"

#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "kmip_consts.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace kmip {

namespace {

struct EncodingContext {
    SecureVector<uint8_t> payload;
};

template <typename T>
void padToEightByteMultipleHelper(std::vector<uint8_t, T>* output,
                                  const std::vector<uint8_t, T>& input) {
    *output = std::vector<uint8_t, T>(input);
    size_t uidSize = input.size();
    int paddingRequired = (uidSize % 8);
    if (paddingRequired != 0) {
        for (int i = 0; i < 8 - paddingRequired; i++) {
            output->push_back(0x00);
        }
    }
}

std::vector<uint8_t> padToEightByteMultiple(const std::vector<uint8_t>& input) {
    std::vector<uint8_t> output;
    padToEightByteMultipleHelper(&output, input);
    return output;
}

SecureVector<uint8_t> padToEightByteMultiple(const SecureVector<uint8_t>& input) {
    SecureVector<uint8_t> output;
    padToEightByteMultipleHelper(&(*output), *input);
    return output;
}

size_t encodeAttribute(struct EncodingContext* ctx,
                       std::vector<uint8_t>* attributeName,
                       ItemType attributeType,
                       const std::vector<uint8_t>& attributeData) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    // The starting value corresponds to the size of the various tags that
    // are always present.
    size_t attributeSizeInt = 16;

    // For non-structures, the Item Length is the number of bytes excluding
    // the padding bytes. However, the overall structure's size includes the padding.
    std::vector<uint8_t> nameSize = convertIntToBigEndianArray(attributeName->size());
    std::vector<uint8_t> paddedName = padToEightByteMultiple(*attributeName);
    attributeSizeInt += paddedName.size();

    std::vector<uint8_t> dataSize = convertIntToBigEndianArray(attributeData.size());
    std::vector<uint8_t> processedAttributeData;
    // Figure out whether we need to do any extra processing to the attributeData,
    // then do it, and update attributeSizeInt for overall structure size.
    if (attributeType == ItemType::integer || attributeType == ItemType::enumeration) {
        // integers and enumerations must be padded with 4 bytes.
        processedAttributeData = padToEightByteMultiple(attributeData);
        attributeSizeInt += processedAttributeData.size();
    } else {
        // not supported
        fassertFailed(4049);
    }
    std::vector<uint8_t> attributeSize = convertIntToBigEndianArray(attributeSizeInt);

    //  Tag: Attribute (0x420008), Type: Structure (0x01), Data: Size
    pl.insert(pl.end(), std::begin(attributeTag), std::end(attributeTag));
    pl.push_back(static_cast<uint8_t>(ItemType::structure));
    pl.insert(pl.end(), std::begin(attributeSize), std::end(attributeSize));

    //  Tag: Attribute Name (0x42000A), Type: Text String (0x07), Size, Data: attributeName
    pl.insert(pl.end(), std::begin(attributeNameTag), std::end(attributeNameTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));
    pl.insert(pl.end(), std::begin(nameSize), std::end(nameSize));
    pl.insert(pl.end(), std::begin(paddedName), std::end(paddedName));

    //  Tag: Attribute Value (0x42000B), Type: attributeType, Size, Data: attributeData
    pl.insert(pl.end(), std::begin(attributeValueTag), std::end(attributeValueTag));
    pl.push_back(static_cast<uint8_t>(attributeType));
    pl.insert(pl.end(), std::begin(dataSize), std::end(dataSize));
    pl.insert(pl.end(), std::begin(processedAttributeData), std::end(processedAttributeData));

    return pl.size() - startSize;
}

size_t encodeActivatePayload(struct EncodingContext* ctx,
                             const ActivateKMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    std::vector<uint8_t> paddedUUID = padToEightByteMultiple(requestParams.uid);
    std::vector<uint8_t> uidSize = convertIntToBigEndianArray(requestParams.uid.size());

    // Tag: Unique Identifier, Type: Text String, Size of UUID, UUID
    pl.insert(pl.end(), std::begin(uniqueIdentifierTag), std::end(uniqueIdentifierTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));

    pl.insert(pl.end(), std::begin(uidSize), std::end(uidSize));
    pl.insert(pl.end(), std::begin(paddedUUID), std::end(paddedUUID));

    return pl.size() - startSize;
}

size_t encodeCreatePayload(struct EncodingContext* ctx,
                           const CreateKMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    // Tag: Object Type, Type: Enumeration, Enumeration Size, Data: Symmetric Key
    pl.insert(pl.end(), std::begin(objectTypeTag), std::end(objectTypeTag));
    pl.push_back(static_cast<uint8_t>(ItemType::enumeration));
    pl.insert(pl.end(), std::begin(enumerationByteLength), std::end(enumerationByteLength));
    pl.insert(
        pl.end(), std::begin(symmetricKeyObjectTypeArray), std::end(symmetricKeyObjectTypeArray));
    pl.insert(pl.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

    // Tag: Template-Attribute, Type: Structure, Data: Size
    pl.insert(pl.end(), std::begin(templateAttributeTag), std::end(templateAttributeTag));
    pl.push_back(static_cast<uint8_t>(ItemType::structure));
    size_t templateSizeIdx = pl.size();
    pl.resize(pl.size() + 4);

    size_t templateSizeInt = 0;

    std::vector<uint8_t> cryptoAlgorithmTextStringVector(std::begin(cryptoAlgorithmTextString),
                                                         std::end(cryptoAlgorithmTextString));
    templateSizeInt += encodeAttribute(ctx,
                                       &cryptoAlgorithmTextStringVector,
                                       ItemType::enumeration,
                                       requestParams.cryptoAlgorithm);

    std::vector<uint8_t> cryptoLengthTextStringVector(std::begin(cryptoLengthTextString),
                                                      std::end(cryptoLengthTextString));
    templateSizeInt += encodeAttribute(
        ctx, &cryptoLengthTextStringVector, ItemType::integer, requestParams.cryptoLength);

    std::vector<uint8_t> cryptoUsageMaskTextStringVector(std::begin(cryptoUsageMaskTextString),
                                                         std::end(cryptoUsageMaskTextString));
    templateSizeInt += encodeAttribute(
        ctx, &cryptoUsageMaskTextStringVector, ItemType::integer, requestParams.cryptoUsageMask);

    std::vector<uint8_t> templateSizeData = convertIntToBigEndianArray(templateSizeInt);
    std::copy(templateSizeData.begin(), templateSizeData.end(), pl.begin() + templateSizeIdx);

    return pl.size() - startSize;
}

size_t encodeGetPayload(struct EncodingContext* ctx,
                        const GetKMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    std::vector<uint8_t> paddedUUID = padToEightByteMultiple(requestParams.uid);
    // For Text Strings, the Item Length is the number of bytes excluding the padding bytes.
    std::vector<uint8_t> uidSize = convertIntToBigEndianArray(requestParams.uid.size());

    // Tag: Unique Identifier, Type: Text String, Size of UUID, UUID
    pl.insert(pl.end(), std::begin(uniqueIdentifierTag), std::end(uniqueIdentifierTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));

    pl.insert(pl.end(), std::begin(uidSize), std::end(uidSize));
    pl.insert(pl.end(), std::begin(paddedUUID), std::end(paddedUUID));

    return pl.size() - startSize;
}

size_t encodeEncryptPayload(struct EncodingContext* ctx,
                            const EncryptKMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    std::vector<uint8_t> paddedUUID = padToEightByteMultiple(requestParams.uid);
    std::vector<uint8_t> uidSize = convertIntToBigEndianArray(requestParams.uid.size());

    // Tag: Unique Identifier, Type: Text String, Size of UUID, UUID
    pl.insert(pl.end(), std::begin(uniqueIdentifierTag), std::end(uniqueIdentifierTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));

    pl.insert(pl.end(), std::begin(uidSize), std::end(uidSize));
    pl.insert(pl.end(), std::begin(paddedUUID), std::end(paddedUUID));

    // Note that this does add an extra copy, but we are copying anyways, and our data is small
    // enough that it shouldn't matter.
    SecureVector<uint8_t> paddedData = padToEightByteMultiple(requestParams.data);
    std::vector<uint8_t> dataSize = convertIntToBigEndianArray(requestParams.data->size());

    // Tag: Data, Type: Byte String, Size of data, data to be encrypted
    pl.insert(pl.end(), std::begin(dataTag), std::end(dataTag));
    pl.push_back(static_cast<uint8_t>(ItemType::byteString));

    pl.insert(pl.end(), std::begin(dataSize), std::end(dataSize));
    pl.insert(pl.end(), paddedData->begin(), paddedData->end());

    return pl.size() - startSize;
}

size_t encodeDecryptPayload(struct EncodingContext* ctx,
                            const DecryptKMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    std::vector<uint8_t> paddedUUID = padToEightByteMultiple(requestParams.uid);
    std::vector<uint8_t> uidSize = convertIntToBigEndianArray(requestParams.uid.size());

    // Tag: Unique Identifier, Type: Text String, Size of UUID, UUID
    pl.insert(pl.end(), std::begin(uniqueIdentifierTag), std::end(uniqueIdentifierTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));

    pl.insert(pl.end(), std::begin(uidSize), std::end(uidSize));
    pl.insert(pl.end(), std::begin(paddedUUID), std::end(paddedUUID));

    std::vector<uint8_t> paddedData = padToEightByteMultiple(requestParams.data);
    std::vector<uint8_t> dataSize = convertIntToBigEndianArray(requestParams.data.size());

    // Tag: Data, Type: Byte String, Size of data, data to be decrypted
    pl.insert(pl.end(), std::begin(dataTag), std::end(dataTag));
    pl.push_back(static_cast<uint8_t>(ItemType::byteString));

    pl.insert(pl.end(), std::begin(dataSize), std::end(dataSize));
    pl.insert(pl.end(), std::begin(paddedData), std::end(paddedData));

    if (!requestParams.iv.empty()) {
        std::vector<uint8_t> paddedIV = padToEightByteMultiple(requestParams.iv);
        std::vector<uint8_t> ivSize = convertIntToBigEndianArray(requestParams.iv.size());

        // Tag: IV/Counter/Nonce, Type: Byte String, Size of IV, IV
        pl.insert(pl.end(), std::begin(ivTag), std::end(ivTag));
        pl.push_back(static_cast<uint8_t>(ItemType::byteString));

        pl.insert(pl.end(), std::begin(ivSize), std::end(ivSize));
        pl.insert(pl.end(), std::begin(paddedIV), std::end(paddedIV));
    }

    return pl.size() - startSize;
}

size_t encodeGetAttributesPayload(struct EncodingContext* ctx,
                                  const GetAttributesKMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    std::vector<uint8_t> paddedUUID = padToEightByteMultiple(requestParams.uid);
    // For Text Strings, the Item Length is the number of bytes excluding the padding bytes.
    std::vector<uint8_t> uidSize = convertIntToBigEndianArray(requestParams.uid.size());

    // Tag: Unique Identifier, Type: Text String, Size of UUID, UUID
    pl.insert(pl.end(), std::begin(uniqueIdentifierTag), std::end(uniqueIdentifierTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));

    pl.insert(pl.end(), std::begin(uidSize), std::end(uidSize));
    pl.insert(pl.end(), std::begin(paddedUUID), std::end(paddedUUID));

    // Tag: Attribute Name, Type: Text String, Size of attribute name, attribute name size
    std::vector<uint8_t> nameSize = convertIntToBigEndianArray(requestParams.attributeName.size());
    std::vector<uint8_t> paddedName = padToEightByteMultiple(requestParams.attributeName);

    pl.insert(pl.end(), std::begin(attributeNameTag), std::end(attributeNameTag));
    pl.push_back(static_cast<uint8_t>(ItemType::textString));
    pl.insert(pl.end(), std::begin(nameSize), std::end(nameSize));
    pl.insert(pl.end(), std::begin(paddedName), std::end(paddedName));

    return pl.size() - startSize;
}

size_t encodeOpAndRequestPayload(struct EncodingContext* ctx,
                                 const KMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    // Tag: Operation, Type: Enumeration, Enumeration Size, Data: OperationType
    pl.insert(pl.end(), std::begin(operationTag), std::end(operationTag));
    pl.push_back(static_cast<uint8_t>(ItemType::enumeration));
    pl.insert(pl.end(), std::begin(enumerationByteLength), std::end(enumerationByteLength));
    pl.insert(
        pl.end(), std::begin(requestParams.operationType), std::end(requestParams.operationType));
    pl.insert(pl.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

    // Tag: RequestPayload, Type: Structure, Data: Size
    pl.insert(pl.end(), std::begin(requestPayloadTag), std::end(requestPayloadTag));
    pl.push_back(static_cast<uint8_t>(ItemType::structure));
    size_t requestPayloadSizeIdx = pl.size();
    pl.resize(pl.size() + 4);

    size_t requestPayloadSize;
    if (std::memcmp(requestParams.operationType,
                    activateOperationTypeArray,
                    sizeof(activateOperationTypeArray)) == 0) {
        requestPayloadSize = encodeActivatePayload(
            ctx, static_cast<const ActivateKMIPRequestParameters&>(requestParams));
    } else if (std::memcmp(requestParams.operationType,
                           createOperationTypeArray,
                           sizeof(createOperationTypeArray)) == 0) {
        requestPayloadSize = encodeCreatePayload(
            ctx, static_cast<const CreateKMIPRequestParameters&>(requestParams));
    } else if (std::memcmp(requestParams.operationType,
                           getOperationTypeArray,
                           sizeof(getOperationTypeArray)) == 0) {
        requestPayloadSize =
            encodeGetPayload(ctx, static_cast<const GetKMIPRequestParameters&>(requestParams));
    } else if (std::memcmp(requestParams.operationType,
                           discoverVersionsOperationTypeArray,
                           sizeof(getOperationTypeArray)) == 0) {
        // Discover Versions has no request payload.
        requestPayloadSize = 0;
    } else if (std::memcmp(requestParams.operationType,
                           encryptOperationTypeArray,
                           sizeof(encryptOperationTypeArray)) == 0) {
        requestPayloadSize = encodeEncryptPayload(
            ctx, static_cast<const EncryptKMIPRequestParameters&>(requestParams));
    } else if (std::memcmp(requestParams.operationType,
                           decryptOperationTypeArray,
                           sizeof(decryptOperationTypeArray)) == 0) {
        requestPayloadSize = encodeDecryptPayload(
            ctx, static_cast<const DecryptKMIPRequestParameters&>(requestParams));
    } else if (std::memcmp(requestParams.operationType,
                           getAttributesOperationTypeArray,
                           sizeof(getAttributesOperationTypeArray)) == 0) {
        requestPayloadSize = encodeGetAttributesPayload(
            ctx, static_cast<const GetAttributesKMIPRequestParameters&>(requestParams));
    } else {
        MONGO_UNREACHABLE;
    }
    std::vector<uint8_t> requestPayloadSizeData = convertIntToBigEndianArray(requestPayloadSize);
    std::copy(requestPayloadSizeData.begin(),
              requestPayloadSizeData.end(),
              pl.begin() + requestPayloadSizeIdx);
    return pl.size() - startSize;
}

size_t encodeBatchItem(struct EncodingContext* ctx, const KMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    // Tag: Batch Item, Type: Structure, Data: Size
    pl.insert(pl.end(), std::begin(batchItemTag), std::end(batchItemTag));
    pl.push_back(static_cast<uint8_t>(ItemType::structure));
    size_t requestSizeIdx = pl.size();
    pl.resize(pl.size() + 4);

    // Operation Type information and Request Payload
    size_t requestSize = encodeOpAndRequestPayload(ctx, requestParams);
    std::vector<uint8_t> requestSizeData = convertIntToBigEndianArray(requestSize);
    std::copy(requestSizeData.begin(), requestSizeData.end(), pl.begin() + requestSizeIdx);
    return pl.size() - startSize;
}

size_t encodeProtocolVersion(struct EncodingContext* ctx,
                             const KMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();

    std::vector<uint8_t> majorVersion =
        convertIntToBigEndianArray(requestParams.protocolVersion[0]);
    std::vector<uint8_t> minorVersion =
        convertIntToBigEndianArray(requestParams.protocolVersion[1]);

    // Tag: Protocol Version Major, Type: Integer, Integer Size, Data: ...
    pl.insert(pl.end(), std::begin(protocolVersionMajorTag), std::end(protocolVersionMajorTag));
    pl.push_back(static_cast<uint8_t>(ItemType::integer));
    pl.insert(pl.end(), std::begin(integerByteLength), std::end(integerByteLength));
    pl.insert(pl.end(), std::begin(majorVersion), std::end(majorVersion));
    pl.insert(pl.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

    // Tag: Protocol Version Minor, Type: Integer, Integer Size, Data: ...
    pl.insert(pl.end(), std::begin(protocolVersionMinorTag), std::end(protocolVersionMinorTag));
    pl.push_back(static_cast<uint8_t>(ItemType::integer));
    pl.insert(pl.end(), std::begin(integerByteLength), std::end(integerByteLength));
    pl.insert(pl.end(), std::begin(minorVersion), std::end(minorVersion));
    pl.insert(pl.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

    return pl.size() - startSize;
}

size_t encodeRequestHeader(struct EncodingContext* ctx,
                           const KMIPRequestParameters& requestParams) {
    auto& pl = *ctx->payload;
    size_t startSize = pl.size();
    // Tag: Protocol Version, Type: Structure, Data: Size
    pl.insert(pl.end(), std::begin(protocolVersionTag), std::end(protocolVersionTag));
    pl.push_back(static_cast<uint8_t>(ItemType::structure));
    size_t versionSizeIdx = pl.size();
    pl.resize(pl.size() + 4);

    size_t versionSize = encodeProtocolVersion(ctx, requestParams);
    std::vector<uint8_t> versionSizeData = convertIntToBigEndianArray(versionSize);

    std::copy(versionSizeData.begin(), versionSizeData.end(), pl.begin() + versionSizeIdx);

    // Tag: Batch Count, Type: Integer, Integer Size, Data: Count (1)
    std::vector<uint8_t> batchCount = convertIntToBigEndianArray(1);
    pl.insert(pl.end(), std::begin(batchCountTag), std::end(batchCountTag));
    pl.push_back(static_cast<uint8_t>(ItemType::integer));
    pl.insert(pl.end(), std::begin(integerByteLength), std::end(integerByteLength));
    pl.insert(pl.end(), std::begin(batchCount), std::end(batchCount));
    pl.insert(pl.end(), std::begin(fourBytePadding), std::end(fourBytePadding));

    return pl.size() - startSize;
}
}  // namespace

std::vector<uint8_t> convertIntToBigEndianArray(uint32_t input) {
    std::vector<uint8_t> output;
    output.push_back((input >> 24) & 0xff);
    output.push_back((input >> 16) & 0xff);
    output.push_back((input >> 8) & 0xff);
    output.push_back(input & 0xff);
    return output;
}

SecureVector<uint8_t> encodeKMIPRequest(const KMIPRequestParameters& requestParams) {
    struct EncodingContext ctx;
    auto& msg = *ctx.payload;

    // Tag: Request Message, Type: Structure, Data: Size
    msg.insert(msg.end(), std::begin(requestMessageTag), std::end(requestMessageTag));
    msg.push_back(static_cast<uint8_t>(ItemType::structure));
    // skip slot for length
    size_t requestSizeIdx = msg.size();
    msg.resize(msg.size() + 4);

    // Tag: Request Header, Type: Structure, Data: Size
    msg.insert(msg.end(), std::begin(requestHeaderTag), std::end(requestHeaderTag));
    msg.push_back(static_cast<uint8_t>(ItemType::structure));
    size_t headerSizeIdx = msg.size();
    msg.resize(msg.size() + 4);

    // The request header.
    size_t headerSize = encodeRequestHeader(&ctx, requestParams);
    std::vector<uint8_t> headerSizeData = convertIntToBigEndianArray(headerSize);

    // The batch item.
    // Request size is the size of the header and the batch items.
    // The +8 is because the header for the request header data is not included in either size.
    size_t requestSize = encodeBatchItem(&ctx, requestParams) + 8 + headerSize;
    std::vector<uint8_t> requestSizeData = convertIntToBigEndianArray(requestSize);

    std::copy(headerSizeData.begin(), headerSizeData.end(), msg.begin() + headerSizeIdx);
    std::copy(requestSizeData.begin(), requestSizeData.end(), msg.begin() + requestSizeIdx);

    return ctx.payload;
}

}  // namespace kmip
}  // namespace mongo
