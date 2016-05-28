/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "kmip_response.h"

#include <string>
#include <time.h>
#include <vector>

#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_endian.h"
#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace kmip {

StatusWith<KMIPResponse> KMIPResponse::create(const char* responseMessage, size_t len) {
    KMIPResponse response;
    ConstDataRangeCursor cdrc(responseMessage, responseMessage + len);
    Status parseResult = response._parseResponse(&cdrc);
    if (!parseResult.isOK()) {
        return parseResult;
    }
    return std::move(response);
}

KMIPResponse::KMIPResponse() {}

/**
 * Consume a 3 byte tag and compare with the expected input
 * Parse and return the length of the block denoted by the tag
 */
StatusWith<size_t> KMIPResponse::_parseTag(ConstDataRangeCursor* cdrc,
                                           const uint8_t tag[],
                                           ItemType itemType,
                                           const std::string& tagName) {
    /**
     * Verify the tag name. Skip any unexpected tags in an attempt
     * to be forwards compatible with newer versions of the protocol.
     */
    do {
        const char* data = cdrc->data();
        Status adv = cdrc->advance(4);
        if (!adv.isOK()) {
            // Reached the end of the buffer without finding the tag
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "KMIP response message invalid, looking for " << tagName
                                        << " tag. "
                                        << adv.reason());
        }

        // Read out the length of the section labeled by the tag
        auto swLen = cdrc->readAndAdvance<BigEndian<uint32_t>>();
        if (!swLen.isOK()) {
            return swLen.getStatus();
        }
        size_t len = swLen.getValue();

        if (memcmp(data, tag, 3) == 0) {
            if (data[3] != static_cast<char>(itemType)) {
                return Status(ErrorCodes::FailedToParse,
                              str::stream() << "Response message was malformed: expected "
                                            << tagName
                                            << " to be of type "
                                            << static_cast<uint8_t>(itemType)
                                            << " but found "
                                            << static_cast<uint8_t>(data[3]));
            }
            // Success
            return static_cast<size_t>(len);
        }

        // Advance to the next tag, handle 8 byte aligned padding
        if (len > std::numeric_limits<size_t>::max() - 8) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Response message was malformed, invalid length " << len
                                        << " found.");
        }
        adv = cdrc->advance(len + (8 - (len % 8)) % 8);
        if (!adv.isOK()) {
            return adv;
        }
    } while (1);
}

StatusWith<std::tuple<uint32_t, uint32_t>> KMIPResponse::_parseProtocolVersion(
    ConstDataRangeCursor* cdrc) {
    StatusWith<size_t> swTag =
        _parseTag(cdrc, protocolVersionTag, ItemType::structure, "protocol version");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    auto swMajor =
        _parseInteger(cdrc, protocolVersionMajorTag, ItemType::integer, "protocol version major");
    if (!swMajor.isOK()) {
        return swMajor.getStatus();
    }

    auto swMinor =
        _parseInteger(cdrc, protocolVersionMinorTag, ItemType::integer, "protocol version minor");
    if (!swMinor.isOK()) {
        return swMinor.getStatus();
    }

    return std::make_tuple(static_cast<uint32_t>(swMajor.getValue()),
                           static_cast<uint32_t>(swMinor.getValue()));
}

StatusWith<time_t> KMIPResponse::_parseTimeStamp(ConstDataRangeCursor* cdrc) {
    StatusWith<size_t> swTag = _parseTag(cdrc, timeStampTag, ItemType::dateTime, "timestamp");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    auto swTimeStamp = cdrc->readAndAdvance<BigEndian<uint64_t>>();
    if (!swTimeStamp.isOK()) {
        return swTimeStamp.getStatus();
    }
    return static_cast<time_t>(swTimeStamp.getValue());
}

Status KMIPResponse::_parseResponseHeader(ConstDataRangeCursor* cdrc) {
    StatusWith<size_t> swTag =
        _parseTag(cdrc, responseHeaderTag, ItemType::structure, "response header");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    StatusWith<std::tuple<uint32_t, uint32_t>> swProtocolVersion = _parseProtocolVersion(cdrc);
    if (!swProtocolVersion.isOK()) {
        return swProtocolVersion.getStatus();
    }
    _protocolVersion[0] = std::get<0>(swProtocolVersion.getValue());
    _protocolVersion[1] = std::get<1>(swProtocolVersion.getValue());

    StatusWith<time_t> swTimeStamp = _parseTimeStamp(cdrc);
    if (!swTimeStamp.isOK()) {
        return swTimeStamp.getStatus();
    }
    _timeStamp = swTimeStamp.getValue();

    auto swBatchCount = _parseInteger(cdrc, batchCountTag, ItemType::integer, "batch count");
    if (!swBatchCount.isOK()) {
        return swBatchCount.getStatus();
    }
    _batchCount = swBatchCount.getValue();

    return Status::OK();
}

StatusWith<std::string> KMIPResponse::_parseUIDPayload(ConstDataRangeCursor* cdrc) {
    auto swObjectType = _parseInteger(cdrc, objectTypeTag, ItemType::enumeration, "object type");
    if (!swObjectType.isOK()) {
        warning() << "Object type missing from batch item, attempting to parse anyway";
    }

    return _parseString(cdrc, uniqueIdentifierTag, "UID");
}

StatusWith<std::unique_ptr<SymmetricKey>> KMIPResponse::_parseSymmetricKeyPayload(
    ConstDataRangeCursor* cdrc) {
    auto swObjectType = _parseInteger(cdrc, objectTypeTag, ItemType::enumeration, "object type");
    if (!swObjectType.isOK()) {
        return swObjectType.getStatus();
    }

    if (swObjectType.getValue() != symmetricKeyObjectType) {
        return Status(ErrorCodes::FailedToParse,
                      "Response message was malformed: does not contain symmetric key.");
    }

    auto swUID = _parseString(cdrc, uniqueIdentifierTag, "UID");
    if (!swUID.isOK()) {
        return swUID.getStatus();
    }
    _uid = swUID.getValue();

    StatusWith<size_t> swTag =
        _parseTag(cdrc, symmetricKeyTag, ItemType::structure, "symmetric key");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    swTag = _parseTag(cdrc, keyBlockTag, ItemType::structure, "key block");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    auto swKeyFormat =
        _parseInteger(cdrc, keyFormatTypeTag, ItemType::enumeration, "key format type");
    if (!swKeyFormat.isOK()) {
        return swKeyFormat.getStatus();
    }
    uint8_t keyFormat = static_cast<uint8_t>(swKeyFormat.getValue());
    if (keyFormat != keyFormatRaw && keyFormat != keyFormatSymmetricKey) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "Unexpected key format " << keyFormat);
    }

    swTag = _parseTag(cdrc, keyValueTag, ItemType::structure, "key value");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    swTag = _parseTag(cdrc, keyMaterialTag, ItemType::byteString, "key material");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }
    uint32_t paddedKeyLen = swTag.getValue();

    // This is safe since the max length 32 is a multiple of 8 and won't be
    // padded
    if (paddedKeyLen < crypto::minKeySize || paddedKeyLen > crypto::maxKeySize) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "Response message was malformed: unexpected "
                                       "symmetric key length: "
                                    << paddedKeyLen);
    }

    const uint8_t* key = reinterpret_cast<const uint8_t*>(cdrc->data());

    // Verify that we can read paddedKeyLen bytes
    Status adv = cdrc->advance(paddedKeyLen);
    if (!adv.isOK()) {
        return adv;
    }

    auto swAlgorithm = _parseInteger(
        cdrc, cryptographicAlgorithmTag, ItemType::enumeration, "cryptographic algorithm");
    if (!swAlgorithm.isOK()) {
        return swAlgorithm.getStatus();
    }
    uint32_t algorithm = static_cast<uint32_t>(swAlgorithm.getValue());

    auto swKeySize =
        _parseInteger(cdrc, cryptographicLengthTag, ItemType::integer, "cryptographic length");
    if (!swKeySize.isOK()) {
        return swKeySize.getStatus();
    }
    size_t keySize = static_cast<size_t>(swKeySize.getValue()) / 8;
    if (keySize > paddedKeyLen) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "Symmetric key size mismatch, " << keySize << " > "
                                    << paddedKeyLen);
    }

    return stdx::make_unique<SymmetricKey>(key, keySize, algorithm, _uid, 0);
}

Status KMIPResponse::_parseBatchItem(ConstDataRangeCursor* cdrc) {
    StatusWith<size_t> swTag = _parseTag(cdrc, batchItemTag, ItemType::structure, "batch item");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    auto swOpType = _parseInteger(cdrc, operationTag, ItemType::enumeration, "operation");
    if (!swOpType.isOK()) {
        return swOpType.getStatus();
    }
    _opType = static_cast<uint8_t>(swOpType.getValue());

    auto swResultStatus =
        _parseInteger(cdrc, resultStatusTag, ItemType::enumeration, "result status");
    if (!swResultStatus.isOK()) {
        return swResultStatus.getStatus();
    }
    _resultStatus = static_cast<uint32_t>(swResultStatus.getValue());

    // Parse error message on failure
    if (_resultStatus != statusSuccess) {
        return _parseFailure(cdrc);
    }

    swTag = _parseTag(cdrc, responsePayloadTag, ItemType::structure, "response payload");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    switch (_opType) {
        case static_cast<uint8_t>(OperationType::create): {
            StatusWith<std::string> swUID = _parseUIDPayload(cdrc);
            if (!swUID.isOK()) {
                return swUID.getStatus();
            }
            _uid = swUID.getValue();
            break;
        }
        case static_cast<uint8_t>(OperationType::get): {
            StatusWith<std::unique_ptr<SymmetricKey>> swSymmetricKey =
                _parseSymmetricKeyPayload(cdrc);
            if (!swSymmetricKey.isOK()) {
                return swSymmetricKey.getStatus();
            }
            _symmetricKey = std::move(swSymmetricKey.getValue());
            break;
        }
        default:
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Response message was malformed: unknown operation type "
                                        << _opType);
    }

    return Status::OK();
}

Status KMIPResponse::_parseFailure(ConstDataRangeCursor* cdrc) {
    auto swResultReason =
        _parseInteger(cdrc, resultReasonTag, ItemType::enumeration, "result reason");
    if (!swResultReason.isOK()) {
        return swResultReason.getStatus();
    }
    _resultReason = static_cast<uint32_t>(swResultReason.getValue());

    StatusWith<std::string> swResMsg = _parseString(cdrc, resultMessageTag, "result message");
    if (!swResMsg.isOK()) {
        return swResMsg.getStatus();
    }
    _resultMsg = swResMsg.getValue();

    return Status::OK();
}

Status KMIPResponse::_parseResponse(ConstDataRangeCursor* cdrc) {
    StatusWith<size_t> swTag =
        _parseTag(cdrc, responseMessageTag, ItemType::structure, "response message");
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }
    Status status = _parseResponseHeader(cdrc);
    if (!status.isOK()) {
        return status;
    }

    // Read the header length
    if (_batchCount > 1) {
        warning() << "KMIP Response contains " << _batchCount << " items, expected 1.";
    }
    // Parse the first (and only) batch item
    return _parseBatchItem(cdrc);
}

StatusWith<std::string> KMIPResponse::_parseString(ConstDataRangeCursor* cdrc,
                                                   const uint8_t tag[],
                                                   const std::string& tagName) {
    StatusWith<size_t> swTag = _parseTag(cdrc, tag, ItemType::textString, tagName);
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }
    size_t len = swTag.getValue();

    /**
     * Advance to the next byte aligned value, if uidLen is a multiple of 8
     * there is no padding so we don't need to advance.
     */
    const char* data = cdrc->data();
    if (len > std::numeric_limits<size_t>::max() - 8) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "Response message was malformed, invalid length " << len
                                    << " found.");
    }
    Status adv = cdrc->advance(len + (8 - (len % 8)) % 8);
    if (!adv.isOK()) {
        return adv;
    }

    return std::string(data, len);
}

StatusWith<std::uint32_t> KMIPResponse::_parseInteger(ConstDataRangeCursor* cdrc,
                                                      const uint8_t tag[],
                                                      ItemType itemType,
                                                      const std::string& tagName) {
    StatusWith<size_t> swTag = _parseTag(cdrc, tag, itemType, tagName);
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    auto swUInt32 = cdrc->readAndAdvance<BigEndian<uint32_t>>();
    if (!swUInt32.isOK()) {
        return swUInt32.getStatus();
    }

    // Skip padding
    Status adv = cdrc->advance(4);
    if (!adv.isOK()) {
        return adv;
    }
    return static_cast<uint32_t>(swUInt32.getValue());
}
}  // namespace kmip
}  // namespace mongo
