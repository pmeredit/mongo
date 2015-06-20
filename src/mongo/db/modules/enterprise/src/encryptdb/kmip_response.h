/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <tuple>

#include "kmip_consts.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/platform/cstdint.h"
#include "symmetric_key.h"

namespace mongo {
class ConstDataRangeCursor;
template <typename T>
class StatusWith;

namespace kmip {

class KMIPResponse {
    MONGO_DISALLOW_COPYING(KMIPResponse);

public:
    /**
     * Factory function to parse a KMIP response. Parsing errors are indicated
     * in the status portion of the return value.
     */
    static StatusWith<KMIPResponse> create(const char* responseMessage, size_t len);

#if defined(_MSC_VER) && _MSC_VER < 1900
    KMIPResponse(KMIPResponse&&);
    KMIPResponse& operator=(KMIPResponse&&);
#else
    KMIPResponse(KMIPResponse&&) = default;
    KMIPResponse& operator=(KMIPResponse&&) = default;
#endif

    /**
     * Gets the protocol version for the parsed response.
     */
    const std::tuple<uint32_t, uint32_t> getProtocolVersion() const {
        return std::make_tuple(_protocolVersion[0], _protocolVersion[1]);
    }

    /**
     * Gets the timestamp for the parsed response.
     */
    time_t getTimeStamp() const {
        return _timeStamp;
    }

    /**
     * Gets the KMIP operations type for the parsed response.
     */
    uint32_t getOpType() const {
        return _opType;
    }

    /**
     * Gets the KMIP result reason and result msg for the parsed response
     */
    uint32_t getResultStatus() const {
        return _resultStatus;
    }
    uint32_t getResultReason() const {
        return _resultReason;
    }
    std::string getResultMsg() const {
        return _resultMsg;
    }

    /**
     * Gets the symmetric key from the parsed message
     */
    std::unique_ptr<SymmetricKey> getSymmetricKey() {
        return std::move(_symmetricKey);
    }

    /**
     * Gets the unique key identifier from the parsed message
     */
    std::string getUID() const {
        return _uid;
    }

private:
    KMIPResponse();

    /**
     * Each of the _parseXXX methods below behaves in a similar fashion.
     *
     * They take in a ConstDataRangeCursor cdrc representing the response buffer.
     * Each method consumes its corresponding message tag and advances the
     * cursor to the next item.
     */
    Status _parseResponse(ConstDataRangeCursor* cdrc);
    StatusWith<size_t> _parseTag(ConstDataRangeCursor* cdrc,
                                 const uint8_t tag[],
                                 ItemType itemType,
                                 const std::string& tagName);
    Status _parseResponseHeader(ConstDataRangeCursor* cdrc);
    Status _parseFailure(ConstDataRangeCursor* cdrc);
    StatusWith<std::tuple<uint32_t, uint32_t>> _parseProtocolVersion(ConstDataRangeCursor* cdrc);
    StatusWith<time_t> _parseTimeStamp(ConstDataRangeCursor* cdrc);
    Status _parseBatchItem(ConstDataRangeCursor* cdrc);
    StatusWith<std::string> _parseUIDPayload(ConstDataRangeCursor* cdrc);
    StatusWith<std::unique_ptr<SymmetricKey>> _parseSymmetricKeyPayload(ConstDataRangeCursor* cdrc);
    StatusWith<std::string> _parseString(ConstDataRangeCursor* cdrc,
                                         const uint8_t tag[],
                                         const std::string& tagName);
    StatusWith<uint32_t> _parseInteger(ConstDataRangeCursor* cdrc,
                                       const uint8_t tag[],
                                       ItemType itemType,
                                       const std::string& tagName);

    /**
     * KMIP response message parameters
     */
    uint32_t _resultStatus;
    uint32_t _resultReason;
    std::string _resultMsg;
    uint32_t _protocolVersion[2];
    time_t _timeStamp;
    uint8_t _opType;
    uint32_t _batchCount;

    std::unique_ptr<SymmetricKey> _symmetricKey;
    std::string _uid;
};
}  // namespace kmip
}  // namespace mongo
