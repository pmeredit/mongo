/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <cstdint>
#include <tuple>

#include "kmip_consts.h"
#include "mongo/base/status.h"
#include "mongo/crypto/symmetric_key.h"

namespace mongo {
class ConstDataRangeCursor;
template <typename T>
class StatusWith;

namespace kmip {

class KMIPResponse {
    KMIPResponse(const KMIPResponse&) = delete;
    KMIPResponse& operator=(const KMIPResponse&) = delete;

public:
    struct Attribute {
        std::string name;
        uint32_t index;
        // The value field varies depending on the type of attribute.
        // State attribute value is an enum describing the state, for others it can be Date-Time
        // or other type of struct.
        union {
            StateName stateEnum;
        } value;
    };

    /**
     * Factory function to parse a KMIP response. Parsing errors are indicated
     * in the status portion of the return value.
     */
    static StatusWith<KMIPResponse> create(const SecureVector<char>& responseMessage);

    KMIPResponse(KMIPResponse&&) = default;
    KMIPResponse& operator=(KMIPResponse&&) = default;
    /**
     * Gets the protocol version for the parsed response.
     */
    std::tuple<uint32_t, uint32_t> getProtocolVersion() const {
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
    const SecureString& getResultMsg() const {
        return _resultMsg;
    }

    /**
     * Gets the symmetric key from the parsed message
     */
    std::unique_ptr<SymmetricKey> getSymmetricKey() {
        return std::move(_symmetricKey);
    }

    /**
     * Gets the data from the parsed message
     */
    const SecureVector<uint8_t>& getData() const {
        return _data;
    }

    /**
     * Gets the IV from the parsed message
     */
    const std::vector<uint8_t>& getIV() const {
        return _iv;
    }

    /**
     * Gets the unique key identifier from the parsed message
     */
    const std::string& getUID() const {
        return _uid;
    }

    /**
     * Gets the attribute struct from the parsed message
     */
    const Attribute& getAttribute() const {
        return _attribute;
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
    StatusWith<SecureString> _parseString(ConstDataRangeCursor* cdrc,
                                          const uint8_t tag[],
                                          const std::string& tagName);
    StatusWith<SecureVector<uint8_t>> _parseByteString(ConstDataRangeCursor* cdrc,
                                                       const uint8_t tag[],
                                                       const std::string& tagName);
    StatusWith<uint32_t> _parseInteger(ConstDataRangeCursor* cdrc,
                                       const uint8_t tag[],
                                       ItemType itemType,
                                       const std::string& tagName);
    StatusWith<Attribute> _parseAttribute(ConstDataRangeCursor* cdrc,
                                          const uint8_t tag[],
                                          const std::string& tagName);
    /**
     * KMIP response message parameters
     */
    uint32_t _resultStatus;
    uint32_t _resultReason;
    SecureString _resultMsg;
    uint32_t _protocolVersion[2];
    time_t _timeStamp;
    uint8_t _opType;
    uint32_t _batchCount;

    std::unique_ptr<SymmetricKey> _symmetricKey;
    std::string _uid;
    SecureVector<uint8_t> _data;
    std::vector<uint8_t> _iv;
    Attribute _attribute;
};

}  // namespace kmip
}  // namespace mongo
