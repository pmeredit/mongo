/**
 * Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "kmip_service.h"

#include <vector>

#include "encryption_options.h"
#include "kmip_consts.h"
#include "kmip_request.h"
#include "kmip_response.h"
#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_endian.h"
#include "mongo/base/init.h"
#include "mongo/base/status_with.h"
#include "mongo/stdx/memory.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/log.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/secure_zero_memory.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace kmip {

StatusWith<KMIPService> KMIPService::createKMIPService(const HostAndPort& server,
                                                       const SSLParams& sslKMIPParams) {
    try {
        std::unique_ptr<SSLManagerInterface> sslManager =
            SSLManagerInterface::create(sslKMIPParams, false);
        KMIPService kmipService(server, sslKMIPParams, std::move(sslManager));
        Status status = kmipService._initServerConnection();
        if (!status.isOK()) {
            return status;
        }
        return std::move(kmipService);
    } catch (const DBException& e) {
        return e.toStatus();
    }
}

KMIPService::KMIPService(const HostAndPort& server,
                         const SSLParams& sslKMIPParams,
                         std::unique_ptr<SSLManagerInterface> sslManager)
    : _sslManager(std::move(sslManager)),
      _server(server),
      _socket(stdx::make_unique<Socket>(10, logger::LogSeverity::Log())) {}

StatusWith<std::string> KMIPService::createExternalKey() {
    StatusWith<KMIPResponse> swResponse = _sendRequest(_generateKMIPCreateRequest());
    if (!swResponse.isOK()) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP create key failed: "
                                    << swResponse.getStatus().reason());
    }

    const KMIPResponse& response = swResponse.getValue();
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP create key failed, code: "
                                    << response.getResultReason()
                                    << " error: "
                                    << response.getResultMsg());
    }
    return response.getUID();
}

StatusWith<std::unique_ptr<SymmetricKey>> KMIPService::getExternalKey(const std::string& uid) {
    StatusWith<KMIPResponse> swResponse = _sendRequest(_generateKMIPGetRequest(uid));

    if (!swResponse.isOK()) {
        return swResponse.getStatus();
    }

    KMIPResponse response = std::move(swResponse.getValue());
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP get key '" << uid << "' failed, code: "
                                    << response.getResultReason()
                                    << " error: "
                                    << response.getResultMsg());
    }

    std::unique_ptr<SymmetricKey> key = response.getSymmetricKey();
    if (key->getKeySize() != crypto::sym256KeySize) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP got a key which was " << key->getKeySize() * 8
                                    << " bits long, but a "
                                    << crypto::sym256KeySize * 8
                                    << " bit key is required");
    }
    return std::move(key);
}

Status KMIPService::_initServerConnection() {
    SockAddr server(_server.host().c_str(), _server.port(), AF_UNSPEC);

    if (!server.isValid()) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP server address " << _server.host() << " is invalid.");
    }

    if (!_socket->connect(server)) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Could not connect to KMIP server " << server.toString());
    }

    if (!_socket->secure(_sslManager.get(), _server.host())) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Failed to perform SSL handshake with the KMIP server "
                                    << _server.toString());
    }

    return Status::OK();
}

// Sends a request message to the KMIP server and creates a KMIPResponse.
StatusWith<KMIPResponse> KMIPService::_sendRequest(const std::vector<uint8_t>& request) {
    char resp[2000];

    _socket->send(reinterpret_cast<const char*>(request.data()), request.size(), "KMIP request");
    /**
     *  Read response header on the form:
     *  data[0:2] - tag identifier
     *  data[3]   - tag type
     *  data[4:7] - big endian encoded message body length
     */
    _socket->recv(resp, 8);
    if (memcmp(resp, kmip::responseMessageTag, 3) != 0 ||
        resp[3] != static_cast<char>(ItemType::structure)) {
        return Status(ErrorCodes::FailedToParse,
                      "Expected KMIP response message to start with"
                      "reponse message tag");
    }

    ConstDataRangeCursor cdrc(resp + 4, resp + 8);
    StatusWith<BigEndian<uint32_t>> swBodyLength = cdrc.readAndAdvance<BigEndian<uint32_t>>();
    if (!swBodyLength.isOK()) {
        return swBodyLength.getStatus();
    }

    uint32_t bodyLength = static_cast<uint32_t>(swBodyLength.getValue());
    massert(4044, "KMIP server response is too long", bodyLength <= sizeof(resp) - 8);
    _socket->recv(&resp[8], bodyLength);

    StatusWith<KMIPResponse> swKMIPResponse = KMIPResponse::create(resp, bodyLength + 8);
    secureZeroMemory(resp, bodyLength + 8);
    return swKMIPResponse;
}

std::vector<uint8_t> KMIPService::_generateKMIPGetRequest(const std::string& uid) {
    std::vector<uint8_t> uuid(std::begin(uid), std::end(uid));
    mongo::kmip::GetKMIPRequestParameters getRequestParams(uuid);
    return encodeKMIPRequest(getRequestParams);
}

std::vector<uint8_t> KMIPService::_generateKMIPCreateRequest() {
    std::vector<uint8_t> algorithm(std::begin(aesCryptoAlgorithm), std::end(aesCryptoAlgorithm));
    std::vector<uint8_t> length = convertIntToBigEndianArray(256);
    std::vector<uint8_t> usageMask{
        0x00, 0x00, 0x00, cryptoUsageMaskEncrypt | cryptoUsageMaskDecrypt};

    CreateKMIPRequestParameters createRequestParams(algorithm, length, usageMask);
    return encodeKMIPRequest(createRequestParams);
}
}  // namespace kmip
}  // namespace mongo
