/**
 * Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "kmip_service.h"

#include <memory>
#include <vector>

#include "encryptdb/encryption_options.h"
#include "kmip_consts.h"
#include "kmip_request.h"
#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_endian.h"
#include "mongo/base/init.h"
#include "mongo/base/status_with.h"
#include "mongo/config.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/secure_zero_memory.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {
namespace kmip {

StatusWith<KMIPService> KMIPService::createKMIPService() {
    return createKMIPService(encryptionGlobalParams.kmipParams, sslGlobalParams.sslFIPSMode);
}

StatusWith<KMIPService> KMIPService::createKMIPService(const HostAndPort& server,
                                                       const SSLParams& sslKMIPParams,
                                                       Milliseconds connectTimeout) try {
    std::shared_ptr<SSLManagerInterface> sslManager =
        SSLManagerInterface::create(sslKMIPParams, false);
    KMIPService kmipService(server, std::move(sslManager));
    kmipService._initServerConnection(connectTimeout);
    return std::move(kmipService);
} catch (const DBException& e) {
    return e.toStatus();
}

StatusWith<KMIPService> KMIPService::createKMIPService(const KMIPParams& kmipParams,
                                                       bool sslFIPSMode) {
    SSLParams sslKMIPParams;

    // KMIP specific parameters.
    sslKMIPParams.sslPEMKeyFile = kmipParams.kmipClientCertificateFile;
    sslKMIPParams.sslPEMKeyPassword = kmipParams.kmipClientCertificatePassword;
    sslKMIPParams.sslClusterFile = "";
    sslKMIPParams.sslClusterPassword = "";
    sslKMIPParams.sslCAFile = kmipParams.kmipServerCAFile;
#ifdef MONGO_CONFIG_SSL_CERTIFICATE_SELECTORS
    sslKMIPParams.sslCertificateSelector = kmipParams.kmipClientCertificateSelector;
#endif
    sslKMIPParams.sslFIPSMode = sslFIPSMode;

    // KMIP servers never should have invalid certificates
    sslKMIPParams.sslAllowInvalidCertificates = false;
    sslKMIPParams.sslAllowInvalidHostnames = false;
    sslKMIPParams.sslCRLFile = "";

    Milliseconds connectTimeout(kmipParams.kmipConnectTimeoutMS);

    // Repeat iteration through the list one or more times.
    auto retries = kmipParams.kmipConnectRetries;
    do {
        // iterate through the list of provided KMIP servers until a valid one is found
        for (auto it = kmipParams.kmipServerName.begin(); it != kmipParams.kmipServerName.end();
             ++it) {
            HostAndPort hp(*it, kmipParams.kmipPort);
            auto swService = createKMIPService(hp, sslKMIPParams, connectTimeout);
            if (swService.isOK()) {
                return std::move(swService.getValue());
            }
            if ((it + 1) != kmipParams.kmipServerName.end()) {
                LOGV2_WARNING(24240,
                              "Connection to KMIP server failed. Trying next server",
                              "failedHost"_attr = hp,
                              "nextHost"_attr = HostAndPort(*(it + 1), kmipParams.kmipPort));
            } else if (retries) {
                LOGV2_WARNING(24241,
                              "Connection to KMIP server failed. Restarting connect "
                              "attempt(s) with remaining retries",
                              "host"_attr = hp,
                              "retryCount"_attr = retries);
            } else {
                return swService.getStatus();
            }
        }
    } while (retries--);

    // Only reachable if the server name list is empty.
    return {ErrorCodes::BadValue, "No KMIP server specified."};
}

KMIPService::KMIPService(const HostAndPort& server, std::shared_ptr<SSLManagerInterface> sslManager)
    : _sslManager(std::move(sslManager)),
      _server(server),
      _socket(std::make_unique<Socket>(10, logv2::LogSeverity::Log())) {}

StatusWith<std::string> KMIPService::createExternalKey() {
    StatusWith<KMIPResponse> swResponse = _sendRequest(_generateKMIPCreateRequest());
    if (!swResponse.isOK()) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "KMIP create key failed: " << swResponse.getStatus().reason());
    }

    const KMIPResponse& response = swResponse.getValue();
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "KMIP create key failed, code: " << response.getResultReason()
                          << " error: " << response.getResultMsg()->data());
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
                      str::stream() << "KMIP get key '" << uid
                                    << "' failed, code: " << response.getResultReason()
                                    << " error: " << response.getResultMsg()->data());
    }

    std::unique_ptr<SymmetricKey> key = response.getSymmetricKey();
    if (key->getKeySize() != crypto::sym256KeySize) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP got a key which was " << key->getKeySize() * 8
                                    << " bits long, but a " << crypto::sym256KeySize * 8
                                    << " bit key is required");
    }
    return std::move(key);
}


StatusWith<KMIPService::EncryptionResult> KMIPService::encrypt(const std::string& uid,
                                                               const SecureVector<uint8_t>& data) {
    StatusWith<KMIPResponse> swResponse = _sendRequest(_generateKMIPEncryptRequest(uid, data));
    if (!swResponse.isOK()) {
        return swResponse.getStatus();
    }

    KMIPResponse response = std::move(swResponse.getValue());
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP encrypt failed, code: " << response.getResultReason()
                                    << " error: " << response.getResultMsg()->data());
    }

    auto sdata = response.getData();
    auto iv = response.getIV();
    return EncryptionResult{iv,
                            std::vector<uint8_t>(std::make_move_iterator(sdata->begin()),
                                                 std::make_move_iterator(sdata->end()))};
}

StatusWith<SecureVector<uint8_t>> KMIPService::decrypt(const std::string& uid,
                                                       const std::vector<uint8_t>& iv,
                                                       const std::vector<uint8_t>& data) {
    StatusWith<KMIPResponse> swResponse = _sendRequest(_generateKMIPDecryptRequest(uid, iv, data));
    if (!swResponse.isOK()) {
        return swResponse.getStatus();
    }

    KMIPResponse response = std::move(swResponse.getValue());
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP decrypt failed, code: " << response.getResultReason()
                                    << " error: " << response.getResultMsg()->data());
    }

    return response.getData();
}

StatusWith<const std::string> KMIPService::activate(const std::string& uid) {
    StatusWith<KMIPResponse> swResponse = _sendRequest(_generateKMIPActivateRequest(uid));
    if (!swResponse.isOK()) {
        return swResponse.getStatus();
    }

    KMIPResponse response = std::move(swResponse.getValue());
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "KMIP activate failed, code: " << response.getResultReason()
                                    << ", error: " << response.getResultMsg()->data());
    }
    return response.getUID();
}

StatusWith<KMIPResponse::Attribute> KMIPService::getAttributes(const std::string& uid,
                                                               const std::string& attributeName) {
    StatusWith<KMIPResponse> swResponse =
        _sendRequest(_generateKMIPGetAttributesRequest(uid, attributeName));
    if (!swResponse.isOK()) {
        return swResponse.getStatus();
    }

    KMIPResponse response = std::move(swResponse.getValue());
    if (response.getResultStatus() != kmip::statusSuccess) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "KMIP get attributes failed, code: " << response.getResultReason()
                          << ", error: " << response.getResultMsg()->data());
    }

    return response.getAttribute();
}

void KMIPService::_initServerConnection(Milliseconds connectTimeout) {
    auto makeAddress = [](const auto& host) -> SockAddr {
        try {
            return SockAddr::create(host.host().c_str(), host.port(), AF_UNSPEC);
        } catch (const DBException& ex) {
            uassertStatusOK(ex.toStatus().withContext("Unable to resolve KMS server address"));
        }

        MONGO_UNREACHABLE;
    };

    auto addr = makeAddress(_server);
    if (!_socket->connect(addr, connectTimeout)) {
        uasserted(ErrorCodes::BadValue,
                  str::stream() << "Could not connect to KMIP server " << addr.toString());
    }

    if (!_socket->secure(_sslManager.get(), _server.host())) {
        uasserted(ErrorCodes::BadValue,
                  str::stream() << "Failed to perform SSL handshake with the KMIP server "
                                << addr.toString());
    }
}

// Sends a request message to the KMIP server and creates a KMIPResponse.
StatusWith<KMIPResponse> KMIPService::_sendRequest(const SecureVector<uint8_t>& request) {
    SecureVector<char> resp(8);

    _socket->send(reinterpret_cast<const char*>(request->data()), request->size(), "KMIP request");
    /**
     *  Read response header on the form:
     *  data[0:2] - tag identifier
     *  data[3]   - tag type
     *  data[4:7] - big endian encoded message body length
     */
    _socket->recv(resp->data(), 8);
    if (memcmp(resp->data(), kmip::responseMessageTag, 3) != 0 ||
        (*resp)[3] != static_cast<char>(ItemType::structure)) {
        return Status(ErrorCodes::FailedToParse,
                      "Expected KMIP response message to start with"
                      "reponse message tag");
    }

    ConstDataRangeCursor cdrc(resp->data() + 4, resp->data() + 8);
    StatusWith<BigEndian<uint32_t>> swBodyLength = cdrc.readAndAdvance<BigEndian<uint32_t>>();
    if (!swBodyLength.isOK()) {
        return swBodyLength.getStatus();
    }

    uint32_t bodyLength = static_cast<uint32_t>(swBodyLength.getValue());
    resp->resize(bodyLength + 8);
    _socket->recv(&(*resp)[8], bodyLength);

    StatusWith<KMIPResponse> swKMIPResponse = KMIPResponse::create(resp);
    return swKMIPResponse;
}

SecureVector<uint8_t> KMIPService::_generateKMIPGetRequest(const std::string& uid) {
    std::vector<uint8_t> uuid(std::begin(uid), std::end(uid));
    mongo::kmip::GetKMIPRequestParameters getRequestParams(uuid);
    return encodeKMIPRequest(getRequestParams);
}

SecureVector<uint8_t> KMIPService::_generateKMIPCreateRequest() {
    std::vector<uint8_t> algorithm(std::begin(aesCryptoAlgorithm), std::end(aesCryptoAlgorithm));
    std::vector<uint8_t> length = convertIntToBigEndianArray(256);
    std::vector<uint8_t> usageMask{
        0x00, 0x00, 0x00, cryptoUsageMaskEncrypt | cryptoUsageMaskDecrypt};

    CreateKMIPRequestParameters createRequestParams(algorithm, length, usageMask);
    return encodeKMIPRequest(createRequestParams);
}

SecureVector<uint8_t> KMIPService::_generateKMIPEncryptRequest(const std::string& uid,
                                                               const SecureVector<uint8_t>& data) {
    EncryptKMIPRequestParameters encryptRequestParams({std::begin(uid), std::end(uid)}, data);
    return encodeKMIPRequest(encryptRequestParams);
}

SecureVector<uint8_t> KMIPService::_generateKMIPDecryptRequest(const std::string& uid,
                                                               const std::vector<uint8_t>& iv,
                                                               const std::vector<uint8_t>& data) {
    DecryptKMIPRequestParameters decryptRequestParams({std::begin(uid), std::end(uid)}, iv, data);
    return encodeKMIPRequest(decryptRequestParams);
}

SecureVector<uint8_t> KMIPService::_generateKMIPActivateRequest(const std::string& uid) {
    ActivateKMIPRequestParameters activateRequestParams({std::begin(uid), std::end(uid)});
    return encodeKMIPRequest(activateRequestParams);
}

SecureVector<uint8_t> KMIPService::_generateKMIPGetAttributesRequest(
    const std::string& uid, const std::string& attributeName) {
    GetAttributesKMIPRequestParameters getAttributesRequestParams(
        {std::begin(uid), std::end(uid)}, {std::begin(attributeName), std::end(attributeName)});
    return encodeKMIPRequest(getAttributesRequestParams);
}
}  // namespace kmip
}  // namespace mongo
