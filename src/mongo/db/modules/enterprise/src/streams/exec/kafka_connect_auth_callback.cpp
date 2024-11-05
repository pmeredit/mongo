/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/crypto/symmetric_crypto.h"
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <cerrno>
#include <chrono>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <termios.h>

#include "mongo/base/data_range.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/json.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/kafka_connect_auth_callback.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace {

// Constants for Kafka Connect Callback methods.
constexpr int kNonceSizeBytes = 12;
constexpr int kProxyGreetingSizeBytes = 8;
constexpr StringData kProxyResponse = "OK"_sd;
constexpr int kConnectSleepMs = 100;
constexpr int kMaxConnectRetries = 100;
constexpr int kSocketTimeoutSecs = 10;
constexpr int kMaxResponseTimeoutSecs = 3;
constexpr int kMaxEpollEvents = 1;

// Error constants for user facing errors.
constexpr const char kErrorVPCPeerNotResponding[] =
    "VPC Proxy for Kafka connection is not ready yet, check connection status";
constexpr const char kErrorEncryptionKeyInvalid[] = "VPC proxy encryption key is invalid";

};  // namespace

namespace streams {

KafkaConnectAuthCallback::KafkaConnectAuthCallback(Context* context,
                                                   const std::string operatorName,
                                                   const std::string symmetricKey,
                                                   const uint32_t connectionTimeoutSeconds)
    : _context(context),
      _operatorName(std::move(operatorName)),
      _symmetricKey(std::move(symmetricKey)),
      _connectionTimeoutSeconds(connectionTimeoutSeconds) {
    LOGV2_INFO(780090,
               "Enabling Kafka GWProxy authentication callback",
               "context"_attr = _context,
               "operatorName"_attr = _operatorName);
}

SymmetricKey KafkaConnectAuthCallback::buildKey(const std::string& plainKey) {
    SecureVector<uint8_t> key(crypto::sym256KeySize);

    // Determine which key type was passed, and decode accordingly.
    switch (plainKey.size()) {
        // For the most part, we should only see hex-encoded strings in production (as the current
        // version of the MMS API should always return them, in general).  Unencoded string support
        // is included for backwards compatibility, and will probably be removed in the future.
        case 32: {
            tassert(ErrorCodes::InternalError,
                    "Decoded authentication key is an invalid size",
                    plainKey.size() == key->size());
            std::copy(plainKey.begin(), plainKey.end(), key->begin());
            break;
        }
        case 64: {
            try {
                // Note that decodedString will contain raw bytes, not an actual string.
                std::string decodedString = hexblob::decode(plainKey);
                tassert(ErrorCodes::InternalError,
                        "Decoded authentication key is an invalid size",
                        decodedString.size() == key->size());
                std::copy(decodedString.begin(), decodedString.end(), key->begin());
            } catch (const ExceptionFor<ErrorCodes::FailedToParse>& e) {
                addUserFacingError(kErrorEncryptionKeyInvalid);
                uasserted(ErrorCodes::InternalError,
                          str::stream() << "Encryption key is invalid: " << e.what());
            }
            break;
        }
        default:
            tasserted(ErrorCodes::InternalError, "Encryption key size is invalid");
    }

    auto keyId =
        fmt::format("{}_{}_{}", _context->tenantId, _context->streamProcessorId, _operatorName);
    auto sk = SymmetricKey(std::move(key), crypto::aesAlgorithm, keyId);

    return sk;
}

std::unique_ptr<mongo::crypto::SymmetricEncryptor> KafkaConnectAuthCallback::getEncryptor(
    const mongo::SymmetricKey& key, const std::array<std::uint8_t, kNonceSizeBytes>& nonce) {
    auto e = crypto::SymmetricEncryptor::create(key, crypto::aesMode::gcm, nonce);
    return std::move(e.getValue());
}

std::array<std::uint8_t, kNonceSizeBytes> getRandomInitializationVector() {
    std::array<std::uint8_t, kNonceSizeBytes> iv;
    Status status = crypto::engineRandBytes(iv);
    uassert(ErrorCodes::InternalError, "Failed to generate random IV for AES GCM.", status.isOK());

    return iv;
}

std::vector<uint8_t> KafkaConnectAuthCallback::buildJSONPayload(const std::string& hostname,
                                                                const std::string timestamp,
                                                                const uint64_t randomSeed) {

    // Specifies the expected payload fields in the authentication message.
    // Increment readBuffer pointer, decrement remaining buffer size for reads.
    KafkaConnectAuthSpec kcas{
        hostname,                  /* Hostname of broker */
        timestamp,                 /* Timestamp */
        std::to_string(randomSeed) /* Random seed */
    };

    auto jsonString = tojson(kcas.toBSON());
    return std::vector<uint8_t>(jsonString.begin(), jsonString.end());
}

std::string KafkaConnectAuthCallback::getEpochAsString() {
    auto currentTime = std::chrono::system_clock::now().time_since_epoch();
    return std::to_string(currentTime.count());
}

// This interacts with the Gateway Proxy authentication code hosted here:
// https://github.com/xgen-cloud/gosre/blob/master/internal/gwproxy/gateway_proxy.go
std::string KafkaConnectAuthCallback::getPayload(uint64_t randomSeed, std::string hostname) {
    // Create a key object from plaintext key.
    auto sk = buildKey(std::string{_symmetricKey});

    // Build initialization vector.
    std::array<std::uint8_t, kNonceSizeBytes> iv;
    iv = getRandomInitializationVector();

    // Get plaintext JSON input message.
    auto plainText = buildJSONPayload(hostname, getEpochAsString(), randomSeed);

    // Configure AES GCM encryptor.
    std::unique_ptr<mongo::crypto::SymmetricEncryptor> encryptor = getEncryptor(sk, iv);

    // Build output buffer, initialize with initialization vector.
    std::vector<std::uint8_t> cryptoBuffer(plainText.size() + iv.size());
    std::copy(iv.begin(), iv.end(), cryptoBuffer.begin());

    // Encrypt plaintext.
    std::vector<uint8_t> cipherText(plainText.size());
    auto swSize = encryptor->update(plainText, cipherText);
    auto cryptoSize = swSize.getValue();
    swSize = encryptor->finalize({cipherText.data() + cryptoSize, cipherText.size() - cryptoSize});

    // Copy encrypted blocks into cipherText buffer after the nonce.
    std::copy(cipherText.begin(), cipherText.end(), cryptoBuffer.begin() + kNonceSizeBytes);

    // Build 96-bit message authentication tag, and append to encrypted payload.
    std::array<std::uint8_t, kNonceSizeBytes> tag;
    auto tagLen = encryptor->finalizeTag(tag);
    // Golang cipher/aes expects the 12 byte auth tag at the end of the ciphertext.
    std::copy(&tag[0], &tag[kNonceSizeBytes], back_inserter(cryptoBuffer));

    // Base64 encode encrypted payload.
    auto b64crypt = base64::encode(StringData(reinterpret_cast<const char*>(cryptoBuffer.data()),
                                              cryptoBuffer.size())) +
        "\n";

    LOGV2_INFO(780041,
               "KafkaConnectAuthCallback sending authentication message",
               "context"_attr = _context);

    return b64crypt;
}

std::vector<char> KafkaConnectAuthCallback::readSocketData(const int socketFile,
                                                           const int timeoutSeconds,
                                                           const size_t bufferSize,
                                                           const int maxRetries,
                                                           const int epollFd,
                                                           epoll_event& epollEvent) {
    uassert(ErrorCodes::InternalError,
            "Calling readSocketData on uninitialized epoll file descriptor.",
            epollFd > -1);

    std::vector<char> readBuffer(bufferSize);
    epoll_event events[kMaxEpollEvents];

    // Modify epoll instance to block for EPOLLIN events only
    epollEvent.events = EPOLLIN;
    epoll_ctl(epollFd, EPOLL_CTL_MOD, socketFile, &epollEvent);

    int eventCounter = epoll_wait(epollFd, events, kMaxEpollEvents, timeoutSeconds * 1000);
    if (eventCounter < 0) {
        uasserted(ErrorCodes::InternalError,
                  "Failed while waiting for data in epoll during readSocketData");
    }

    // Read and block until we receive all expected bytes, or receive a signal/error
    size_t bytesStored{0};
    ssize_t bytesReceived{0};
    int retryCounter{0};

    while (retryCounter++ < maxRetries) {
        // Increment readBuffer pointer, decrement remaining buffer size for reads.
        bytesReceived =
            recv(socketFile, readBuffer.data() + bytesStored, readBuffer.size() - bytesStored, 0);
        if (bytesReceived < 0) {
            // Error condition - assert and let rdkafka attempt retries.
            LOGV2_ERROR(780022,
                        "KafkaConnectAuthCallback error occurred reading data",
                        "error"_attr = bytesReceived,
                        "errno"_attr = errno,
                        "context"_attr = _context);
            uasserted(ErrorCodes::InternalError,
                      "Failed while waiting for data in socket recv during readSocketData");
        } else if (bytesReceived == 0) {
            // No errors, but no data received in this cycle, continue looping.
            LOGV2_ERROR(780023,
                        "KafkaConnectAuthCallback received 0 bytes, retrying read",
                        "context"_attr = _context);
        } else {
            // Good response, data received. Loop will check to see if we filled the buffer,
            // or if we need to iterate to read more data before proceeding.
            bytesStored += bytesReceived;
            LOGV2_DEBUG(780024,
                        1, /* debug level */
                        "KafkaConnectAuthCallback::readSocketData received data",
                        "bytesReceived"_attr = bytesReceived,
                        "bytesStored"_attr = bytesStored,
                        "context"_attr = _context);

            // This condition should never really exist (unless our math above contains a bug),
            // but this check will ensure we don't allow an overflow to go unchecked.
            tassert(ErrorCodes::InternalError,
                    str::stream() << "KafkaConnectAuthCallback::readSocketData received more data "
                                     "than expected, received a total of "
                                  << bytesStored << " bytes, and expected " << readBuffer.size(),
                    bytesStored <= readBuffer.size());

            if (bytesStored == readBuffer.size()) {
                // All bytes received, stop reading from nonblocking socket and move on.
                LOGV2_INFO(780029,
                           "KafkaConnectAuthCallback::readSocketData read complete",
                           "bytesReceived"_attr = bytesReceived,
                           "bytesStored"_attr = bytesStored,
                           "readBuffer_size"_attr = readBuffer.size(),
                           "context"_attr = _context);
                break;
            }
        }

        stdx::this_thread::sleep_for(stdx::chrono::milliseconds(kConnectSleepMs));
    }

    uassert(ErrorCodes::InternalError,
            str::stream() << "KafkaConnectAuthCallback::readSocketData received less data than "
                             "expected, received a total of "
                          << bytesStored << " bytes, and expected " << readBuffer.size()
                          << ". This is often caused by transient network errors. If this happens"
                             " constantly, the proxy may be down, or has invalid security group"
                             " rules for some reason",
            bytesStored == readBuffer.size());

    return readBuffer;
}

uint64_t KafkaConnectAuthCallback::parseHeader(const int sockfd,
                                               const int epollFd,
                                               epoll_event& epollEvent) {
    LOGV2_INFO(780042,
               "KafkaConnectAuthCallback waiting for authentication header",
               "context"_attr = _context);

    std::vector<char> readBuffer = readSocketData(sockfd,
                                                  kMaxResponseTimeoutSecs,
                                                  kProxyGreetingSizeBytes,
                                                  kMaxConnectRetries,
                                                  epollFd,
                                                  epollEvent);

    uint64_t randomSeed;
    tassert(ErrorCodes::InternalError,
            "KafkaConnectAuthCallback::parseHeader failed, randomSeed isn't large enough to copy "
            "readBuffer into",
            (readBuffer.size() * sizeof(char)) >= sizeof(randomSeed));

    std::memcpy(&randomSeed, readBuffer.data(), sizeof(randomSeed));

    LOGV2_INFO(780012,
               "KafkaConnectAuthCallback received random seed",
               "context"_attr = _context,
               "len"_attr = readBuffer.size(),
               "randomSeed"_attr = randomSeed);

    return randomSeed;
}

// Creates an epoll file descriptor on our open connection, waits for the
// connection to be established, and either returns a valid epoll file
// descriptor or an error.
int KafkaConnectAuthCallback::waitForConnection(const int sockfd,
                                                int epollFd,
                                                epoll_event& epollEvent) {
    int epollRes = epoll_ctl(epollFd, EPOLL_CTL_ADD, sockfd, &epollEvent);
    tassert(ErrorCodes::InternalError, "Error adding socket to epoll list", epollRes != -1);

    // Wait for the connection to be established or an error to occur
    struct epoll_event events[1];
    int epollResult = epoll_wait(epollFd, events, 1, kSocketTimeoutSecs * 1000);

    if (epollResult == -1) {
        LOGV2_ERROR(780015,
                    "KafkaConnectAuthCallback failed to connect to proxy",
                    "context"_attr = _context);
        close(epollFd);
        return -1;
    } else if (epollResult == 0) {
        LOGV2_ERROR(780016,
                    "KafkaConnectAuthCallback timed out connecting to proxy",
                    "context"_attr = _context);
        close(epollFd);
        return -1;
    }

    return 0;
}

// connect_cb provides a simple wrapper to connectCbImpl to ensure we handle
// unexpected exceptions.
int KafkaConnectAuthCallback::connect_cb(int sockfd,
                                         const struct sockaddr* addr,
                                         int addrlen,
                                         const char* id) {
    try {
        return connectCbImpl(sockfd, addr, addrlen, id);
    } catch (const std::exception& e) {
        LOGV2_ERROR(
            780050,
            "Unexpected exception while running connect_cb in librdkafka - check gwproxy health",
            "context"_attr = _context,
            "exception"_attr = e.what());
    }
    return -1;
}

// connectCbImpl contains the logic for the connect_cb callback that we provide
// to librdkafka, to implement the GWProxy connection functionality.
int KafkaConnectAuthCallback::connectCbImpl(int sockfd,
                                            const struct sockaddr* addr,
                                            int addrlen,
                                            const char* id) {
    int conn;

    LOGV2_INFO(780043,
               "KafkaConnectAuthCallback is running",
               "context"_attr = _context,
               "broker_id"_attr = id);

    // Create an epoll instance
    int epollFd = epoll_create1(0);
    tassert(ErrorCodes::InternalError, "Error creating epoll socket", epollFd != 0);

    // Add the socket to the epoll event list for read or write events
    struct epoll_event epollEvent;
    epollEvent.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP;
    epollEvent.data.fd = sockfd;

    // Connect to proxy server
    conn = connect(sockfd, reinterpret_cast<const struct sockaddr*>(addr), addrlen);
    if (conn == -1 && (errno != EINPROGRESS)) {
        return conn;
    }

    // Wait for connection to be ready
    if (conn == -1 && errno == EINPROGRESS) {
        LOGV2_INFO(780017,
                   "KafkaConnectAuthCallback connect in progress, EINPROGRESS",
                   "context"_attr = _context);

        if (waitForConnection(sockfd, epollFd, epollEvent) < 0) {
            // Encountered error waiting for connection.
            close(sockfd);
            LOGV2_ERROR(780018,
                        "KafkaConnectAuthCallback failed to connect to proxy",
                        "context"_attr = _context);
            addUserFacingError(kErrorVPCPeerNotResponding);
            return -1;
        }
        LOGV2_INFO(780019, "KafkaConnectAuthCallback socket is ready", "context"_attr = _context);
    }
    ScopeGuard guard([&] { close(epollFd); });

    // Wait for proxy greeting (eg. 8 bytes available to read in buffer).
    uint64_t randomSeed;
    try {
        randomSeed = parseHeader(sockfd, epollFd, epollEvent);
    } catch (const DBException& e) {
        LOGV2_ERROR(780020,
                    "KafkaConnectAuthCallback failed to get proxy greeting, giving up",
                    "context"_attr = _context,
                    "exception"_attr = e.what());
        addUserFacingError(kErrorVPCPeerNotResponding);
        return -1;
    }

    // Send encrypted preamble.
    // The id field is passed from rkb->rkb_nodename in rdkafka_broker.c, and will reflect
    // the actual node name of the broker being addressed in the connect() call.
    auto payload = this->getPayload(randomSeed, id);
    int sent = send(sockfd, payload.c_str(), payload.length(), /* flags */ 0);
    // Note, this will probably be triggered sometimes if gwproxy crashes or becomes
    // unavailable.
    tassert(ErrorCodes::InternalError,
            str::stream() << "Payload bytes sent does not match what is expected, sent " << sent
                          << " and expected " << payload.length(),
            (sent > 0 && static_cast<size_t>(sent) == payload.length()));

    // Wait for valid authentication response.
    LOGV2_INFO(780025,
               "KafkaConnectAuthCallback sent payload, waiting for auth response",
               "context"_attr = _context,
               "payload_size"_attr = sent);

    std::vector<char> readBuffer = readSocketData(sockfd,
                                                  kMaxResponseTimeoutSecs,
                                                  kProxyResponse.length(),
                                                  kMaxConnectRetries,
                                                  epollFd,
                                                  epollEvent);

    if (std::string{readBuffer.begin(), readBuffer.end()} == kProxyResponse) {
        LOGV2_INFO(780026,
                   "KafkaConnectAuthCallback received valid auth response (OK), returning success "
                   "and proceeding with connection to Kafka",
                   "context"_attr = _context);
        return 0;
    } else {
        LOGV2_ERROR(
            780027,
            "KafkaConnectAuthCallback received an invalid auth response, closing connection",
            "context"_attr = _context);
    }

    // If we made it here, we've encountered an error condition, so clean up
    // and notify rdkafka.
    close(sockfd);
    return -1;
}

}  // namespace streams
