/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <boost/algorithm/string/classification.hpp>
#include <chrono>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <termios.h>

#include "mongo/base/data_range.h"
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
constexpr int kConnectSleepMs = 100;
constexpr int kMaxConnectRetries = 100;
constexpr int kSocketTimeoutSecs = 10;

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
    std::copy(plainKey.begin(), plainKey.end(), key->begin());
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

uint64_t KafkaConnectAuthCallback::parseHeader(const int sockfd) {
    int headerLength = 0;
    int retryCounter = 0;
    while (headerLength < kProxyGreetingSizeBytes && retryCounter < kMaxConnectRetries) {
        LOGV2_INFO(780042,
                   "KafkaConnectAuthCallback waiting for bytes",
                   "context"_attr = _context,
                   "len"_attr = headerLength);

        if (ioctl(sockfd, FIONREAD, &headerLength) < 0) {
            // error
            close(sockfd);
            uasserted(ErrorCodes::InternalError, "Unable to get proxy greeting");
        }

        ++retryCounter;
        stdx::this_thread::sleep_for(stdx::chrono::milliseconds(kConnectSleepMs));
    }

    if (headerLength < kProxyGreetingSizeBytes) {
        // Timed out or encountered error before getting valid greeting.
        close(sockfd);
        uasserted(ErrorCodes::InternalError, "Invalid proxy greeting header");
    }

    // Copy (up to max bytes) of greeting buffer.
    char greetingBytes[kProxyGreetingSizeBytes];
    headerLength = read(sockfd, greetingBytes, kProxyGreetingSizeBytes);
    LOGV2_INFO(780010,
               "KafkaConnectAuthCallback received bytes",
               "context"_attr = _context,
               "len"_attr = headerLength,
               "bytes"_attr = greetingBytes);
    tassert(
        780011,
        str::stream()
            << "KafkaConnectAuthCallback received greeting that was the incorrect size, expected "
            << kProxyGreetingSizeBytes << " and received " << headerLength,
        headerLength == kProxyGreetingSizeBytes);

    uint64_t randomSeed;
    std::memcpy(&randomSeed, greetingBytes, sizeof(randomSeed));

    LOGV2_INFO(780012,
               "KafkaConnectAuthCallback received random seed",
               "context"_attr = _context,
               "len"_attr = headerLength,
               "randomSeed"_attr = randomSeed);

    return randomSeed;
}

int KafkaConnectAuthCallback::waitForConnection(const int sockfd) {
    // Create an epoll instance
    int epollFd = epoll_create1(0);
    tassert(780013, "Error creating epoll socket", epollFd != 0);

    // Add the socket to the epoll event list for read or write events
    struct epoll_event event;
    event.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP;
    event.data.fd = sockfd;

    int epollRes = epoll_ctl(epollFd, EPOLL_CTL_ADD, sockfd, &event);
    tassert(780014, "Error adding socket to epoll list", epollRes != -1);

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

        if (waitForConnection(sockfd) < 0) {
            // Encountered error waiting for connection.
            close(sockfd);
            LOGV2_ERROR(780018,
                        "KafkaConnectAuthCallback failed to connect to proxy",
                        "context"_attr = _context);
            return -1;
        }

        LOGV2_INFO(780019, "KafkaConnectAuthCallback socket is ready", "context"_attr = _context);
    }

    // Wait for proxy greeting (eg. 8 bytes available to read in buffer).
    uint64_t randomSeed;
    try {
        randomSeed = parseHeader(sockfd);
    } catch (const DBException& e) {
        LOGV2_ERROR(780020,
                    "KafkaConnectAuthCallback failed to get proxy greeting, giving up",
                    "context"_attr = _context,
                    "exception"_attr = e.what());
        return -1;
    }

    // Send encrypted preamble.
    // The id field is passed from rkb->rkb_nodename in rdkafka_broker.c, and will reflect
    // the actual node name of the broker being addressed in the connect() call.
    auto payload = this->getPayload(randomSeed, id);
    int sent = send(sockfd, payload.c_str(), payload.length(), /* flags */ 0);
    tassert(780021,
            str::stream() << "Payload bytes sent does not match what is expected, sent " << sent
                          << " and expected " << payload.length(),
            (sent > 0 && static_cast<size_t>(sent) == payload.length()));

    LOGV2_INFO(780022,
               "KafkaConnectAuthCallback returning value",
               "context"_attr = _context,
               "sent_bytes"_attr = sent);

    return 0;
}

}  // namespace streams
