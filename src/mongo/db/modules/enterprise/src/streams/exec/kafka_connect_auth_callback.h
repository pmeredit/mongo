/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>
#include <sys/epoll.h>

#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/crypto/symmetric_key.h"
#include "streams/exec/kafka_callback_base.h"

namespace streams {

struct Context;

// KafkaConnectAuthCallback is used to authenticate to GWProxy and provide support
// for VPC peering sessions.  The full technical design for this authentication flow
// can be found here:
// https://docs.google.com/document/d/1gu4sJsiNE-K5yKNIThcUNApo7bsVj5zY1GVqhPF_64g
class KafkaConnectAuthCallback : public RdKafka::ConnectCb, public KafkaCallbackBase {
public:
    KafkaConnectAuthCallback(Context* context,
                             std::string operatorName,
                             std::string symmetricKey,
                             uint32_t connectionTimeoutSeconds);
    // Called by librdkafka.
    int connect_cb(int sockfd, const struct sockaddr* addr, int addrlen, const char* id) override;

private:
    friend class KafkaConnectAuthCallbackTest;

    mongo::SymmetricKey buildKey(const std::string& plainKey);
    std::vector<uint8_t> buildJSONPayload(const std::string& hostname,
                                          std::string timestamp,
                                          uint64_t randomSeed);
    std::string getEpochAsString();
    std::unique_ptr<mongo::crypto::SymmetricEncryptor> getEncryptor(
        const mongo::SymmetricKey& key, const std::array<std::uint8_t, 12>& nonce);
    std::string getPayload(uint64_t randomSeed, std::string hostname);
    uint64_t parseHeader(int sockfd, int epollFd, epoll_event& epollEvent);
    int waitForConnection(int sockfd, int epollFd, epoll_event& epollEvent);
    int connectCbImpl(int sockfd, const struct sockaddr* addr, int addrlen, const char* id);

    // readSocketData will wait for the next EPOLLIN event, and then attempt
    // to read the specified number of bytes (as long as they arrive within
    // maxRetries iterations over the non-blocking recv attempt), and will
    // then return this to the caller. If we error or time out, a DBException
    // is thrown.
    std::vector<char> readSocketData(int socketFile,
                                     int timeoutSeconds,
                                     size_t bufferSize,
                                     int maxRetries,
                                     int epollFd,
                                     epoll_event& epollEvent);

    Context* _context{nullptr};
    std::string _operatorName;
    std::string _symmetricKey;
    uint32_t _connectionTimeoutSeconds;
};

}  // namespace streams
