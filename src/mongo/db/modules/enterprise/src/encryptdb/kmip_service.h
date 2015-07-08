/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <vector>

#include "kmip_consts.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/sock.h"

namespace mongo {
template <typename T>
class StatusWith;
class SSLManager;
struct SSLParams;
class Status;
class SymmetricKey;

namespace kmip {
class KMIPResponse;

/**
 * The KMIPService handles communication with the KMIP server.
 */
class KMIPService {
public:
    KMIPService(const HostAndPort& server, const SSLParams& sslKMIPParams);

    /**
     * Communicates with the KMIP server that a key should be
     * created, and returns a StatusWith< uid >.
     */
    StatusWith<std::string> createExternalKey();

    /**
     * Attempts to retrieve the key 'uid' from the KMIP server
     */
    StatusWith<std::unique_ptr<SymmetricKey>> getExternalKey(const std::string& uid);

private:
    /**
     * Initialize a connection to the KMIP server
     */
    Status _initServerConnection();

    /**
     * Send a request to the KMIP server.
     */
    StatusWith<KMIPResponse> _sendRequest(const std::vector<uint8_t>& request);

    std::vector<uint8_t> _generateKMIPGetRequest(const std::string& uid);

    std::vector<uint8_t> _generateKMIPCreateRequest();

    std::unique_ptr<SSLManagerInterface> _sslManager;
    HostAndPort _server;
    Socket _socket;
};

}  // namespace kmip
}  // namespace mongo
