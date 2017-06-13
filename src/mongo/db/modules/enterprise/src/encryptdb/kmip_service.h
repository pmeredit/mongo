/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <vector>

#include "kmip_consts.h"
#include "mongo/base/status_with.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/sock.h"

namespace mongo {

class SSLManager;
struct SSLParams;
class SymmetricKey;

namespace kmip {
class KMIPResponse;

/**
 * The KMIPService handles communication with the KMIP server.
 */
class KMIPService {
public:
    KMIPService(KMIPService&&) = default;
    KMIPService& operator=(KMIPService&&) = default;

    KMIPService(const KMIPService&) = delete;
    KMIPService& operator=(const KMIPService&) = delete;

    static StatusWith<KMIPService> createKMIPService(const HostAndPort& server,
                                                     const SSLParams& sslKMIPParams);

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
    KMIPService(const HostAndPort& server,
                const SSLParams& sslKMIPParams,
                std::unique_ptr<SSLManagerInterface> sslManager);

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
    std::unique_ptr<Socket> _socket;
};

}  // namespace kmip
}  // namespace mongo
