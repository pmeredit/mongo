/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <vector>

#include "kmip_consts.h"
#include "kmip_options.h"
#include "kmip_response.h"
#include "mongo/base/secure_allocator.h"
#include "mongo/base/status_with.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/net/ssl_manager.h"

namespace mongo {

class SSLManager;
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

    /**
     * Communicates with the KMIP server that a key should be
     * created, and returns a StatusWith< uid >.
     */
    StatusWith<std::string> createExternalKey();

    /**
     * Attempts to retrieve the key 'uid' from the KMIP server
     */
    StatusWith<std::unique_ptr<SymmetricKey>> getExternalKey(const std::string& uid);

    struct EncryptionResult {
        std::vector<uint8_t> iv;
        std::vector<uint8_t> data;
    };

    /**
     * Requests the KMIP server to perform an encryption operation on the specified data with the
     * given key, and returns a StatusWith of EncryptionResult.
     */
    StatusWith<EncryptionResult> encrypt(const std::string& uid, const SecureVector<uint8_t>& data);

    /**
     * Requests the KMIP server to perform a decryption operation on the specified data with the
     * given key and IV, and returns a StatusWith of the decrypted data.
     */
    StatusWith<SecureVector<uint8_t>> decrypt(const std::string& uid,
                                              const std::vector<uint8_t>& data,
                                              const std::vector<uint8_t>& iv);

    /**
     * Requests the KMIP server to perform an activation operation on a non-activated KMIP key with
     * the UID specified and returns the key's UID.
     */
    StatusWith<const std::string> activate(const std::string& uid);

    /**
     * Requests the KMIP server to get the attribute of an object specified by the UID and returns
     * the requested attribute consisting of the: attribute name, attribute index, attribute value.
     */
    StatusWith<KMIPResponse::Attribute> getAttributes(const std::string& uid,
                                                      const std::string& attributeName);

    static StatusWith<KMIPService> createKMIPService();

private:
    KMIPService(const HostAndPort& server, std::shared_ptr<SSLManagerInterface> sslManager);
    static StatusWith<KMIPService> createKMIPService(const KMIPParams& kmipParams, bool fipsMode);
    static StatusWith<KMIPService> createKMIPService(const HostAndPort& server,
                                                     const SSLParams& sslKMIPParams,
                                                     Milliseconds connectTimeout);

    /**
     * Initialize a connection to the KMIP server
     *
     * Throws upon failure.
     */
    void _initServerConnection(Milliseconds connectTimeout);

    /**
     * Send a request to the KMIP server.
     */
    StatusWith<KMIPResponse> _sendRequest(const SecureVector<uint8_t>& request);

    SecureVector<uint8_t> _generateKMIPActivateRequest(const std::string& uid);

    SecureVector<uint8_t> _generateKMIPGetRequest(const std::string& uid);

    SecureVector<uint8_t> _generateKMIPCreateRequest();

    SecureVector<uint8_t> _generateKMIPEncryptRequest(const std::string& uid,
                                                      const SecureVector<uint8_t>& data);

    SecureVector<uint8_t> _generateKMIPDecryptRequest(const std::string& uid,
                                                      const std::vector<uint8_t>& data,
                                                      const std::vector<uint8_t>& iv);

    SecureVector<uint8_t> _generateKMIPGetAttributesRequest(const std::string& uid,
                                                            const std::string& attributeName);

    std::shared_ptr<SSLManagerInterface> _sslManager;
    HostAndPort _server;
    std::unique_ptr<Socket> _socket;
};

}  // namespace kmip
}  // namespace mongo
