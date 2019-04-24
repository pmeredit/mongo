/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include <kms_message/kms_message.h>

#include <stdlib.h>

#include "mongo/base/init.h"
#include "mongo/base/parse_number.h"
#include "mongo/base/secure_allocator.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/json.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/time_support.h"

#include "fle/shell/kms_gen.h"
#include "kms.h"

namespace mongo {
namespace {

constexpr StringData kAwsKMS = "awsKMS"_sd;

/**
 * Free kms_request_t
 */
struct kms_request_tFree {
    void operator()(kms_request_t* p) noexcept {
        if (p) {
            ::kms_request_destroy(p);
        }
    }
};

using UniqueKmsRequest = std::unique_ptr<kms_request_t, kms_request_tFree>;

/**
 * Free kms_response_parser_t
 */
struct kms_response_parser_tFree {
    void operator()(kms_response_parser_t* p) noexcept {
        if (p) {
            ::kms_response_parser_destroy(p);
        }
    }
};

using UniqueKmsResponseParser = std::unique_ptr<kms_response_parser_t, kms_response_parser_tFree>;

/**
 * Free kms_response_t
 */
struct kms_response_tFree {
    void operator()(kms_response_t* p) noexcept {
        if (p) {
            ::kms_response_destroy(p);
        }
    }
};

using UniqueKmsResponse = std::unique_ptr<kms_response_t, kms_response_tFree>;

/**
 * Free kms_char_buffer
 */
struct kms_char_free {
    void operator()(char* x) {
        kms_request_free_string(x);
    }
};

using UniqueKmsCharBuffer = std::unique_ptr<char, kms_char_free>;

/**
 * Make a request to a AWS HTTP endpoint.
 *
 * Does not maintain a persistent HTTP connection.
 */
class AWSConnection {
public:
    AWSConnection(SSLManagerInterface* ssl)
        : _sslManager(ssl), _socket(std::make_unique<Socket>(10, logger::LogSeverity::Log())) {}

    UniqueKmsResponse makeOneRequest(const HostAndPort& host, ConstDataRange request);

private:
    UniqueKmsResponse sendRequest(ConstDataRange request);

    void connect(const HostAndPort& host);

private:
    // SSL Manager for connections
    SSLManagerInterface* _sslManager;

    // Synchronous socket
    std::unique_ptr<Socket> _socket;
};

/**
 * AWS configuration settings
 */
struct AWSConfig {
    // AWS_ACCESS_KEY_ID
    std::string accessKeyId;

    // AWS_SECRET_ACCESS_KEY
    SecureString secretAccessKey;

    // AWS region name like "us-east-1"
    std::string region;

    // Optional AWS_SESSION_TOKEN for AWS STS tokens
    boost::optional<std::string> sessionToken;
};

/**
 * Manages SSL information and config for how to talk to AWS KMS.
 */
class AWSKMSService : public KMSService {
public:
    AWSKMSService() = default;
    ~AWSKMSService() final = default;

    static std::unique_ptr<KMSService> create(const AwsKMS& config);

    std::vector<std::byte> encrypt(ConstDataRange cdr, StringData kmsKeyId) final;

    std::vector<std::byte> decrypt(ConstDataRange cdr) final;

    BSONObj encryptDataKey(ConstDataRange cdr, StringData keyId) final;

private:
    void initRequest(kms_request_t* request);

private:
    // SSL Manager
    std::unique_ptr<SSLManagerInterface> _sslManager;

    // Server to connect to
    HostAndPort _server;

    // AWS configuration settings
    AWSConfig _config;
};

void uassertKmsRequestInternal(kms_request_t* request, bool ok) {
    if (!ok) {
        const char* msg = kms_request_get_error(request);
        uasserted(51135, str::stream() << "Internal AWS KMS Error: " << msg);
    }
}

#define uassertKmsRequest(X) uassertKmsRequestInternal(request, (X));

void AWSKMSService::initRequest(kms_request_t* request) {
    // use current time
    uassertKmsRequest(kms_request_set_date(request, nullptr));

    uassertKmsRequest(kms_request_set_region(request, _config.region.c_str()));

    // kms is always the name of the service
    uassertKmsRequest(kms_request_set_service(request, "kms"));

    uassertKmsRequest(kms_request_set_access_key_id(request, _config.accessKeyId.c_str()));
    uassertKmsRequest(kms_request_set_secret_key(request, _config.secretAccessKey->c_str()));

    if (_config.sessionToken) {
        // TODO: move this into kms-message
        uassertKmsRequest(kms_request_add_header_field(
            request, "X-Amz-Security-Token", _config.sessionToken.get().c_str()));
    }
}

std::vector<std::byte> toVector(const std::string& str) {
    std::vector<std::byte> blob;

    std::transform(std::begin(str), std::end(str), std::back_inserter(blob), [](auto c) {
        return std::byte{static_cast<uint8_t>(c)};
    });

    return blob;
}

std::vector<std::byte> AWSKMSService::encrypt(ConstDataRange cdr, StringData kmsKeyId) {
    auto request =
        UniqueKmsRequest(kms_encrypt_request_new(reinterpret_cast<const uint8_t*>(cdr.data()),
                                                 cdr.length(),
                                                 kmsKeyId.toString().c_str(),
                                                 NULL));

    initRequest(request.get());

    auto buffer = UniqueKmsCharBuffer(kms_request_get_signed(request.get()));
    auto buffer_len = strlen(buffer.get());

    AWSConnection connection(_sslManager.get());
    auto response = connection.makeOneRequest(_server, ConstDataRange(buffer.get(), buffer_len));

    auto body = kms_response_get_body(response.get());

    BSONObj obj = fromjson(body);

    auto awsResponse = AwsEncryptResponse::parse(IDLParserErrorContext("root"), obj);

    auto blobStr = base64::decode(awsResponse.getCiphertextBlob().toString());

    return toVector(blobStr);
}

BSONObj AWSKMSService::encryptDataKey(ConstDataRange cdr, StringData keyId) {
    auto dataKey = encrypt(cdr, keyId);

    std::string dataKeyBase64 =
        base64::encode(reinterpret_cast<const char*>(dataKey.data()), dataKey.size());

    AwsMasterKey masterKey;
    masterKey.setAwsKey(keyId);
    masterKey.setAwsRegion(_config.region);

    MasterKeyAndMaterial keyAndMaterial;
    keyAndMaterial.setKeyMaterial(dataKeyBase64);
    keyAndMaterial.setMasterKey(masterKey);

    return keyAndMaterial.toBSON();
}

std::vector<std::byte> AWSKMSService::decrypt(ConstDataRange cdr) {
    auto request = UniqueKmsRequest(kms_decrypt_request_new(
        reinterpret_cast<const uint8_t*>(cdr.data()), cdr.length(), nullptr));

    initRequest(request.get());

    auto buffer = UniqueKmsCharBuffer(kms_request_get_signed(request.get()));
    auto buffer_len = strlen(buffer.get());
    AWSConnection connection(_sslManager.get());
    auto response = connection.makeOneRequest(_server, ConstDataRange(buffer.get(), buffer_len));

    auto body = kms_response_get_body(response.get());

    BSONObj obj = fromjson(body);

    auto awsResponse = AwsDecryptResponse::parse(IDLParserErrorContext("root"), obj);

    auto blobStr = base64::decode(awsResponse.getPlaintext().toString());

    return toVector(blobStr);
}

void AWSConnection::connect(const HostAndPort& host) {
    SockAddr server(host.host().c_str(), host.port(), AF_UNSPEC);

    uassert(51136,
            str::stream() << "AWS KMS server address " << host.host() << " is invalid.",
            server.isValid());

    uassert(51137,
            str::stream() << "Could not connect to AWS KMS server " << server.toString(),
            _socket->connect(server));

    uassert(51138,
            str::stream() << "Failed to perform SSL handshake with the AWS KMS server "
                          << host.toString(),
            _socket->secure(_sslManager, host.host()));
}

// Sends a request message to the AWS KMS server and creates a KMS Response.
UniqueKmsResponse AWSConnection::sendRequest(ConstDataRange request) {
    std::array<char, 512> resp;

    _socket->send(
        reinterpret_cast<const char*>(request.data()), request.length(), "AWS KMS request");

    auto parser = UniqueKmsResponseParser(kms_response_parser_new());
    int bytes_to_read = 0;

    while ((bytes_to_read = kms_response_parser_wants_bytes(parser.get(), resp.size())) > 0) {
        bytes_to_read = std::min(bytes_to_read, static_cast<int>(resp.size()));
        bytes_to_read = _socket->unsafe_recv(resp.data(), bytes_to_read);

        uassert(51139,
                "kms_response_parser_feed failed",
                kms_response_parser_feed(
                    parser.get(), reinterpret_cast<uint8_t*>(resp.data()), bytes_to_read));
    }

    auto response = UniqueKmsResponse(kms_response_parser_get_response(parser.get()));

    return response;
}

UniqueKmsResponse AWSConnection::makeOneRequest(const HostAndPort& host, ConstDataRange request) {
    connect(host);

    auto resp = sendRequest(request);

    _socket->close();

    return resp;
}

boost::optional<std::string> toString(boost::optional<StringData> str) {
    if (str) {
        return {str.get().toString()};
    }
    return boost::none;
}

std::unique_ptr<KMSService> AWSKMSService::create(const AwsKMS& config) {
    auto awsKMS = std::make_unique<AWSKMSService>();

    SSLParams params;
    params.sslPEMKeyFile = "";
    params.sslPEMKeyPassword = "";
    params.sslClusterFile = "";
    params.sslClusterPassword = "";

    // Leave the CA file empty so we default to system CA but for local testing allow it to inherit
    // the CA file.
    params.sslCAFile = "";
    if (!getTestCommandsEnabled()) {
        params.sslCAFile = sslGlobalParams.sslCAFile;
    }

    params.sslCRLFile = "";

    // Copy the rest from the global SSL manager options.
    params.sslFIPSMode = sslGlobalParams.sslFIPSMode;

    // KMS servers never should have invalid certificates
    params.sslAllowInvalidCertificates = false;
    params.sslAllowInvalidHostnames = false;

    params.sslDisabledProtocols =
        std::vector({SSLParams::Protocols::TLS1_0, SSLParams::Protocols::TLS1_1});

    awsKMS->_sslManager = SSLManagerInterface::create(sslGlobalParams, false);

    if (!config.getUrl()) {
        std::string hostname = str::stream() << "kms."
                                             << "us-east-1"
                                             << ".amazonaws.com";
        awsKMS->_server = HostAndPort(hostname, 443);
    } else {
        awsKMS->_server = parseUrl(config.getUrl().get());
    }

    awsKMS->_config.accessKeyId = config.getAccessKeyId().toString();

    awsKMS->_config.secretAccessKey = config.getSecretAccessKey().toString();

    awsKMS->_config.sessionToken = toString(config.getSessionToken());

    return awsKMS;
}

/**
 * Factory for AWSKMSService if user specifies aws config to mongo() JS constructor.
 */
class AWSKMSServiceFactory final : public KMSServiceFactory {
public:
    AWSKMSServiceFactory() = default;
    ~AWSKMSServiceFactory() = default;

    std::unique_ptr<KMSService> create(const BSONObj& config) final {
        auto obj = config[kAwsKMS].Obj();
        return AWSKMSService::create(AwsKMS::parse(IDLParserErrorContext("root"), obj));
    }
};

}  // namspace

MONGO_INITIALIZER(KMSRegister)(::mongo::InitializerContext* context) {
    KMSServiceController::registerFactory(KMSProviderEnum::aws,
                                          std::make_unique<AWSKMSServiceFactory>());
    return Status::OK();
}

}  // namespace mongo
