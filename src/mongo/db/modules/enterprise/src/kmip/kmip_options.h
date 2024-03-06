/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

#include "kmip/kmip_options_gen.h"
#include "mongo/base/status_with.h"
#include "mongo/config.h"
#include "mongo/util/net/ssl_options.h"

namespace mongo {

struct KMIPParams {
    int kmipPort = 5696;
    int kmipConnectTimeoutMS = kSecurity_kmip_connectTimeoutMSDefault;
    int kmipConnectRetries = kSecurity_kmip_connectRetriesDefault;
    bool activateKeys;
    std::string kmipKeyIdentifier;
    std::vector<std::string> kmipServerName;
    std::string kmipClientCertificateFile;
    std::string kmipClientCertificatePassword;
    std::string kmipServerCAFile;
    boost::optional<Seconds> kmipKeyStatePollingSeconds;

#ifdef MONGO_CONFIG_SSL_CERTIFICATE_SELECTORS
    SSLParams::CertificateSelector kmipClientCertificateSelector;  // --kmipCertificateSelector
#endif
};

namespace optionenvironment {
class Environment;
}  // namespace optionenvironment

StatusWith<KMIPParams> parseKMIPOptions(const optionenvironment::Environment& params);

}  // namespace mongo
