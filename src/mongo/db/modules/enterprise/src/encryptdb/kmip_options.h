/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

#include "mongo/base/status_with.h"
#include "mongo/config.h"
#include "mongo/util/net/ssl_options.h"

namespace mongo {

struct KMIPParams {
    int kmipPort = 5696;
    std::string kmipKeyIdentifier;
    std::string kmipServerName;
    std::string kmipClientCertificateFile;
    std::string kmipClientCertificatePassword;
    std::string kmipServerCAFile;

#ifdef MONGO_CONFIG_SSL_CERTIFICATE_SELECTORS
    SSLParams::CertificateSelector kmipClientCertificateSelector;  // --kmipCertificateSelector
#endif
};

namespace optionenvironment {
class Environment;
}  // namespace optionenvironment

StatusWith<KMIPParams> parseKMIPOptions(const optionenvironment::Environment& params);

}  // namespace mongo
