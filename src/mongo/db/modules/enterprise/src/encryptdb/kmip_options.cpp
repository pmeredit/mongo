/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "kmip_options.h"

#include "mongo/config.h"
#include "mongo/util/options_parser/environment.h"

namespace mongo {

StatusWith<KMIPParams> parseKMIPOptions(const optionenvironment::Environment& params) {
    KMIPParams kmipParams;

    if (params.count("security.kmip.keyIdentifier")) {
        kmipParams.kmipKeyIdentifier = params["security.kmip.keyIdentifier"].as<std::string>();
    }

    if (params.count("security.kmip.serverName")) {
        kmipParams.kmipServerName = params["security.kmip.serverName"].as<std::string>();
    }

    if (params.count("security.kmip.port")) {
        kmipParams.kmipPort = params["security.kmip.port"].as<int>();
    }

    if (params.count("security.kmip.clientCertificateFile")) {
        kmipParams.kmipClientCertificateFile =
            params["security.kmip.clientCertificateFile"].as<std::string>();
    }

    if (params.count("security.kmip.clientCertificatePassword")) {
        kmipParams.kmipClientCertificatePassword =
            params["security.kmip.clientCertificatePassword"].as<std::string>();
    }

    if (params.count("security.kmip.serverCAFile")) {
        kmipParams.kmipServerCAFile = params["security.kmip.serverCAFile"].as<std::string>();
    }

#ifdef MONGO_CONFIG_SSL_CERTIFICATE_SELECTORS
    if (params.count("security.kmip.clientCertificateSelector")) {
        const auto status = parseCertificateSelector(
            &kmipParams.kmipClientCertificateSelector,
            "security.kmip.clientCertificateSelector",
            params["security.kmip.clientCertificateSelector"].as<std::string>());
        if (!status.isOK()) {
            return status;
        }
    }
#endif

    return kmipParams;
}

}  // namespace mongo
