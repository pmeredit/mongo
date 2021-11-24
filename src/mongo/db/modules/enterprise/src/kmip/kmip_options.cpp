/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string/trim.hpp"

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
        // the input parameter is a comma-separated list (string) of KMIP server names
        auto kmipServerNamesStr = params["security.kmip.serverName"].as<std::string>();
        // split the list, delimited by commas, into a vector of strings, trimming whitespace
        std::vector<std::string> kmipServerNames;
        boost::split(kmipServerNames,
                     kmipServerNamesStr,
                     std::bind(std::equal_to<char>(), std::placeholders::_1, ','),
                     boost::token_compress_on);
        for (std::string& kmipServerName : kmipServerNames) {
            boost::trim(kmipServerName);
        }
        kmipParams.kmipServerName = kmipServerNames;
    }

    if (params.count("security.kmip.port")) {
        kmipParams.kmipPort = params["security.kmip.port"].as<int>();
    }

    if (params.count("security.kmip.connectTimeoutMS")) {
        kmipParams.kmipConnectTimeoutMS = params["security.kmip.connectTimeoutMS"].as<int>();
    }

    if (params.count("security.kmip.connectRetries")) {
        kmipParams.kmipConnectRetries = params["security.kmip.connectRetries"].as<int>();
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

    if (params.count("security.kmip.activateKeys")) {
        kmipParams.activateKeys = params["security.kmip.activateKeys"].as<bool>();
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
