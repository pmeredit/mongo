/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "kmip_options.h"

#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

void addKMIPOptions(moe::OptionSection* options) {
    options->addOptionChaining("security.kmip.keyIdentifier",
                               "kmipKeyIdentifier",
                               moe::String,
                               "KMIP unique identifier for existing key to use");
    options->addOptionChaining(
        "security.kmip.serverName", "kmipServerName", moe::String, "KMIP server host name");
    options
        ->addOptionChaining(
            "security.kmip.port", "kmipPort", moe::Int, "KMIP server port (defaults to 5696)")
        .requires("security.kmip.serverName");
    options
        ->addOptionChaining("security.kmip.clientCertificateFile",
                            "kmipClientCertificateFile",
                            moe::String,
                            "Client certificate for authenticating to KMIP server")
        .requires("security.kmip.serverName");
    options
        ->addOptionChaining("security.kmip.clientCertificatePassword",
                            "kmipClientCertificatePassword",
                            moe::String,
                            "Client certificate for authenticating Mongo to KMIP server")
        .requires("security.kmip.clientCertificateFile");
    options->addOptionChaining("security.kmip.serverCAFile",
                               "kmipServerCAFile",
                               moe::String,
                               "CA File for validating connection to KMIP server");
#ifdef MONGO_CONFIG_SSL_CERTIFICATE_SELECTORS
    options
        ->addOptionChaining("security.kmip.clientCertificateSelector",
                            "kmipClientCertificateSelector",
                            moe::String,
                            "Client Certificate in system store for authenticating to KMIP server")
        .incompatibleWith("security.kmip.clientCertificateFile")
        .incompatibleWith("security.kmip.clientCertificatePassword");
#endif
}

StatusWith<KMIPParams> parseKMIPOptions(const moe::Environment& params) {
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
