/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "encryption_options.h"

#include <openssl/evp.h>

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/startup_options.h"
#include "symmetric_crypto.h"

namespace mongo {

EncryptionGlobalParams encryptionGlobalParams;

Status addEncryptionOptions(moe::OptionSection* options) {
    moe::OptionSection encryptionOptions("Encryption at rest options");

    encryptionOptions.addOptionChaining(
        "security.enableEncryption", "enableEncryption", moe::Switch, "Enable encryption at rest");
    encryptionOptions.addOptionChaining("security.kmip.keyIdentifier",
                                        "kmipKeyIdentifier",
                                        moe::String,
                                        "KMIP unique identifier for existing key to use")
        .requires("security.enableEncryption");
    encryptionOptions.addOptionChaining("security.kmip.serverName",
                                        "kmipServerName",
                                        moe::String,
                                        "KMIP server host name")
        .requires("security.enableEncryption");
    encryptionOptions.addOptionChaining("security.kmip.port",
                                        "kmipPort",
                                        moe::Int,
                                        "KMIP server port (defaults to 5696)")
        .requires("security.kmip.serverName");
    encryptionOptions.addOptionChaining("security.kmip.clientCertificateFile",
                                        "kmipClientCertificateFile",
                                        moe::String,
                                        "Client certificate for authenticating to KMIP server")
        .requires("security.kmip.serverName");
    encryptionOptions.addOptionChaining(
                          "security.kmip.clientCertificatePassword",
                          "kmipClientCertificatePassword",
                          moe::String,
                          "Client certificate for authenticating Mongo to KMIP server")
        .requires("security.kmip.clientCertificateFile");
    encryptionOptions.addOptionChaining("security.kmip.serverCAFile",
                                        "kmipServerCAFile",
                                        moe::String,
                                        "CA File for validating connection to KMIP server");
    encryptionOptions.addOptionChaining("security.encryptionKeyFile",
                                        "encryptionKeyFile",
                                        moe::String,
                                        "File path for encryption key file")
        .requires("security.enableEncryption");
    encryptionOptions.addOptionChaining("security.encryptionCipherMode",
                                        "encryptionCipherMode",
                                        moe::String,
                                        "Cipher mode to use for encryption at rest")
        .requires("security.enableEncryption")
        .format("(:?AES256-CBC)|(:?AES256-GCM)", "'AES256-CBC' or 'AES256-GCM'");

    Status ret = options->addSection(encryptionOptions);
    if (!ret.isOK()) {
        log() << "Failed to add encryption option section: " << ret.toString();
        return ret;
    }
    return Status::OK();
}

static Status validateEncryptionOptions(const moe::Environment& params) {
    if (params.count("security.enableEncryption")) {
        if (params.count("security.encryptionKeyFile") ==
            params.count("security.kmip.serverName")) {
            return {ErrorCodes::InvalidOptions,
                    "Must specify either an encryption key file or "
                    "a KMIP server when enabling file encryption"};
        }
        if (params.count("security.encryptionCipherMode")) {
            std::string mode = params["security.encryptionCipherMode"].as<std::string>();

#ifndef EVP_CTRL_GCM_GET_TAG
            if (mode == crypto::aes256GCMName) {
                return {ErrorCodes::InvalidOptions, "Server not compiled with GCM support"};
            }
#endif
            if (!(mode == crypto::aes256CBCName || mode == crypto::aes256GCMName)) {
                return {ErrorCodes::InvalidOptions, "Cipher mode unrecognized"};
            }
        }
    }
    return Status::OK();
}

Status storeEncryptionOptions(const moe::Environment& params) {
    Status validate = validateEncryptionOptions(params);
    if (!validate.isOK()) {
        return validate;
    }

    if (params.count("security.enableEncryption")) {
        encryptionGlobalParams.enableEncryption = params["security.enableEncryption"].as<bool>();
    }

    if (params.count("security.kmip.keyIdentifier")) {
        encryptionGlobalParams.kmipKeyIdentifier =
            params["security.kmip.keyIdentifier"].as<std::string>();
    }

    if (params.count("security.kmip.serverName")) {
        encryptionGlobalParams.kmipServerName =
            params["security.kmip.serverName"].as<std::string>();
    }

    if (params.count("security.kmip.port")) {
        encryptionGlobalParams.kmipPort = params["security.kmip.port"].as<int>();
    } else {
        encryptionGlobalParams.kmipPort = 5696;
    }

    if (params.count("security.kmip.clientCertificateFile")) {
        encryptionGlobalParams.kmipClientCertificateFile =
            params["security.kmip.clientCertificateFile"].as<std::string>();
    }

    if (params.count("security.kmip.clientCertificatePassword")) {
        encryptionGlobalParams.kmipClientCertificatePassword =
            params["security.kmip.clientCertificatePassword"].as<std::string>();
    }

    if (params.count("security.kmip.serverCAFile")) {
        encryptionGlobalParams.kmipServerCAFile =
            params["security.kmip.serverCAFile"].as<std::string>();
    }

    if (params.count("security.encryptionKeyFile")) {
        encryptionGlobalParams.encryptionKeyFile =
            params["security.encryptionKeyFile"].as<std::string>();
    }

    if (params.count("security.encryptionCipherMode")) {
        encryptionGlobalParams.encryptionCipherMode =
            params["security.encryptionCipherMode"].as<std::string>();
    }
    return Status::OK();
}
}  // namespace mongo
