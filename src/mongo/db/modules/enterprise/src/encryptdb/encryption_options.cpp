/*
 *    Copyright (C) 2015 MongoDB Inc.
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include "encryption_options.h"

#include <openssl/evp.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "symmetric_crypto.h"

namespace mongo {

EncryptionGlobalParams encryptionGlobalParams;

namespace {
Status addEncryptionOptions(moe::OptionSection* options) {
    moe::OptionSection encryptionOptions("Encryption at rest options");

    encryptionOptions.addOptionChaining(
        "security.enableEncryption", "enableEncryption", moe::Switch, "Enable encryption at rest");
    encryptionOptions
        .addOptionChaining("security.encryptionKeyFile",
                           "encryptionKeyFile",
                           moe::String,
                           "File path for encryption key file")
        .requires("security.enableEncryption");
    encryptionOptions
        .addOptionChaining("security.encryptionCipherMode",
                           "encryptionCipherMode",
                           moe::String,
                           "Cipher mode to use for encryption at rest")
        .requires("security.enableEncryption")
        .format("(:?AES256-CBC)|(:?AES256-GCM)", "'AES256-CBC' or 'AES256-GCM'");
    encryptionOptions.addOptionChaining("security.kmip.rotateMasterKey",
                                        "kmipRotateMasterKey",
                                        moe::Switch,
                                        "Rotate master encryption key");
    addKMIPOptions(&encryptionOptions);

    Status ret = options->addSection(encryptionOptions);
    if (!ret.isOK()) {
        log() << "Failed to add encryption option section: " << ret.toString();
        return ret;
    }
    return Status::OK();
}

static Status validateEncryptionOptions(const moe::Environment& params) {
    if (params.count("security.enableEncryption")) {
        if (params.count("storage.engine")) {
            std::string storageEngine = params["storage.engine"].as<std::string>();
            if (storageEngine != "wiredTiger") {
                return {ErrorCodes::InvalidOptions,
                        str::stream() << "Storage engine " << storageEngine
                                      << " specified, encryption at rest requires wiredTiger."};
            }
        }

        if (params.count("security.encryptionKeyFile")) {
            if (params.count("security.kmip.serverName") ||
                params.count("security.kmip.keyIdentifier")) {
                return {ErrorCodes::InvalidOptions,
                        "Must specify either an encryption key file or "
                        "a KMIP server when enabling file encryption"};
            } else if (params.count("security.kmip.rotateMasterKey")) {
                return {ErrorCodes::InvalidOptions, "Key rotation is only supported for KMIP keys"};
            }
        }

        if (params.count("security.kmip.serverName") &&
            !params.count("security.kmip.serverCAFile")) {
            return {ErrorCodes::InvalidOptions,
                    "Please specify a kmipServerCAFile parameter to validate the KMIP server's "
                    "certificate"};
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

    encryptionGlobalParams.kmipParams = parseKMIPOptions(params);

    if (params.count("security.enableEncryption")) {
        encryptionGlobalParams.enableEncryption = params["security.enableEncryption"].as<bool>();
    }

    if (params.count("security.kmip.rotateMasterKey")) {
        encryptionGlobalParams.rotateMasterKey = true;
        encryptionGlobalParams.kmipKeyIdentifierRot =
            encryptionGlobalParams.kmipParams.kmipKeyIdentifier;
        encryptionGlobalParams.kmipParams.kmipKeyIdentifier = "";
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
}  // namespace

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(EncryptionOptions)(InitializerContext* context) {
    return addEncryptionOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_STORE(EncryptionOptions)(InitializerContext* context) {
    return storeEncryptionOptions(moe::startupOptionsParsed);
}

}  // namespace mongo
