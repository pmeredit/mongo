/*
 *    Copyright (C) 2015 MongoDB Inc.
 */
#include "mongo/platform/basic.h"

#include "encryption_options.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "symmetric_crypto.h"

namespace moe = mongo::optionenvironment;

namespace mongo {

EncryptionGlobalParams encryptionGlobalParams;

Status validateCipherModeOption(const std::string& mode) {
    if ((mode != "AES256-CBC") && (mode != "AES256-GCM")) {
        return {ErrorCodes::BadValue, "cipherMode must be one of 'AES256-CBC' or 'AES256-GCM'"};
    }
    return Status::OK();
}

namespace {
Status validateEncryptionOptions(const moe::Environment& params) {
    if (params.count("security.enableEncryption")) {
        if (params.count("storage.engine")) {
            std::string storageEngine = params["storage.engine"].as<std::string>();
            if (storageEngine != "wiredTiger" && storageEngine != "queryable_wt") {
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
            !params.count("security.kmip.clientCertificateSelector") &&
            !params.count("security.kmip.serverCAFile")) {
            return {ErrorCodes::InvalidOptions,
                    "Please specify a kmipServerCAFile or a kmmipClientCertificateSelector "
                    "parameter to validate the KMIP server's "
                    "certificate"};
        }

        if (params.count("security.encryptionCipherMode")) {
            std::string mode = params["security.encryptionCipherMode"].as<std::string>();

            if (crypto::getSupportedSymmetricAlgorithms().count(mode) == 0) {
                return {ErrorCodes::InvalidOptions,
                        str::stream() << "Server not compiled with " << mode << " support"};
            }
        }
    }
    return Status::OK();
}
}  // namespace

MONGO_STARTUP_OPTIONS_STORE(EncryptionOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    Status validate = validateEncryptionOptions(params);
    if (!validate.isOK()) {
        return validate;
    }

    auto swKmipParams = parseKMIPOptions(params);
    if (!swKmipParams.isOK()) {
        return swKmipParams.getStatus();
    }
    encryptionGlobalParams.kmipParams = std::move(swKmipParams.getValue());

    if (params.count("security.kmip.rotateMasterKey") &&
        params["security.kmip.rotateMasterKey"].as<bool>()) {
        encryptionGlobalParams.rotateMasterKey = true;
        encryptionGlobalParams.kmipKeyIdentifierRot =
            encryptionGlobalParams.kmipParams.kmipKeyIdentifier;
        encryptionGlobalParams.kmipParams.kmipKeyIdentifier = "";
    }

    if (params.count("storage.queryableBackupMode")) {
        encryptionGlobalParams.readOnlyMode = params["storage.queryableBackupMode"].as<bool>();
    }

    return Status::OK();
}

}  // namespace mongo
