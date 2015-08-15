/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"

namespace mongo {
class Status;

namespace optionenvironment {
class OptionSection;
class Environment;
}  // namespace optionenvironment

namespace moe = mongo::optionenvironment;

struct EncryptionGlobalParams {
    bool enableEncryption = false;
    std::string encryptionCipherMode = "AES256-CBC";  // The default cipher mode

    // KMIP Options.
    std::string kmipKeyIdentifier;
    std::string kmipServerName;
    int kmipPort = 5696;
    std::string kmipClientCertificateFile;
    std::string kmipClientCertificatePassword;
    std::string kmipServerCAFile;

    // Keyfile Options.
    std::string encryptionKeyFile;
};

extern EncryptionGlobalParams encryptionGlobalParams;

Status addEncryptionOptions(moe::OptionSection* options);
Status storeEncryptionOptions(const moe::Environment& params);
}  // namespace mongo
