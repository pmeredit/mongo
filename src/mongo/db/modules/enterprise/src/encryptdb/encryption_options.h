/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

#include "kmip_options.h"

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

    // Keyfile Options.
    std::string encryptionKeyFile;

    // KMIP Options.
    KMIPParams kmipParams;

    // Master key rotation options
    bool rotateMasterKey = false;
    std::string kmipKeyIdentifierRot;
};

extern EncryptionGlobalParams encryptionGlobalParams;
}  // namespace mongo
