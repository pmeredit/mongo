/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"

#include "kmip_options.h"

namespace mongo {

struct EncryptionGlobalParams {
    bool enableEncryption = false;
    std::string encryptionCipherMode = "AES256-CBC";  // The default cipher mode

    // Keyfile Options.
    std::string encryptionKeyFile;

    // KMIP Options.
    KMIPParams kmipParams;

    // Master key rotation options
    bool rotateMasterKey = false;
    bool rotateDatabaseKeys = false;
    std::string kmipKeyIdentifierRot;

    // Whether the keystore should be opened read only.
    // This is distinct from the global readOnly flag which can be toggled
    // after startup. If this occurs, writes may still occur in the storage layer.
    bool readOnlyMode = false;
};

extern EncryptionGlobalParams encryptionGlobalParams;
Status validateCipherModeOption(const std::string&);

}  // namespace mongo
