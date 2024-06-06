/*
 *    Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"

#include "kmip/kmip_options.h"

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

    // StorageGlobalParams.repair option
    bool repair;
};

extern EncryptionGlobalParams encryptionGlobalParams;
Status validateCipherModeOption(const std::string&);

}  // namespace mongo
