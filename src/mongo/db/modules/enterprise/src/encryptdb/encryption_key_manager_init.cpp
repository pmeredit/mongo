/**
 * Copyright (C) 2015 MongoDB Inc.
 * The main KMIP API that is exposed to the rest of the codebase.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "encryption_key_manager.h"
#include "encryption_options.h"
#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/net/ssl_options.h"
#include "symmetric_crypto_smoke.h"

namespace mongo {
// XXX: The customization hook mechanism only supports a single customizer. That is enough
// for now, since the two enterprise modules that configure customization hooks (encryption
// and in-memory) are mutually exclusive.
MONGO_INITIALIZER_WITH_PREREQUISITES(CreateEncryptionWiredTigerCustomizationHooks,
                                     ("SetWiredTigerCustomizationHooks",
                                      "SetWiredTigerExtensions",
                                      "SetGlobalEnvironment",
                                      "WiredTigerEngineInit"))
(InitializerContext* context) {
    // Reset the WiredTigerCustomizationHooks pointer
    if (encryptionGlobalParams.enableEncryption) {
        auto configHooks =
            stdx::make_unique<EncryptionWiredTigerCustomizationHooks>(&encryptionGlobalParams);
        WiredTigerCustomizationHooks::set(getGlobalServiceContext(), std::move(configHooks));
    }

    return Status::OK();
}

MONGO_INITIALIZER_WITH_PREREQUISITES(CreateEncryptionKeyManager,
                                     ("CreateKeyEntropySource",
                                      "SecureAllocator",
                                      "SetEncryptionHooks",
                                      "SetWiredTigerCustomizationHooks",
                                      "SetWiredTigerExtensions",
                                      "SetGlobalEnvironment",
                                      "SSLManager",
                                      "WiredTigerEngineInit"))
(InitializerContext* context) {
    // Reset the EncryptionHooks pointer to be the EncryptionKeyManager
    if (encryptionGlobalParams.enableEncryption) {
        // Verify that encryption algorithms are functioning
        Status implementationStatus =
            crypto::smokeTestAESCipherMode(encryptionGlobalParams.encryptionCipherMode);
        if (!implementationStatus.isOK()) {
            return Status(implementationStatus.code(),
                          str::stream() << "Validation of cryptographic functions for "
                                        << encryptionGlobalParams.encryptionCipherMode
                                        << " failed: "
                                        << implementationStatus.reason());
        }

        auto keyManager = stdx::make_unique<EncryptionKeyManager>(
            storageGlobalParams.dbpath, &encryptionGlobalParams, &sslGlobalParams);
        EncryptionHooks::set(getGlobalServiceContext(), std::move(keyManager));
        WiredTigerExtensions::get(getGlobalServiceContext())
            ->addExtension(mongo::kEncryptionEntrypointConfig);
    }

    return Status::OK();
}
}
