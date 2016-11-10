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
MONGO_INITIALIZER_WITH_PREREQUISITES(CreateEncryptionKeyManager,
                                     ("CreateKeyEntropySource",
                                      "SecureAllocator",
                                      "SetWiredTigerCustomizationHooks",
                                      "SetWiredTigerExtensions",
                                      "SetGlobalEnvironment",
                                      "SSLManager",
                                      "WiredTigerEngineInit"))
(InitializerContext* context) {
    // Reset the WiredTigerCustomizationHooks pointer to be the EncryptionKeyManager
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
        WiredTigerCustomizationHooks::set(getGlobalServiceContext(), std::move(keyManager));
        WiredTigerExtensions::get(getGlobalServiceContext())
            ->addExtension(mongo::kEncryptionEntrypointConfig);
    }

    return Status::OK();
}
}
