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
#include "mongo/db/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/net/ssl_options.h"

namespace mongo {
MONGO_INITIALIZER_WITH_PREREQUISITES(CreateEncryptionKeyManager,
                                     ("SetWiredTigerCustomizationHooks",
                                      "SetGlobalEnvironment",
                                      "SSLManager",
                                      "WiredTigerEngineInit"))
(InitializerContext* context) {
    // Reset the WiredTigerCustomizationHooks pointer to be the EncryptionKeyManager
    if (encryptionGlobalParams.enableEncryption) {
        auto keyManager = stdx::make_unique<EncryptionKeyManager>(
            storageGlobalParams.dbpath, &encryptionGlobalParams, &sslGlobalParams);
        WiredTigerCustomizationHooks::set(getGlobalServiceContext(), std::move(keyManager));
    }

    return Status::OK();
}
}
