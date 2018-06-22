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
namespace {
// XXX: The customization hook mechanism only supports a single customizer. That is enough
// for now, since the two enterprise modules that configure customization hooks (encryption
// and in-memory) are mutually exclusive.
ServiceContext::ConstructorActionRegisterer registerEncryptionWiredTigerCustomizationHooks{
    "CreateEncryptionWiredTigerCustomizationHooks",
    {"SetWiredTigerCustomizationHooks",
     "WiredTigerEngineInit",
     "SecureAllocator",
     "CreateKeyEntropySource",
     "SSLManager"},
    [](ServiceContext* service) {
        if (!encryptionGlobalParams.enableEncryption) {
            return;
        }
        auto configHooks =
            stdx::make_unique<EncryptionWiredTigerCustomizationHooks>(&encryptionGlobalParams);
        WiredTigerCustomizationHooks::set(service, std::move(configHooks));
        uassertStatusOKWithContext(
            crypto::smokeTestAESCipherMode(encryptionGlobalParams.encryptionCipherMode),
            str::stream() << "Validation of cryptographic functions for "
                          << encryptionGlobalParams.encryptionCipherMode
                          << " failed");

        initializeEncryptionKeyManager(service);
        WiredTigerExtensions::get(service)->addExtension(mongo::kEncryptionEntrypointConfig);
    }};
}  // namespace
}  // namespace mongo
