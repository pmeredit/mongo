/**
 * Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 * The main KMIP API that is exposed to the rest of the codebase.
 */


#include "mongo/platform/basic.h"

#include "encryption_key_manager.h"

#include <memory>

#include "encryption_options.h"
#include "mongo/base/init.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/util/net/ssl_options.h"
#include "symmetric_crypto_smoke.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {
namespace {
// XXX: The customization hook mechanism only supports a single customizer. That is enough
// for now, since the two enterprise modules that configure customization hooks (encryption
// and in-memory) are mutually exclusive.
ServiceContext::ConstructorActionRegisterer registerEncryptionWiredTigerCustomizationHooks{
    "CreateEncryptionWiredTigerCustomizationHooks",
    {"SetWiredTigerCustomizationHooks", "WiredTigerEngineInit", "SecureAllocator", "SSLManager"},
    [](ServiceContext* service) {
        if (!encryptionGlobalParams.enableEncryption) {
            return;
        }
        auto configHooks =
            std::make_unique<EncryptionWiredTigerCustomizationHooks>(&encryptionGlobalParams);
        WiredTigerCustomizationHooks::set(service, std::move(configHooks));
        const auto cipherMode =
            crypto::getCipherModeFromString(encryptionGlobalParams.encryptionCipherMode);

#ifdef _WIN32
        uassert(ErrorCodes::BadValue,
                "Only AES256-CBC is supported on Windows",
                cipherMode == crypto::aesMode::cbc);
#endif  // _WIN32
        uassertStatusOKWithContext(
            crypto::smokeTestAESCipherMode(cipherMode, crypto::PageSchema::k0),
            str::stream() << "Validation of cryptographic functions for "
                          << encryptionGlobalParams.encryptionCipherMode << " failed");

        initializeEncryptionKeyManager(service);
        WiredTigerExtensions::get(service)->addExtension(mongo::kEncryptionEntrypointConfig);
    }};
}  // namespace
}  // namespace mongo
