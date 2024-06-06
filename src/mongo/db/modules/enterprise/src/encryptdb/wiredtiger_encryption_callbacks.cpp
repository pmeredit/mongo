/**
 *    Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include <iostream>
#include <memory>
#include <string>
#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include "encryption_key_manager.h"
#include "encryption_options.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/net/ssl_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {

namespace {

/**
 * The WiredTiger encryption API consists of the following callbacks:
 *
 * sizing    - Return the maximum padding an encryption operation might
 *             add to the plaintext data.
 *
 * customize - Customize a WT_ENCRYPTOR struct for a specific keyid with
 *             the correct key.
 *
 * encrypt   - Encrypt data.
 *
 * decrypt   - Decrypt data.
 *
 * destroyEncryptor - Called when a WT_ENCRYPTOR is no longer used and its
 *                    resources can be freed.
 *
 * The ExtendedWTEncryptor struct is used by the customize function
 * to store data that WiredTiger does not need to know about, but will
 * store for us. WiredTiger will cast the ExtendedWTEncryptor* to a
 * WT_ENCRYPTOR* and ignore any other members.
 *
 * The stored ExtendedWTEncryptor* will be passed back in the encrypt,
 * decrypt and destroyEncryptor callbacks.
 */
struct ExtendedWTEncryptor {
    WT_ENCRYPTOR encryptor;      // Must come first
    UniqueSymmetricKey dbKey;    // Symmetric key
    crypto::aesMode cipherMode;  // Cipher mode
};

// Called if the system key is destroyed, so we know we need to re-initialize when we re-open.
bool systemKeyDestroyed = false;

int sizing(WT_ENCRYPTOR* encryptor, WT_SESSION* session, size_t* expansionConstant) noexcept {
    // Add a constant factor for WiredTiger to know how much extra memory it must allocate,
    // at most, for encryption.
    // The modes have different amounts of overhead. Use the largest (currently GCM-v1).
    *expansionConstant = crypto::kMaxHeaderSize;
    return 0;
}

// Guard access to customize with a mutex
// It must be a recursive mutext since this code can call back in to WT itself.
stdx::recursive_mutex encryptedCustomizeMutex;

int customize(WT_ENCRYPTOR* encryptor,
              WT_SESSION* session,
              WT_CONFIG_ARG* encryptConfig,
              WT_ENCRYPTOR** customEncryptor) noexcept {
    stdx::lock_guard<stdx::recursive_mutex> lock(encryptedCustomizeMutex);

    WT_EXTENSION_API* extApi = session->connection->get_extension_api(session->connection);

    const auto* origEncryptor = reinterpret_cast<const ExtendedWTEncryptor*>(encryptor);
    std::unique_ptr<ExtendedWTEncryptor> myEncryptor(new ExtendedWTEncryptor);

    // Get the key id from the encryption configuration
    WT_CONFIG_ITEM keyIdItem;
    if (0 != extApi->config_get(extApi, session, encryptConfig, "keyid", &keyIdItem) ||
        keyIdItem.len == 0) {
        LOGV2_ERROR(24246, "Unable to retrieve keyid when customizing encryptor");
        return EINVAL;
    }
    std::string keyId(keyIdItem.str, keyIdItem.len);

    // Get the name from the encryption configuration
    WT_CONFIG_ITEM encryptorNameItem;
    if (0 != extApi->config_get(extApi, session, encryptConfig, "name", &encryptorNameItem) ||
        encryptorNameItem.len == 0) {
        LOGV2_ERROR(24247, "Unable to retrieve name when customizing encryptor");
        return EINVAL;
    }

    std::string encryptorName(encryptorNameItem.str, encryptorNameItem.len);
    if (mongo::encryptionGlobalParams.encryptionCipherMode != encryptorName) {
        LOGV2_FATAL_CONTINUE(24250,
                             "Invalid cipher mode",
                             "actualCipherMode"_attr =
                                 mongo::encryptionGlobalParams.encryptionCipherMode,
                             "expectedCipherMode"_attr = encryptorName);
        return EINVAL;
    }

    // Get the symmetric key for the key id
    // For servers configured with KMIP this will make the request
    try {
        auto swSymmetricKey = EncryptionKeyManager::get(getGlobalServiceContext())->getKey(keyId);
        if (!swSymmetricKey.isOK()) {
            LOGV2_ERROR(24248,
                        "Unable to retrieve key",
                        "keyId"_attr = keyId,
                        "error"_attr = swSymmetricKey.getStatus());
            return EINVAL;
        }
        myEncryptor->encryptor = origEncryptor->encryptor;
        myEncryptor->dbKey = std::move(swSymmetricKey.getValue());
        myEncryptor->cipherMode = crypto::getCipherModeFromString(encryptorName);
    } catch (const ExceptionForCat<ErrorCategory::NetworkError>&) {
        LOGV2_ERROR(24249,
                    "Socket/network exception in WT_ENCRYPTOR::customize on getKey to KMIP "
                    "server",
                    "error"_attr = exceptionToStatus());
        return EINVAL;
    }

    *customEncryptor = reinterpret_cast<WT_ENCRYPTOR*>(myEncryptor.release());
    return 0;
}

int encrypt(WT_ENCRYPTOR* encryptor,
            WT_SESSION* session,
            uint8_t* src,
            size_t srcLen,
            uint8_t* dst,
            size_t dstLen,
            size_t* resultLen) noexcept {
    const auto* crypto = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);
    if (!src || !dst || !resultLen || !crypto || !crypto->dbKey) {
        return EINVAL;
    }

    const auto mode = crypto->cipherMode;
    const auto* key = crypto->dbKey.get();
    const auto& keyId = key->getKeyId();

    auto schema = crypto::PageSchema::k0;
    if ((mode == crypto::aesMode::gcm) && (keyId.id()) &&
        EncryptionKeyManager::get(getGlobalServiceContext())->getKeystoreVersion()) {
        schema = crypto::PageSchema::k1;
    }

    Status ret = crypto::aesEncrypt(*key, mode, schema, src, srcLen, dst, dstLen, resultLen);
    if (!ret.isOK()) {
        LOGV2_FATAL_CONTINUE(
            24251, "Encrypt error for key", "keyId"_attr = keyId, "error"_attr = ret);
        return EINVAL;
    }

    return 0;
}

int decrypt(WT_ENCRYPTOR* encryptor,
            WT_SESSION* session,
            uint8_t* src,
            size_t srcLen,
            uint8_t* dst,
            size_t dstLen,
            size_t* resultLen) noexcept {
    const auto* crypto = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);
    if (!src || !dst || !resultLen || !crypto || !crypto->dbKey) {
        return EINVAL;
    }

    auto* key = crypto->dbKey.get();
    const auto mode = crypto->cipherMode;
    auto schema = crypto::PageSchema::k0;

    std::unique_ptr<SymmetricKey> pageKey;
    if (mode == crypto::aesMode::gcm) {
        SymmetricKeyId::id_type numericKeyFromPage;
        std::tie(schema, numericKeyFromPage) = crypto::parseGCMPageSchema(src, srcLen);

        auto pageKeyId = key->getKeyId();
        auto findMode = EncryptionKeyManager::FindMode::kById;
        if (schema == crypto::PageSchema::k1) {
            pageKeyId = SymmetricKeyId(pageKeyId.name(), numericKeyFromPage);
        } else {
            invariant(schema == crypto::PageSchema::k0);
            pageKeyId = SymmetricKeyId(pageKeyId.name());
            findMode = EncryptionKeyManager::FindMode::kOldest;
        }

        if (pageKeyId != key->getKeyId()) {
            // Fetch and use page-specific key instead of the current database key.
            auto swPageKey =
                EncryptionKeyManager::get(getGlobalServiceContext())->getKey(pageKeyId, findMode);
            if (!swPageKey.isOK()) {
                LOGV2_FATAL_CONTINUE(24252,
                                     "Unable to retrieve encryption key for page",
                                     "error"_attr = swPageKey.getStatus());
                return EINVAL;
            }

            pageKey = std::move(swPageKey.getValue());
            key = pageKey.get();
        }
    }

    Status ret = crypto::aesDecrypt(*key, mode, schema, src, srcLen, dst, dstLen, resultLen);

    if (!ret.isOK()) {
        if (key->getKeyId().name() == kSystemKeyId) {
            LOGV2_FATAL_CONTINUE(
                24253, "Decryption failed, invalid encryption master key or keystore encountered");
        } else {
            LOGV2_FATAL_CONTINUE(
                24254, "Decrypt error for key", "keyId"_attr = key->getKeyId(), "error"_attr = ret);
        }
        return EINVAL;
    }
    return 0;
}

int destroyEncryptor(WT_ENCRYPTOR* encryptor, WT_SESSION* session) noexcept {
    auto* myEncryptor = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);

    if (myEncryptor) {
        // Destroy the SymmetricKey object
        if (myEncryptor->dbKey) {
            if (myEncryptor->dbKey->getKeyId().name() == kSystemKeyId) {
                // With the destruction of the system key, the key manager is not needed
                // any more since the process is in shutdown. It may be used during feature
                // compatibility version downgrade so make sure a valid global encryption
                // manager is installed.
                initializeEncryptionKeyManager(getGlobalServiceContext());
                systemKeyDestroyed = true;
            }
        }
        delete myEncryptor;
    }

    return 0;
}


/**
 * mongo_addWiredTigerEncryptors is the entry point for the WiredTiger crypto
 * callback API. WiredTiger uses dlsym to identify the entry point based on
 * configuration parameters to the wiredtiger_open call.
 *
 * This function adds a single AES encryptor. The API is flexible enough to
 * support multiple different encryptors but at the moment we are using a single one.
 */
int mongo_addWiredTigerEncryptors_impl(WT_CONNECTION* connection) noexcept {
    if (!mongo::encryptionGlobalParams.enableEncryption) {
        LOGV2_FATAL_CONTINUE(24255, "Encrypted data files detected, please enable encryption");
        return EINVAL;
    }

    mongo::ExtendedWTEncryptor* extWTEncryptor = new mongo::ExtendedWTEncryptor;
    extWTEncryptor->dbKey = nullptr;
    extWTEncryptor->encryptor.sizing = mongo::sizing;
    extWTEncryptor->encryptor.customize = mongo::customize;
    extWTEncryptor->encryptor.encrypt = mongo::encrypt;
    extWTEncryptor->encryptor.decrypt = mongo::decrypt;
    extWTEncryptor->encryptor.terminate = mongo::destroyEncryptor;

    if (systemKeyDestroyed) {
        // Storage has been restarted, we need to re-initialize the encryption key manager.
        systemKeyDestroyed = false;
        initializeEncryptionKeyManager(getGlobalServiceContext());
    }

    return connection->add_encryptor(connection,
                                     mongo::encryptionGlobalParams.encryptionCipherMode.c_str(),
                                     reinterpret_cast<WT_ENCRYPTOR*>(extWTEncryptor),
                                     nullptr);
}

extern "C" MONGO_COMPILER_API_EXPORT int mongo_addWiredTigerEncryptors(WT_CONNECTION* connection) {
    return mongo_addWiredTigerEncryptors_impl(connection);
}
}  // namespace
}  // namespace mongo
