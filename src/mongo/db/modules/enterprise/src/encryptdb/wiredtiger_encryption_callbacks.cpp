/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include <iostream>
#include <string>

#include "encryption_key_manager.h"
#include "encryption_options.h"
#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/random.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "symmetric_crypto.h"

namespace mongo {

namespace {
std::unique_ptr<PseudoRandom> prng;
MONGO_INITIALIZER(SeedPRNG)(InitializerContext* context) {
    std::unique_ptr<SecureRandom> sr(SecureRandom::create());
    prng.reset(new PseudoRandom(sr->nextInt64()));
    return Status::OK();
}

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
 * terminate - Called when a WT_ENCRYPTOR is no longer used and its
 *             resources can be freed.
 *
 * The ExtendedWTEncryptor struct is used by the customize function
 * to store data that WiredTiger does not need to know about, but will
 * store for us. WiredTiger will cast the ExtendedWTEncryptor* to a
 * WT_ENCRYPTOR* and ignore any other members.
 *
 * The stored ExtendedWTEncryptor* will be passed back in the encrypt,
 * decrypt and terminate callbacks.
 */
struct ExtendedWTEncryptor {
    WT_ENCRYPTOR encryptor;      // Must come first
    SymmetricKey* symmetricKey;  // Symmetric key
    uint8_t cipherMode;          // Cipher mode
};

int sizing(WT_ENCRYPTOR* encryptor, WT_SESSION* session, size_t* expansionConstant) {
    try {
        // Add a constant factor for the iv + padding block
        *expansionConstant = 2 * crypto::aesBlockSize;
        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::sizing: " << exceptionToStatus();
        fassertFailed(4037);
    }
}

int customize(WT_ENCRYPTOR* encryptor,
              WT_SESSION* session,
              WT_CONFIG_ARG* encryptConfig,
              WT_ENCRYPTOR** customEncryptor) {
    try {
        WT_EXTENSION_API* extApi = session->connection->get_extension_api(session->connection);

        const ExtendedWTEncryptor* origEncryptor =
            reinterpret_cast<const ExtendedWTEncryptor*>(encryptor);
        std::unique_ptr<ExtendedWTEncryptor> myEncryptor(new ExtendedWTEncryptor);

        // Get the key id from the encryption configuration
        WT_CONFIG_ITEM keyIdItem;
        if (0 != extApi->config_get(extApi, session, encryptConfig, "keyid", &keyIdItem) ||
            keyIdItem.len == 0) {
            error() << "Unable to retrieve keyid when customizing encryptor";
            return EINVAL;
        }
        std::string keyId = std::string(keyIdItem.str, keyIdItem.len);

        // Get the symmetric key for the key id
        StatusWith<std::unique_ptr<SymmetricKey>> swSymmetricKey =
            EncryptionKeyManager::get(getGlobalServiceContext())->getKey(keyId);
        if (!swSymmetricKey.isOK()) {
            error() << "Unable to initialize encryption. " << swSymmetricKey.getStatus().reason();
            return EINVAL;
        }
        *(myEncryptor.get()) = *origEncryptor;

        myEncryptor.get()->symmetricKey = swSymmetricKey.getValue().release();
        myEncryptor.get()->cipherMode = crypto::cbcMode;

        *customEncryptor = &(myEncryptor.release()->encryptor);
        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::customize: " << exceptionToStatus();
        fassertFailed(4045);
    }
}

int encrypt(WT_ENCRYPTOR* encryptor,
            WT_SESSION* session,
            uint8_t* src,
            size_t srcLen,
            uint8_t* dst,
            size_t dstLen,
            size_t* resultLen) {
    try {
        if (!src || !dst) {
            return EINVAL;
        }
        if (dstLen < srcLen + crypto::aesBlockSize) {
            return ENOMEM;
        }

        // Generate IV, we only need something reasonably unpredictable
        int64_t iv[2] = {prng->nextInt64(), prng->nextInt64()};
        memcpy(dst, &iv[0], crypto::aesBlockSize);

        const ExtendedWTEncryptor* crypto = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);
        *resultLen = dstLen;
        Status ret = crypto::aesEncrypt(src,
                                        srcLen,
                                        crypto->symmetricKey->getKey(),
                                        crypto->symmetricKey->getKeySize(),
                                        crypto->cipherMode,
                                        reinterpret_cast<uint8_t*>(iv),
                                        &dst[crypto::aesBlockSize],
                                        resultLen);

        if (!ret.isOK()) {
            log() << "encrypt error: " << ret;
            return EINVAL;
        }
        // Check the returned length, including block size padding
        if (*resultLen != crypto::aesBlockSize * (1 + srcLen / crypto::aesBlockSize)) {
            log() << "encrypt error, expected cipher text of length "
                  << crypto::aesBlockSize * (1 + srcLen / crypto::aesBlockSize) << "] but found "
                  << *resultLen;
            return EINVAL;
        }

        *resultLen += crypto::aesBlockSize;

        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::encrypt: " << exceptionToStatus();
        fassertFailed(4046);
    }
}

int decrypt(WT_ENCRYPTOR* encryptor,
            WT_SESSION* session,
            uint8_t* src,
            size_t srcLen,
            uint8_t* dst,
            size_t dstLen,
            size_t* resultLen) {
    try {
        if (!src || !dst) {
            return EINVAL;
        }

        const ExtendedWTEncryptor* crypto = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);
        *resultLen = dstLen;
        Status ret = crypto::aesDecrypt(&src[crypto::aesBlockSize],
                                        srcLen - crypto::aesBlockSize,
                                        crypto->symmetricKey->getKey(),
                                        crypto->symmetricKey->getKeySize(),
                                        crypto->cipherMode,
                                        &src[0],
                                        dst,
                                        resultLen);

        if (!ret.isOK()) {
            log() << "decrypt error: " << ret;
            return EINVAL;
        }
        // Check the returned length, excluding IV and removed block size padding
        if (*resultLen < srcLen - 2 * crypto::aesBlockSize ||
            *resultLen > srcLen - crypto::aesBlockSize) {
            log() << "encrypt error, expected clear text length in interval ["
                  << srcLen - 2 * crypto::aesBlockSize << "," << srcLen - crypto::aesBlockSize
                  << " but found " << *resultLen;
            return EINVAL;
        }

        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::decrypt: " << exceptionToStatus();
        fassertFailed(4047);
    }
}

int destroyEncryptor(WT_ENCRYPTOR* encryptor, WT_SESSION* session) {
    try {
        ExtendedWTEncryptor* myEncryptor = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);

        if (myEncryptor) {
            // Destroy the SymmetricKey object
            if (myEncryptor->symmetricKey) {
                delete myEncryptor->symmetricKey;
            }
            delete myEncryptor;
        }
        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::terminate: " << exceptionToStatus();
        fassertFailed(4048);
    }
}

}  // namespace
}  // namespace mongo

/**
 * mongo_addWiredTigerEncryptors is the entry point for the WiredTiger crypto
 * callback API. WiredTiger uses dlsym to identify the entry point based on
 * configuration parameters to the wiredtiger_open call.
 *
 * This function adds a single AES encryptor. The API is flexible enough to
 * support multiple different encryptors but at the moment we are using a single one.
 */
extern "C" MONGO_COMPILER_API_EXPORT int mongo_addWiredTigerEncryptors(WT_CONNECTION* connection) {
    mongo::ExtendedWTEncryptor* extWTEncryptor = new mongo::ExtendedWTEncryptor;
    extWTEncryptor->symmetricKey = nullptr;
    extWTEncryptor->encryptor.sizing = mongo::sizing;
    extWTEncryptor->encryptor.customize = mongo::customize;
    extWTEncryptor->encryptor.encrypt = mongo::encrypt;
    extWTEncryptor->encryptor.decrypt = mongo::decrypt;
    extWTEncryptor->encryptor.terminate = mongo::destroyEncryptor;
    int ret;
    if (mongo::encryptionGlobalParams.enableEncryption &&
        (ret = connection->add_encryptor(
             connection, "aes", reinterpret_cast<WT_ENCRYPTOR*>(extWTEncryptor), nullptr)) != 0) {
        return ret;
    }

    return 0;
}
