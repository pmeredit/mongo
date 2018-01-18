/**
 *    Copyright (C) 2018 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <openssl/rand.h>
#include <set>

#include "mongo/base/data_cursor.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_manager.h"

#include "symmetric_crypto.h"
#include "symmetric_key.h"

namespace mongo {
namespace crypto {

namespace {
template <typename Init>
void initCipherContext(
    EVP_CIPHER_CTX* ctx, const SymmetricKey& key, aesMode mode, const uint8_t* iv, Init init) {
    const auto keySize = key.getKeySize();
    const EVP_CIPHER* cipher = nullptr;
    if (keySize == sym256KeySize) {
        if (mode == crypto::aesMode::cbc) {
            cipher = EVP_get_cipherbyname("aes-256-cbc");
        } else if (mode == crypto::aesMode::gcm) {
            cipher = EVP_get_cipherbyname("aes-256-gcm");
        }
    }
    uassert(ErrorCodes::BadValue,
            str::stream() << "Unrecognized AES key size/cipher mode. Size: " << keySize << " Mode: "
                          << getStringFromCipherMode(mode),
            cipher);

    const bool initOk = (1 == init(ctx, cipher, nullptr, key.getKey(), iv));
    uassert(ErrorCodes::UnknownError,
            str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()),
            initOk);
}

class SymmetricEncryptorOpenSSL : public SymmetricEncryptor {
public:
    SymmetricEncryptorOpenSSL(const SymmetricKey& key, aesMode mode, const uint8_t* iv)
        : _ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free), _mode(mode) {
        initCipherContext(_ctx.get(), key, mode, iv, EVP_EncryptInit_ex);
    }

    StatusWith<size_t> update(const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen) final {
        int len = 0;
        if (1 != EVP_EncryptUpdate(_ctx.get(), out, &len, in, inLen)) {
            return Status(ErrorCodes::UnknownError,
                          str::stream()
                              << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
        }
        return static_cast<size_t>(len);
    }

    StatusWith<size_t> finalize(uint8_t* out, size_t outLen) final {
        int len = 0;
        if (1 != EVP_EncryptFinal_ex(_ctx.get(), out, &len)) {
            return Status(ErrorCodes::UnknownError,
                          str::stream()
                              << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
        }
        return static_cast<size_t>(len);
    }

    StatusWith<size_t> finalizeTag(uint8_t* out, size_t outLen) final {
        if (_mode == aesMode::gcm) {
#ifdef EVP_CTRL_GCM_GET_TAG
            if (1 != EVP_CIPHER_CTX_ctrl(_ctx.get(), EVP_CTRL_GCM_GET_TAG, outLen, out)) {
                return Status(ErrorCodes::UnknownError,
                              str::stream()
                                  << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
            }
            return crypto::aesGCMTagSize;
#else
            return Status(ErrorCodes::UnsupportedFormat, "GCM support is not available");
#endif
        }

        // Otherwise, not a tagged cipher mode, write nothing.
        return 0;
    }

private:
    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> _ctx;
    const aesMode _mode;
};

class SymmetricDecryptorOpenSSL : public SymmetricDecryptor {
public:
    SymmetricDecryptorOpenSSL(const SymmetricKey& key, aesMode mode, const uint8_t* iv)
        : _ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free), _mode(mode) {
        initCipherContext(_ctx.get(), key, mode, iv, EVP_DecryptInit_ex);
    }

    StatusWith<size_t> update(const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen) final {
        int len = 0;
        if (1 != EVP_DecryptUpdate(_ctx.get(), out, &len, in, inLen)) {
            return Status(ErrorCodes::UnknownError,
                          str::stream()
                              << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
        }
        return static_cast<size_t>(len);
    }

    StatusWith<size_t> finalize(uint8_t* out, size_t outLen) final {
        int len = 0;
        if (1 != EVP_DecryptFinal_ex(_ctx.get(), out, &len)) {
            return Status(ErrorCodes::UnknownError,
                          str::stream()
                              << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
        }
        return static_cast<size_t>(len);
    }

    Status updateTag(const uint8_t* tag, size_t tagLen) final {
        // validateEncryptionOption asserts that platforms without GCM will never start in GCM mode
        if (_mode == aesMode::gcm) {
#ifdef EVP_CTRL_GCM_GET_TAG
            if (1 != EVP_CIPHER_CTX_ctrl(
                         _ctx.get(), EVP_CTRL_GCM_SET_TAG, tagLen, const_cast<uint8_t*>(tag))) {
                return Status(ErrorCodes::UnknownError,
                              str::stream()
                                  << "Unable to set GCM tag: "
                                  << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
            }
#else
            return {ErrorCodes::UnsupportedFormat, "GCM support is not available"};
#endif
        } else if (tagLen != 0) {
            return {ErrorCodes::BadValue, "Unexpected tag for non-gcm cipher"};
        }

        return Status::OK();
    }

private:
    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> _ctx;
    const aesMode _mode;
};

}  // namespace

std::set<std::string> getSupportedSymmetricAlgorithms() {
#ifdef EVP_CTRL_GCM_GET_TAG
    return {aes256CBCName, aes256GCMName};
#else
    return {aes256CBCName};
#endif
}

Status engineRandBytes(uint8_t* buffer, size_t len) {
    if (RAND_bytes(reinterpret_cast<unsigned char*>(buffer), len) == 1) {
        return Status::OK();
    }
    return {ErrorCodes::UnknownError,
            str::stream() << "Unable to acquire random bytes from OpenSSL: "
                          << SSLManagerInterface::getSSLErrorMessage(ERR_get_error())};
}

StatusWith<std::unique_ptr<SymmetricEncryptor>> SymmetricEncryptor::create(const SymmetricKey& key,
                                                                           aesMode mode,
                                                                           const uint8_t* iv,
                                                                           size_t ivLen) try {
    std::unique_ptr<SymmetricEncryptor> encryptor =
        stdx::make_unique<SymmetricEncryptorOpenSSL>(key, mode, iv);
    return std::move(encryptor);
} catch (const DBException& e) {
    return e.toStatus();
}

StatusWith<std::unique_ptr<SymmetricDecryptor>> SymmetricDecryptor::create(const SymmetricKey& key,
                                                                           aesMode mode,
                                                                           const uint8_t* iv,
                                                                           size_t ivLen) try {
    std::unique_ptr<SymmetricDecryptor> decryptor =
        std::make_unique<SymmetricDecryptorOpenSSL>(key, mode, iv);
    return std::move(decryptor);
} catch (const DBException& e) {
    return e.toStatus();
}

}  // namespace crypto
}  // namespace mongo
