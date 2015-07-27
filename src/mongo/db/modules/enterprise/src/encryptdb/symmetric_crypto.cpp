/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "symmetric_crypto.h"

#include <openssl/evp.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_manager.h"

#include "symmetric_key.h"

namespace mongo {
namespace crypto {
namespace {
std::unique_ptr<SecureRandom> random;
}  // namespace

MONGO_INITIALIZER(CreateKeyEntropySource)(InitializerContext* context) {
    random = std::unique_ptr<SecureRandom>(SecureRandom::create());
    return Status::OK();
}

Status aesEncrypt(const uint8_t* in,
                  size_t inLen,
                  const uint8_t* key,
                  size_t keySize,
                  int mode,
                  const uint8_t* iv,
                  uint8_t* out,
                  size_t* outLen) {
    if (!(in && key && iv && out && outLen)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption buffers");
    }
    if (mode != cbcMode) {
        return Status(ErrorCodes::BadValue, "Invalid encryption mode");
    }
    if (*outLen < aesBlockSize * (1 + inLen / aesBlockSize)) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Cipher text buffer too short " << *outLen << " < "
                                    << aesBlockSize * (1 + inLen / aesBlockSize));
    }

    const EVP_CIPHER* cipher = nullptr;
    if (keySize == sym256KeySize) {
        cipher = EVP_aes_256_cbc();
    } else if (keySize == sym128KeySize) {
        cipher = EVP_aes_128_cbc();
    } else {
        return Status(ErrorCodes::BadValue, str::stream() << "Invalid key length: " << keySize);
    }

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> encryptCtx(EVP_CIPHER_CTX_new(),
                                                                               EVP_CIPHER_CTX_free);

    if (!encryptCtx.get()) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    if (1 != EVP_EncryptInit_ex(encryptCtx.get(), cipher, nullptr, key, iv)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int len = 0;
    if (1 != EVP_EncryptUpdate(encryptCtx.get(), out, &len, in, inLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int extraLen = 0;
    if (1 != EVP_EncryptFinal_ex(encryptCtx.get(), out + len, &extraLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    *outLen = len + extraLen;
    return Status::OK();
}

Status aesDecrypt(const uint8_t* in,
                  size_t inLen,
                  const uint8_t* key,
                  size_t keySize,
                  int mode,
                  const uint8_t* iv,
                  uint8_t* out,
                  size_t* outLen) {
    if (!(in && key && iv && out && outLen)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption buffers");
    }
    if (mode != cbcMode) {
        return Status(ErrorCodes::BadValue, "Invalid encryption mode");
    }
    if (*outLen < inLen - aesBlockSize) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Clear text buffer too short " << *outLen << " < "
                                    << inLen - aesBlockSize);
    }

    const EVP_CIPHER* cipher = nullptr;
    if (keySize == sym256KeySize) {
        cipher = EVP_aes_256_cbc();
    } else if (keySize == sym128KeySize) {
        cipher = EVP_aes_128_cbc();
    } else {
        return Status(ErrorCodes::BadValue, "Invalid key length");
    }

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> decryptCtx(EVP_CIPHER_CTX_new(),
                                                                               EVP_CIPHER_CTX_free);

    if (!decryptCtx.get()) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    if (1 != EVP_DecryptInit_ex(decryptCtx.get(), cipher, nullptr, key, iv)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int len = 0;
    if (1 != EVP_DecryptUpdate(decryptCtx.get(), out, &len, in, inLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int extraLen = 0;
    if (1 != EVP_DecryptFinal_ex(decryptCtx.get(), out + len, &extraLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    *outLen = len + extraLen;
    return Status::OK();
}

SymmetricKey aesGenerate(size_t keySize) {
    invariant(keySize == sym128KeySize || keySize == sym256KeySize);

    std::unique_ptr<std::uint8_t[]> keyArray = stdx::make_unique<std::uint8_t[]>(keySize);

    size_t offset = 0;
    while (offset < keySize) {
        std::uint64_t randomValue = random->nextInt64();
        memcpy(keyArray.get() + offset, &randomValue, sizeof(randomValue));
        offset += sizeof(randomValue);
    }

    return SymmetricKey(std::move(keyArray), keySize, aesAlgorithm);
}

}  // namespace crypto
}  // namespace mongo
