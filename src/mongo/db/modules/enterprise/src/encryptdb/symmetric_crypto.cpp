/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "symmetric_crypto.h"

#include <openssl/evp.h>

#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_manager.h"

namespace mongo {
namespace crypto {

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

        const EVP_CIPHER* cipher = nullptr;
        if (keySize == sym256KeySize) {
            cipher = EVP_aes_256_cbc();
        }
        else if (keySize == sym128KeySize) {
            cipher = EVP_aes_128_cbc();
        }
        else {
            return Status(ErrorCodes::BadValue, str::stream() <<
                    "Invalid key length: " << keySize);
        }

        std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_cleanup)>
                        encryptCtx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_cleanup);

        if (1 != EVP_EncryptInit_ex(encryptCtx.get(), cipher, nullptr, key, iv)) {
            return Status(ErrorCodes::UnknownError, str::stream() <<
                    getSSLManager()->getSSLErrorMessage(ERR_get_error()));
        }

        int len = 0;
        if (1 != EVP_EncryptUpdate(encryptCtx.get(), out, &len, in, inLen)) {
            return Status(ErrorCodes::UnknownError, str::stream() <<
                    getSSLManager()->getSSLErrorMessage(ERR_get_error()));
        }

        int extraLen = 0;
        if (1 != EVP_EncryptFinal_ex(encryptCtx.get(), out + len, &extraLen)) {
            return Status(ErrorCodes::UnknownError, str::stream() <<
                    getSSLManager()->getSSLErrorMessage(ERR_get_error()));
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

        const EVP_CIPHER* cipher = nullptr;
        if (keySize == sym256KeySize) {
            cipher = EVP_aes_256_cbc();
        }
        else if (keySize == sym128KeySize) {
            cipher = EVP_aes_128_cbc();
        }
        else {
            return Status(ErrorCodes::BadValue, "Invalid key length");
        }

        std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_cleanup)>
                        decryptCtx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_cleanup);

        if (1 != EVP_DecryptInit_ex(decryptCtx.get(), cipher, nullptr, key, iv)) {
            return Status(ErrorCodes::UnknownError, str::stream() <<
                    getSSLManager()->getSSLErrorMessage(ERR_get_error()));
        }

        int len = 0;
        if (1 != EVP_DecryptUpdate(decryptCtx.get(), out, &len, in, inLen)) {
            return Status(ErrorCodes::UnknownError, str::stream() <<
                    getSSLManager()->getSSLErrorMessage(ERR_get_error()));
        }

        int extraLen = 0;
        if (1 != EVP_DecryptFinal_ex(decryptCtx.get(), out + len, &extraLen)) {
            return Status(ErrorCodes::UnknownError, str::stream() <<
                    getSSLManager()->getSSLErrorMessage(ERR_get_error()));
        }

        *outLen = len + extraLen;
        return Status::OK();
    }
} // namespace crypto
} // namespace mongo
