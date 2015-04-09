/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "symmetric_crypto.h"

#include <openssl/evp.h>

#include "mongo/util/assert_util.h"

namespace mongo {
namespace crypto {

    // TODO Andreas: keep mapped list of EVP_CIPHER_CTX for the different databases
    // instead of re-scheduling the key on each encrypt/decrypt
    // TODO Andreas: implement support for GCM
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

        std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_cleanup)>
            cryptoCtx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_cleanup);

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

        if (1 != EVP_EncryptInit_ex(cryptoCtx.get(), cipher, nullptr, key, iv)) {
            return Status(ErrorCodes::InternalError, "EVP_EncryptInit_ex failed");
        }

        int len = 0;
        if(1 != EVP_EncryptUpdate(cryptoCtx.get(), out, &len, in, inLen)) {
            return Status(ErrorCodes::InternalError, "EVP_EncryptUpdate failed");
        }

        int extraLen = 0;
        if(1 != EVP_EncryptFinal_ex(cryptoCtx.get(), out + len, &extraLen)) {
            return Status(ErrorCodes::InternalError, "EVP_EncryptFinal failed");
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

        std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_cleanup)>
            cryptoCtx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_cleanup);

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

        if (1 != EVP_DecryptInit_ex(cryptoCtx.get(), cipher, nullptr, key, iv)) {
            return Status(ErrorCodes::InternalError, "EVP_DecryptInit_ex failed");
        }

        int len = 0;
        if(1 != EVP_DecryptUpdate(cryptoCtx.get(), out, &len, in, inLen)) {
            return Status(ErrorCodes::InternalError, "EVP_DecryptUpdate failed");
        }

        int extraLen = 0;
        if(1 != EVP_DecryptFinal_ex(cryptoCtx.get(), out + len, &extraLen)) {
            return Status(ErrorCodes::InternalError, "EVP_DecryptFinal failed");
        }

        *outLen = len + extraLen;
        return Status::OK();
    }
} // namespace crypto
} // namespace mongo
