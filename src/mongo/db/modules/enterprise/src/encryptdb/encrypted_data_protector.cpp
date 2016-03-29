/**
 *  Copyright (C) 2015 MongoDB Inc.
 */
#include "mongo/platform/basic.h"

#include "encrypted_data_protector.h"

#include <cstring>

#include "mongo/base/status.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_manager.h"

#include "symmetric_crypto.h"
#include "symmetric_key.h"

namespace mongo {

EncryptedDataProtector::EncryptedDataProtector(const SymmetricKey* key, crypto::aesMode mode)
    : _key(key), _mode(mode), _encryptCtx(nullptr, &EVP_CIPHER_CTX_free), _bytesReservedForTag(0) {}

Status EncryptedDataProtector::protect(const std::uint8_t* in,
                                       std::size_t inLen,
                                       std::uint8_t* out,
                                       std::size_t outLen,
                                       std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (!_encryptCtx) {
        StatusWith<const EVP_CIPHER*> swCipher = acquireAESCipher(_key->getKeySize(), _mode);
        if (!swCipher.isOK()) {
            return swCipher.getStatus();
        }

        _encryptCtx = std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>(
            EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);

        // Add a 1 byte version token
        if (outLen < *bytesWritten + 1) {
            return Status(ErrorCodes::InvalidLength,
                          "EncryptedDataProtector attempted to reserve version field in "
                          "insufficiently sized buffer");
        }
        *out = DATA_PROTECTOR_VERSION_0;
        *bytesWritten += sizeof(DATA_PROTECTOR_VERSION_0);
        out += sizeof(DATA_PROTECTOR_VERSION_0);

        // Allocate space for a tag
        if (_mode == crypto::aesMode::gcm) {
            if (outLen < *bytesWritten + crypto::aesGCMTagSize) {
                return Status(ErrorCodes::InvalidLength,
                              "EncryptedDataProtector attempted to reserve tag in insufficiently "
                              "sized buffer");
            }
            memset(out, 0xFF, crypto::aesGCMTagSize);
            *bytesWritten += crypto::aesGCMTagSize;
            out += crypto::aesGCMTagSize;
            _bytesReservedForTag = crypto::aesGCMTagSize;
        }
        // Generate a new IV, and populate the buffer with it
        if (outLen < *bytesWritten + aesGetIVSize(_mode)) {
            return Status(
                ErrorCodes::InvalidLength,
                "EncryptedDataProtector attempted to write IV in insufficiently sized buffer");
        }
        aesGenerateIV(_key, _mode, out, outLen - *bytesWritten);
        *bytesWritten += aesGetIVSize(_mode);

        if (1 != EVP_EncryptInit_ex(
                     _encryptCtx.get(), swCipher.getValue(), nullptr, _key->getKey(), out)) {
            return Status(ErrorCodes::UnknownError,
                          str::stream()
                              << "Failed to init OpenSSL encrypt context: "
                              << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
        }
        out += aesGetIVSize(_mode);
    }

    // Populate the buffer with ciphertext
    int updateBytes = outLen - *bytesWritten;
    if (1 != EVP_EncryptUpdate(_encryptCtx.get(), out, &updateBytes, in, inLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Failed to update OpenSSL encrypt context: "
                                    << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }
    *bytesWritten += updateBytes;
    invariant(outLen >= *bytesWritten);

    return Status::OK();
}

Status EncryptedDataProtector::finalize(std::uint8_t* out,
                                        std::size_t outLen,
                                        std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (_encryptCtx) {
        int tmpLen = 0;
        if (1 != EVP_EncryptFinal_ex(_encryptCtx.get(), out, &tmpLen)) {
            return Status(ErrorCodes::UnknownError,
                          str::stream()
                              << "Failed to finalize OpenSSL encrypt context"
                              << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
        }
        *bytesWritten = tmpLen;
    }
    invariant(outLen >= *bytesWritten);
    return Status::OK();
}

std::size_t EncryptedDataProtector::getNumberOfBytesReservedForTag() const {
    return _bytesReservedForTag;
}

Status EncryptedDataProtector::finalizeTag(std::uint8_t* out,
                                           std::size_t outLen,
                                           std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (_encryptCtx) {
#ifdef EVP_CTRL_GCM_GET_TAG
        if (_mode == crypto::aesMode::gcm) {
            if (1 != EVP_CIPHER_CTX_ctrl(
                         _encryptCtx.get(), EVP_CTRL_GCM_GET_TAG, crypto::aesGCMTagSize, out)) {
                return Status(ErrorCodes::UnknownError,
                              str::stream()
                                  << "Generating GCM tag failed for OpenSSL encrypt context"
                                  << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
            }
            *bytesWritten = crypto::aesGCMTagSize;
        }
#endif
    }
    invariant(outLen >= *bytesWritten);
    return Status::OK();
}

}  // namespace mongo
