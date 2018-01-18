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
    : _key(key), _mode(mode) {}

Status EncryptedDataProtector::protect(const std::uint8_t* in,
                                       std::size_t inLen,
                                       std::uint8_t* out,
                                       std::size_t outLen,
                                       std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (!_encryptor) {
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
        }
        // Generate a new IV, and populate the buffer with it
        auto* iv = out;
        const auto ivLen = aesGetIVSize(_mode);
        if (outLen < *bytesWritten + ivLen) {
            return Status(
                ErrorCodes::InvalidLength,
                "EncryptedDataProtector attempted to write IV in insufficiently sized buffer");
        }
        aesGenerateIV(_key, _mode, out, outLen - *bytesWritten);
        out += ivLen;
        *bytesWritten += ivLen;

        auto swEncryptor = crypto::SymmetricEncryptor::create(*_key, _mode, iv, ivLen);
        if (!swEncryptor.isOK()) {
            return swEncryptor.getStatus();
        }
        _encryptor = std::move(swEncryptor.getValue());
    }

    // Populate the buffer with ciphertext
    int updateBytes = outLen - *bytesWritten;
    auto swUpdate = _encryptor->update(in, inLen, out, updateBytes);
    if (!swUpdate.isOK()) {
        return swUpdate.getStatus();
    }
    *bytesWritten += swUpdate.getValue();
    invariant(outLen >= *bytesWritten);

    return Status::OK();
}

Status EncryptedDataProtector::finalize(std::uint8_t* out,
                                        std::size_t outLen,
                                        std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (!_encryptor) {
        return {ErrorCodes::UnknownError, "Encryptor not initialized"};
    }

    auto swFinal = _encryptor->finalize(out, outLen);
    if (!swFinal.isOK()) {
        return swFinal.getStatus();
    }
    *bytesWritten = swFinal.getValue();
    invariant(outLen >= *bytesWritten);
    return Status::OK();
}

std::size_t EncryptedDataProtector::getNumberOfBytesReservedForTag() const {
    if (_mode == crypto::aesMode::gcm) {
        return crypto::aesGCMTagSize;
    } else {
        return 0;
    }
}

Status EncryptedDataProtector::finalizeTag(std::uint8_t* out,
                                           std::size_t outLen,
                                           std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (_mode != crypto::aesMode::gcm) {
        // Nothing to do.
        return Status::OK();
    }
    if (!_encryptor) {
        return {ErrorCodes::UnknownError, "Encryptor not initialized"};
    }

    auto swTag = _encryptor->finalizeTag(out, outLen);
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }
    *bytesWritten = swTag.getValue();
    return Status::OK();
}

}  // namespace mongo
