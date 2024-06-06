/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "mongo/platform/basic.h"

#include "encrypted_data_protector.h"

#include <cstring>
#include <memory>

#include "mongo/base/status.h"
#include "mongo/crypto/symmetric_key.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/str.h"

#include "symmetric_crypto.h"

namespace mongo {

EncryptedDataProtector::EncryptedDataProtector(const SymmetricKey* key, crypto::aesMode mode)
    : _key(key), _mode(mode) {}

/**
 * The first call to this method writes a version byte (currently 0),
 * followed by a PageSchema::k0 header. For GCM, we won't know the tag yet,
 * so a placeholder value of all 0xFF bytes is used.
 *
 * From there, every invocation (including the first), appends chunks of encrypted data.
 * Eventually, ::finalize() is invoked to flush any queued data (for CBC),
 * then ::finalizeTag() is invoked to update the tag (for GCM) and the file is closed.
 *
 * The end result is a file which may be decrypted (from offset 1) using aesDecrypt
 * with PageSchema::k0.
 *
 * We don't invoke aesEncrypt() directly since we want a streaming
 * cipher which can be continually appended to rather than a single-shot encrypt.
 */
Status EncryptedDataProtector::protect(const std::uint8_t* in,
                                       std::size_t inLen,
                                       std::uint8_t* out,
                                       std::size_t outLen,
                                       std::size_t* bytesWritten) {
    *bytesWritten = 0;
    if (!_encryptor) {
        // Allocate space for a version and (for GCM) a tag
        const auto tagLen = getNumberOfBytesReservedForTag();
        if (outLen < *bytesWritten + tagLen) {
            return Status(ErrorCodes::InvalidLength,
                          "EncryptedDataProtector attempted to reserve tag in insufficiently "
                          "sized buffer");
        }
        memset(out, 0xFF, tagLen);
        *bytesWritten += tagLen;
        out += tagLen;

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

        auto swEncryptor = crypto::SymmetricEncryptor::create(*_key, _mode, {iv, ivLen});
        if (!swEncryptor.isOK()) {
            return swEncryptor.getStatus();
        }
        _encryptor = std::move(swEncryptor.getValue());
    }

    // Populate the buffer with ciphertext
    auto updateBytes = static_cast<std::size_t>(outLen - *bytesWritten);
    auto swUpdate = _encryptor->update({in, inLen}, {out, updateBytes});
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

    auto swFinal = _encryptor->finalize({out, outLen});
    if (!swFinal.isOK()) {
        return swFinal.getStatus();
    }
    *bytesWritten = swFinal.getValue();
    invariant(outLen >= *bytesWritten);
    return Status::OK();
}

std::size_t EncryptedDataProtector::getNumberOfBytesReservedForTag() const {
    if (_mode == crypto::aesMode::gcm) {
        return sizeof(DATA_PROTECTOR_VERSION_0) + crypto::aesGCMTagSize;
    } else {
        return sizeof(DATA_PROTECTOR_VERSION_0);
    }
}

Status EncryptedDataProtector::finalizeTag(std::uint8_t* out,
                                           std::size_t outLen,
                                           std::size_t* bytesWritten) {
    static_assert(sizeof(DATA_PROTECTOR_VERSION_0) == 1, "Unexpected data protector version size");
    if (outLen < sizeof(DATA_PROTECTOR_VERSION_0)) {
        return {ErrorCodes::BadValue, "Insufficient space to write data protector version"};
    }

    out[0] = DATA_PROTECTOR_VERSION_0;
    out += sizeof(DATA_PROTECTOR_VERSION_0);
    outLen -= sizeof(DATA_PROTECTOR_VERSION_0);
    *bytesWritten = sizeof(DATA_PROTECTOR_VERSION_0);

    if (_mode != crypto::aesMode::gcm) {
        // Nothing to do.
        return Status::OK();
    }

    if (!_encryptor) {
        return {ErrorCodes::UnknownError, "Encryptor not initialized"};
    }

    auto swTag = _encryptor->finalizeTag({out, outLen});
    if (!swTag.isOK()) {
        return swTag.getStatus();
    }

    *bytesWritten += swTag.getValue();
    return Status::OK();
}

}  // namespace mongo
