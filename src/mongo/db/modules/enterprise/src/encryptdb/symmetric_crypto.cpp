/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "symmetric_crypto.h"

#include <openssl/evp.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/crypto/crypto.h"
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

StatusWith<const EVP_CIPHER*> acquireAESCipher(size_t keySize, crypto::aesMode mode) {
    const EVP_CIPHER* cipher = nullptr;
    if (keySize == sym256KeySize) {
        if (mode == crypto::aesMode::cbc) {
            cipher = EVP_get_cipherbyname("aes-256-cbc");
        } else if (mode == crypto::aesMode::gcm) {
            cipher = EVP_get_cipherbyname("aes-256-gcm");
        }
    } else if (keySize == sym128KeySize) {
        if (mode == crypto::aesMode::cbc) {
            cipher = EVP_get_cipherbyname("aes-128-cbc");
        } else if (mode == crypto::aesMode::gcm) {
            cipher = EVP_get_cipherbyname("aes-128-gcm");
        }
    }

    if (cipher == nullptr) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Unrecognized AES key size/cipher mode: " << (int)mode);
    }

    return cipher;
}
}  // namespace

MONGO_INITIALIZER(CreateKeyEntropySource)(InitializerContext* context) {
    random = std::unique_ptr<SecureRandom>(SecureRandom::create());
    return Status::OK();
}

aesMode getCipherModeFromString(const std::string& mode) {
    if (mode == aes256CBCName) {
        return aesMode::cbc;
    } else if (mode == aes256GCMName) {
        return aesMode::gcm;
    } else {
        MONGO_UNREACHABLE;
    }
}

EncryptedMemoryLayout::EncryptedMemoryLayout(aesMode mode, uint8_t* basePtr, size_t baseSize)
    : _basePtr(basePtr), _baseSize(baseSize), _aesMode(mode) {
    invariant(basePtr);
    switch (_aesMode) {
        case aesMode::cbc:
            _tagSize = 0;
            _ivSize = aesBlockSize;
            break;
        case aesMode::gcm:
            _tagSize = 12;
            _ivSize = 12;
            break;
        default:
            fassertFailed(4052);
    }
    _headerSize = _tagSize + _ivSize;
    _dataSize = _baseSize - _headerSize;
}

bool EncryptedMemoryLayout::canFitPlaintext(size_t plaintextLen) const {
    return _baseSize >= _headerSize + expectedCiphertextLen(plaintextLen);
}

size_t EncryptedMemoryLayout::expectedCiphertextLen(size_t plaintextLen) const {
    if (_aesMode == aesMode::cbc) {
        return crypto::aesBlockSize * (1 + plaintextLen / crypto::aesBlockSize);
    } else if (_aesMode == aesMode::gcm) {
        return plaintextLen;
    }
    MONGO_UNREACHABLE;
}

std::pair<size_t, size_t> EncryptedMemoryLayout::expectedPlaintextLen() const {
    if (_aesMode == aesMode::cbc) {
        return {_dataSize - crypto::aesBlockSize, _dataSize};
    } else if (_aesMode == aesMode::gcm) {
        return {_dataSize, _dataSize};
    }
    MONGO_UNREACHABLE;
}


void EncryptedMemoryLayout::setDataSize(size_t dataSize) {
    invariant(dataSize <= _baseSize - _headerSize);
    _dataSize = dataSize;
}

Status aesEncrypt(const uint8_t* in,
                  size_t inLen,
                  EncryptedMemoryLayout* layout,
                  const SymmetricKey& key,
                  aesMode mode) {
    if (!(in && layout)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption buffers");
    }
    if (!(mode == aesMode::cbc || mode == aesMode::gcm)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption mode");
    }
    if (layout->getDataSize() < layout->expectedCiphertextLen(inLen)) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Cipher text buffer too short " << layout->getDataSize()
                                    << " < " << layout->expectedCiphertextLen(inLen));
    }


    StatusWith<const EVP_CIPHER*> swCipher = acquireAESCipher(key.getKeySize(), mode);
    if (!swCipher.isOK()) {
        return swCipher.getStatus();
    }

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> encryptCtx(EVP_CIPHER_CTX_new(),
                                                                               EVP_CIPHER_CTX_free);

    if (!encryptCtx.get()) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    if (1 != EVP_EncryptInit_ex(
                 encryptCtx.get(), swCipher.getValue(), nullptr, key.getKey(), layout->getIV())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int len = 0;
    if (1 != EVP_EncryptUpdate(encryptCtx.get(), layout->getData(), &len, in, inLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int extraLen = 0;
    if (1 != EVP_EncryptFinal_ex(encryptCtx.get(), layout->getData() + len, &extraLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    // Some cipher modes, such as GCM, will know in advance exactly how large their ciphertexts will
    // be.
    // Others, like CBC, will have an upper bound. When this is true, we must allocate enough memory
    // to store
    // the worst case. We must then set the actual size of the ciphertext so that the buffer it has
    // been written to may be serialized.
    layout->setDataSize(len + extraLen);

// validateEncryptionOption asserts that platforms without GCM will never start in GCM mode
#ifdef EVP_CTRL_GCM_GET_TAG
    if (mode == aesMode::gcm &&
        1 != EVP_CIPHER_CTX_ctrl(
                 encryptCtx.get(), EVP_CTRL_GCM_GET_TAG, layout->getTagSize(), layout->getTag())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }
#endif

    return Status::OK();
}

Status aesDecrypt(EncryptedMemoryLayout* layout,
                  const SymmetricKey& key,
                  aesMode mode,
                  uint8_t* out,
                  size_t* outLen) {
    if (!(layout && out && outLen)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption buffers");
    }
    if (!(mode == aesMode::cbc || mode == aesMode::gcm)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption mode");
    }


    StatusWith<const EVP_CIPHER*> swCipher = acquireAESCipher(key.getKeySize(), mode);
    if (!swCipher.isOK()) {
        return swCipher.getStatus();
    }

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> decryptCtx(EVP_CIPHER_CTX_new(),
                                                                               EVP_CIPHER_CTX_free);

    if (!decryptCtx.get()) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    if (1 != EVP_DecryptInit_ex(
                 decryptCtx.get(), swCipher.getValue(), nullptr, key.getKey(), layout->getIV())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

    int len = 0;
    if (1 !=
        EVP_DecryptUpdate(decryptCtx.get(), out, &len, layout->getData(), layout->getDataSize())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }

// validateEncryptionOption asserts that platforms without GCM will never start in GCM mode
#ifdef EVP_CTRL_GCM_SET_TAG
    if (mode == aesMode::gcm &&
        !EVP_CIPHER_CTX_ctrl(
            decryptCtx.get(), EVP_CTRL_GCM_SET_TAG, layout->getTagSize(), layout->getTag())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to set GCM tag: "
                                    << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
    }
#endif

    int extraLen = 0;
    if (1 != EVP_DecryptFinal_ex(decryptCtx.get(), out + len, &extraLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to finalize decryption: "
                                    << getSSLManager()->getSSLErrorMessage(ERR_get_error()));
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
