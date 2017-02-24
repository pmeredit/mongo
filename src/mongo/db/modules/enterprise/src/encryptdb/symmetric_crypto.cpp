/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "symmetric_crypto.h"

#include <openssl/rand.h>

#include "mongo/base/data_cursor.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_manager.h"
#include "symmetric_key.h"

namespace mongo {
namespace crypto {

namespace {
std::unique_ptr<SecureRandom> random;
}  // namespace

StatusWith<const EVP_CIPHER*> acquireAESCipher(size_t keySize, crypto::aesMode mode) {
    const EVP_CIPHER* cipher = nullptr;
    if (keySize == sym256KeySize) {
        if (mode == crypto::aesMode::cbc) {
            cipher = EVP_get_cipherbyname("aes-256-cbc");
        } else if (mode == crypto::aesMode::gcm) {
            cipher = EVP_get_cipherbyname("aes-256-gcm");
        }
    }

    if (cipher == nullptr) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Unrecognized AES key size/cipher mode. Size: " << keySize
                                    << " Mode: "
                                    << (int)mode);
    }

    return cipher;
}

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

template <typename T>
EncryptedMemoryLayout<T>::EncryptedMemoryLayout(aesMode mode, T basePtr, size_t baseSize)
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
}

template <typename T>
bool EncryptedMemoryLayout<T>::canFitPlaintext(size_t plaintextLen) const {
    return _baseSize >= _headerSize + expectedCiphertextLen(plaintextLen);
}

template <typename T>
size_t EncryptedMemoryLayout<T>::expectedCiphertextLen(size_t plaintextLen) const {
    if (_aesMode == aesMode::cbc) {
        return crypto::aesBlockSize * (1 + plaintextLen / crypto::aesBlockSize);
    } else if (_aesMode == aesMode::gcm) {
        return plaintextLen;
    }
    MONGO_UNREACHABLE;
}

template <typename T>
std::pair<size_t, size_t> EncryptedMemoryLayout<T>::expectedPlaintextLen() const {
    if (_aesMode == aesMode::cbc) {
        return {getDataSize() - crypto::aesBlockSize, getDataSize()};
    } else if (_aesMode == aesMode::gcm) {
        return {getDataSize(), getDataSize()};
    }
    MONGO_UNREACHABLE;
}

template class EncryptedMemoryLayout<const uint8_t*>;
template class EncryptedMemoryLayout<uint8_t*>;

size_t aesGetTagSize(crypto::aesMode mode) {
    if (mode == crypto::aesMode::gcm) {
        return crypto::aesGCMTagSize;
    }
    return 0;
}

size_t aesGetIVSize(crypto::aesMode mode) {
    switch (mode) {
        case crypto::aesMode::cbc:
            return crypto::aesCBCIVSize;
        case crypto::aesMode::gcm:
            return crypto::aesGCMIVSize;
        default:
            fassertFailed(4053);
    }
}

void aesGenerateIV(const SymmetricKey* key,
                   crypto::aesMode mode,
                   uint8_t* buffer,
                   size_t bufferLen) {
    uint32_t initializationCount = key->getInitializationCount();
    invariant(bufferLen >= aesGetIVSize(mode));
    if (mode == crypto::aesMode::gcm && initializationCount != 0) {
        // Generate deterministic 12 byte IV for GCM but not for the master key (identified by
        // having initializationCount = 0). The rational being that we can't keep a usage count
        // for the master key and it will never be used more than 2^32 times so using a random
        // IV is safe.
        DataCursor dc(reinterpret_cast<char*>(buffer));
        dc.writeAndAdvance<uint32_t>(initializationCount);
        dc.write<uint64_t>(key->getAndIncrementInvocationCount());
    } else {
        if (RAND_bytes(reinterpret_cast<unsigned char*>(buffer), aesGetIVSize(mode)) != 1) {
            error() << "Unable to acquire random bytes from OpenSSL: "
                    << SSLManagerInterface::getSSLErrorMessage(ERR_get_error());
            fassertFailed(4050);
        }
    }
}

Status aesEncrypt(const SymmetricKey& key,
                  aesMode mode,
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen,
                  bool ivProvided) {
    if (!(in && out)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption buffers");
    }
    if (!(mode == aesMode::cbc || mode == aesMode::gcm)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption mode");
    }

    crypto::MutableEncryptedMemoryLayout layout(mode, out, outLen);

    if (!layout.canFitPlaintext(inLen)) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Insufficient memory allocated for encryption.");
    }

    if (!ivProvided) {
        aesGenerateIV(&key, mode, layout.getIV(), layout.getIVSize());
    }

    StatusWith<const EVP_CIPHER*> swCipher = acquireAESCipher(key.getKeySize(), mode);
    if (!swCipher.isOK()) {
        return swCipher.getStatus();
    }

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> encryptCtx(EVP_CIPHER_CTX_new(),
                                                                               EVP_CIPHER_CTX_free);

    if (!encryptCtx.get()) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }

    if (1 != EVP_EncryptInit_ex(
                 encryptCtx.get(), swCipher.getValue(), nullptr, key.getKey(), layout.getIV())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }

    int len = 0;
    if (1 != EVP_EncryptUpdate(encryptCtx.get(), layout.getData(), &len, in, inLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }
    invariant(size_t(layout.getHeaderSize() + len) <= outLen);

    int extraLen = 0;
    if (1 != EVP_EncryptFinal_ex(encryptCtx.get(), layout.getData() + len, &extraLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }

// validateEncryptionOption asserts that platforms without GCM will never start in GCM mode
#ifdef EVP_CTRL_GCM_GET_TAG
    if (mode == aesMode::gcm &&
        1 != EVP_CIPHER_CTX_ctrl(
                 encryptCtx.get(), EVP_CTRL_GCM_GET_TAG, layout.getTagSize(), layout.getTag())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }
#endif

    // Some cipher modes, such as GCM, will know in advance exactly how large their ciphertexts will
    // be.
    // Others, like CBC, will have an upper bound. When this is true, we must allocate enough memory
    // to store
    // the worst case. We must then set the actual size of the ciphertext so that the buffer it has
    // been written to may be serialized.
    *resultLen = layout.getHeaderSize() + len + extraLen;
    invariant(*resultLen <= outLen);

    // Check the returned length, including block size padding
    if (size_t(len + extraLen) != layout.expectedCiphertextLen(inLen)) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Encrypt error, expected cipher text of length "
                                    << layout.expectedCiphertextLen(inLen)
                                    << " but found "
                                    << len + extraLen);
    }

    return Status::OK();
}

Status aesDecrypt(const SymmetricKey& key,
                  aesMode mode,
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen) {
    if (!(in && out)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption buffers");
    }
    if (!(mode == aesMode::cbc || mode == aesMode::gcm)) {
        return Status(ErrorCodes::BadValue, "Invalid encryption mode");
    }

    crypto::ConstEncryptedMemoryLayout layout(mode, in, inLen);

    // Check the plaintext buffer can fit the product of decryption
    size_t lowerBound, upperBound;
    std::tie(lowerBound, upperBound) = layout.expectedPlaintextLen();
    if (upperBound > outLen) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Cleartext buffer of size " << outLen
                                    << " too small for output which can be as large as "
                                    << upperBound
                                    << "]");
    }

    StatusWith<const EVP_CIPHER*> swCipher = acquireAESCipher(key.getKeySize(), mode);
    if (!swCipher.isOK()) {
        return swCipher.getStatus();
    }

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> decryptCtx(EVP_CIPHER_CTX_new(),
                                                                               EVP_CIPHER_CTX_free);

    if (!decryptCtx.get()) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }

    if (1 != EVP_DecryptInit_ex(
                 decryptCtx.get(), swCipher.getValue(), nullptr, key.getKey(), layout.getIV())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }

    int len = 0;
    if (1 !=
        EVP_DecryptUpdate(decryptCtx.get(), out, &len, layout.getData(), layout.getDataSize())) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }
    invariant(size_t(len) <= outLen);

// validateEncryptionOption asserts that platforms without GCM will never start in GCM mode
#ifdef EVP_CTRL_GCM_SET_TAG
    if (mode == aesMode::gcm &&
        !EVP_CIPHER_CTX_ctrl(decryptCtx.get(),
                             EVP_CTRL_GCM_SET_TAG,
                             layout.getTagSize(),
                             const_cast<uint8_t*>(layout.getTag()))) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to set GCM tag: "
                                    << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }
#endif

    int extraLen = 0;
    if (1 != EVP_DecryptFinal_ex(decryptCtx.get(), out + len, &extraLen)) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to finalize decryption: "
                                    << SSLManagerInterface::getSSLErrorMessage(ERR_get_error()));
    }

    *resultLen = len + extraLen;
    invariant(*resultLen <= outLen);

    // Check the returned length, excluding headers block padding
    if (*resultLen < lowerBound || *resultLen > upperBound) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Decrypt error, expected clear text length in interval"
                                    << "["
                                    << lowerBound
                                    << ","
                                    << upperBound
                                    << "]"
                                    << "but found "
                                    << *resultLen);
    }

    return Status::OK();
}

SymmetricKey aesGenerate(size_t keySize, std::string keyId) {
    invariant(keySize == sym256KeySize);

    SecureVector<uint8_t> key(keySize);

    size_t offset = 0;
    while (offset < keySize) {
        std::uint64_t randomValue = random->nextInt64();
        memcpy(key->data() + offset, &randomValue, sizeof(randomValue));
        offset += sizeof(randomValue);
    }

    return SymmetricKey(std::move(key), aesAlgorithm, std::move(keyId));
}

}  // namespace crypto
}  // namespace mongo
