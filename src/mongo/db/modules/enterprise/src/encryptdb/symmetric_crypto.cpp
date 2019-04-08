/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "symmetric_crypto.h"

#include "encryption_options.h"
#include "mongo/base/data_cursor.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/str.h"
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

aesMode getCipherModeFromString(const std::string& mode) {
    if (mode == aes256CBCName) {
        return aesMode::cbc;
    } else if (mode == aes256GCMName) {
        return aesMode::gcm;
    } else {
        MONGO_UNREACHABLE;
    }
}

std::string getStringFromCipherMode(aesMode mode) {
    if (mode == aesMode::cbc) {
        return aes256CBCName;
    } else if (mode == aesMode::gcm) {
        return aes256GCMName;
    } else {
        MONGO_UNREACHABLE;
    }
}

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

std::pair<PageSchema, SymmetricKeyId::id_type> parseGCMPageSchema(const std::uint8_t* ptr,
                                                                  std::size_t len) {
    static_assert(HeaderGCMV0::kTagSize == HeaderGCMV1::kTagSize, "Expected common GCM tag size");

    uassert(ErrorCodes::BadValue,
            str::stream() << "Invalid GCM page length: " << len,
            len >= (HeaderGCMV1::kTagSize + HeaderGCMV1::kExtraSize));

    ConstDataRangeCursor cursor(ptr, len);
    cursor.advance(HeaderGCMV1::kTagSize);

    const auto marker = cursor.readAndAdvance<std::uint32_t>();
    if (marker != std::numeric_limits<std::uint32_t>::max()) {
        return {PageSchema::k0, 0};
    }

    const auto version = cursor.readAndAdvance<std::uint8_t>();
    uassert(ErrorCodes::BadValue,
            str::stream() << "Unknown page encryption schema version " << static_cast<int>(version),
            version == 1);

    static_assert(std::is_same<SymmetricKeyId::id_type, std::uint64_t>::value,
                  "GCMV1 page format depends on a uint64 key id");

    const auto id = cursor.readAndAdvance<LittleEndian<SymmetricKeyId::id_type>>();
    return {PageSchema::k1, id};
}

std::pair<std::size_t, std::size_t> expectedPlaintextLen(aesMode mode,
                                                         const std::uint8_t* ptr,
                                                         std::size_t len) {
    switch (mode) {
        case aesMode::cbc:
            return ConstEncryptedMemoryLayout<HeaderCBCV0>(ptr, len).expectedPlaintextLen();
        case aesMode::gcm:
            switch (parseGCMPageSchema(ptr, len).first) {
                case PageSchema::k0:
                    return ConstEncryptedMemoryLayout<HeaderGCMV0>(ptr, len).expectedPlaintextLen();
                case PageSchema::k1:
                    return ConstEncryptedMemoryLayout<HeaderGCMV1>(ptr, len).expectedPlaintextLen();
            }
    }
    MONGO_UNREACHABLE;
}

void aesGenerateIV(const SymmetricKey* key,
                   crypto::aesMode mode,
                   uint8_t* buffer,
                   size_t bufferLen) {
    invariant(!encryptionGlobalParams.readOnlyMode);
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
        const auto ivSize = aesGetIVSize(mode);
        if (bufferLen < ivSize) {
            fassert(40683, "IV buffer is too small for selected mode");
        }
        auto status = engineRandBytes(buffer, ivSize);
        if (!status.isOK()) {
            fassert(4050, status);
        }
    }
}

namespace {

Status _validateModeSchema(aesMode mode, PageSchema schema) {
    if (schema == PageSchema::k0) {
        return Status::OK();
    }

    if ((mode == aesMode::gcm) && (schema == PageSchema::k1)) {
        return Status::OK();
    }

    return {ErrorCodes::BadValue,
            str::stream() << "Invalid schema version " << static_cast<int>(schema)
                          << " for encryption mode '"
                          << getStringFromCipherMode(mode)
                          << "'"};
}

template <typename T>
Status _doAESEncrypt(const SymmetricKey& key,
                     T layout,
                     const std::uint8_t* in,
                     std::size_t inLen,
                     std::size_t* resultLen,
                     bool ivProvided) try {
    if (!layout.canFitPlaintext(inLen)) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Insufficient memory allocated for encryption.");
    }

    constexpr auto mode = T::header_type::kMode;
    if (!ivProvided) {
        aesGenerateIV(&key, mode, layout.getIV(), layout.getIVSize());
    }

    auto encryptor =
        uassertStatusOK(SymmetricEncryptor::create(key, mode, layout.getIV(), layout.getIVSize()));

    if (std::is_same<typename T::header_type, crypto::HeaderGCMV1>::value) {
        static_assert(crypto::HeaderGCMV1::kExtraSize == 13, "Page schema layout has changed");

        const auto id = key.getKeyId().id();
        fassert(51155, id != boost::none);

        static_assert(std::is_same<SymmetricKeyId::id_type, std::uint64_t>::value,
                      "GCMV1 page format depends on a uint64 key id");

        auto extra = DataRangeCursor(layout.getExtra(), layout.getExtraSize());
        extra.writeAndAdvance<std::uint32_t>(0xFFFFFFFFU);
        extra.writeAndAdvance(static_cast<std::uint8_t>(T::header_type::kSchema));
        extra.writeAndAdvance<LittleEndian<SymmetricKeyId::id_type>>(*id);

        // Use the entire "extra" block except for the fixed 0xFFFFFFFF marker for AAD.
        uassertStatusOK(
            encryptor->addAuthenticatedData(layout.getExtra() + 4, layout.getExtraSize() - 4));
    } else {
        invariant(layout.getExtraSize() == 0);
    }

    const auto updateLen =
        uassertStatusOK(encryptor->update(in, inLen, layout.getData(), layout.getDataSize()));
    const auto finalLen = uassertStatusOK(
        encryptor->finalize(layout.getData() + updateLen, layout.getDataSize() - updateLen));
    const auto len = updateLen + finalLen;

    uassertStatusOK(encryptor->finalizeTag(layout.getTag(), layout.getTagSize()));


    // Some cipher modes, such as GCM, will know in advance exactly how large their ciphertexts will
    // be.
    // Others, like CBC, will have an upper bound. When this is true, we must allocate enough memory
    // to store
    // the worst case. We must then set the actual size of the ciphertext so that the buffer it has
    // been written to may be serialized.
    invariant(len <= layout.getDataSize());
    *resultLen = layout.getHeaderSize() + len;

    // Check the returned length, including block size padding
    if (len != layout.expectedCiphertextLen(inLen)) {
        return {ErrorCodes::BadValue,
                str::stream() << "Encrypt error, expected cipher text of length "
                              << layout.expectedCiphertextLen(inLen)
                              << " but found "
                              << len};
    }

    return Status::OK();
} catch (const AssertionException& ex) {
    return ex.toStatus();
}
}  // namespace

Status aesEncrypt(const SymmetricKey& key,
                  aesMode mode,
                  PageSchema schema,
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen,
                  bool ivProvided) {
    if (!(in && out)) {
        return {ErrorCodes::BadValue, "Invalid encryption buffers"};
    }

    auto status = _validateModeSchema(mode, schema);
    if (!status.isOK()) {
        return status;
    }

    if (mode == aesMode::cbc) {
        invariant(schema == PageSchema::k0);
        return _doAESEncrypt(key,
                             crypto::MutableEncryptedMemoryLayout<crypto::HeaderCBCV0>(out, outLen),
                             in,
                             inLen,
                             resultLen,
                             ivProvided);
    } else {
        invariant(mode == aesMode::gcm);
        if (schema == PageSchema::k0) {
            return _doAESEncrypt(
                key,
                crypto::MutableEncryptedMemoryLayout<crypto::HeaderGCMV0>(out, outLen),
                in,
                inLen,
                resultLen,
                ivProvided);
        } else {
            invariant(schema == PageSchema::k1);
            return _doAESEncrypt(
                key,
                crypto::MutableEncryptedMemoryLayout<crypto::HeaderGCMV1>(out, outLen),
                in,
                inLen,
                resultLen,
                ivProvided);
        }
    }
}

namespace {
template <typename T>
Status _doAESDecrypt(const SymmetricKey& key,
                     T layout,
                     std::uint8_t* out,
                     std::size_t outLen,
                     std::size_t* resultLen) try {
    // Check the plaintext buffer can fit the product of decryption
    size_t lowerBound, upperBound;
    std::tie(lowerBound, upperBound) = layout.expectedPlaintextLen();
    if (upperBound > outLen) {
        return {ErrorCodes::BadValue,
                str::stream() << "Cleartext buffer of size " << outLen
                              << " too small for output which can be as large as "
                              << upperBound
                              << "]"};
    }

    constexpr auto mode = T::header_type::kMode;
    auto decryptor =
        uassertStatusOK(SymmetricDecryptor::create(key, mode, layout.getIV(), layout.getIVSize()));

    if (std::is_same<typename T::header_type, crypto::HeaderGCMV1>::value) {
        invariant(layout.getExtraSize() == 13);
        // GCM Page schema only uses the post-marker bytes for AAD.
        uassertStatusOK(
            decryptor->addAuthenticatedData(layout.getExtra() + 4, layout.getExtraSize() - 4));
    } else {
        invariant(layout.getExtraSize() == 0);
    }

    const auto updateLen =
        uassertStatusOK(decryptor->update(layout.getData(), layout.getDataSize(), out, outLen));
    uassertStatusOK(decryptor->updateTag(layout.getTag(), layout.getTagSize()));
    const auto finalLen = uassertStatusOK(decryptor->finalize(out + updateLen, outLen - updateLen));

    *resultLen = updateLen + finalLen;
    invariant(*resultLen <= outLen);

    // Check the returned length, excluding headers block padding
    if (*resultLen < lowerBound || *resultLen > upperBound) {
        return {ErrorCodes::BadValue,
                str::stream() << "Decrypt error, expected clear text length in interval"
                              << "["
                              << lowerBound
                              << ","
                              << upperBound
                              << "]"
                              << "but found "
                              << *resultLen};
    }

    return Status::OK();
} catch (const AssertionException& ex) {
    return ex.toStatus();
}
}  // namespace

Status aesDecrypt(const SymmetricKey& key,
                  aesMode mode,
                  PageSchema schema,
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen) {
    if (!(in && out)) {
        return {ErrorCodes::BadValue, "Invalid encryption buffers"};
    }

    auto status = _validateModeSchema(mode, schema);
    if (!status.isOK()) {
        return status;
    }

    if (mode == aesMode::cbc) {
        invariant(schema == PageSchema::k0);
        return _doAESDecrypt(key,
                             crypto::ConstEncryptedMemoryLayout<HeaderCBCV0>(in, inLen),
                             out,
                             outLen,
                             resultLen);
    } else {
        invariant(mode == aesMode::gcm);

        if (schema == PageSchema::k0) {
            crypto::ConstEncryptedMemoryLayout<HeaderGCMV0> layout(in, inLen);
            return _doAESDecrypt(key, std::move(layout), out, outLen, resultLen);
        } else {
            invariant(schema == PageSchema::k1);
            crypto::ConstEncryptedMemoryLayout<HeaderGCMV1> layout(in, inLen);
            return _doAESDecrypt(key, std::move(layout), out, outLen, resultLen);
        }
    }
}

SymmetricKey aesGenerate(size_t keySize, SymmetricKeyId keyId) {
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
