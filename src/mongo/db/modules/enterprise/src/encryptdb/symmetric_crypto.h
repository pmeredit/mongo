/**
 *    Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <set>
#include <string>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/crypto/symmetric_key.h"


namespace mongo {
namespace crypto {

size_t aesGetIVSize(crypto::aesMode mode);
size_t aesGetTagSize(crypto::aesMode mode);
std::pair<std::size_t, std::size_t> expectedPlaintextLen(aesMode, const std::uint8_t*, std::size_t);
void aesGenerateIV(const SymmetricKey* key,
                   crypto::aesMode mode,
                   uint8_t* buffer,
                   size_t bufferLen);

enum class PageSchema : std::uint8_t {
    k0 = 0,  // Single key per database
    k1 = 1,  // Per-page key selection
};

/**
 * Determine page schema used by this GCM encrypted page
 * and extract the KeyId if it is a V1 layout.
 */
std::pair<PageSchema, SymmetricKeyId::id_type> parseGCMPageSchema(const std::uint8_t* ptr,
                                                                  std::size_t len);

struct HeaderCBCV0 {
    // CBC mode has a simple 128bit IV.
    static constexpr auto kMode = aesMode::cbc;
    static constexpr auto kSchema = PageSchema::k0;
    static constexpr std::size_t kTagSize = 0;    // Authentication Tag: GCM only
    static constexpr std::size_t kExtraSize = 0;  // Schema version specific data (>= V1 only)
    static constexpr std::size_t kIVSize = 16;    // IV length.
};

struct HeaderGCMV0 {
    // GCM uses a 96bit tag and 96bit IV.
    static constexpr auto kMode = aesMode::gcm;
    static constexpr auto kSchema = PageSchema::k0;
    static constexpr std::size_t kTagSize = 12;
    static constexpr std::size_t kExtraSize = 0;
    static constexpr std::size_t kIVSize = 12;
};

struct HeaderGCMV1 {
    // The first four bytes after the tag in a GCM page
    // is either FFFFFFFF demarking a V1 page,
    // or a smaller number demarking V0.
    static constexpr auto kMode = aesMode::gcm;
    static constexpr auto kSchema = PageSchema::k1;
    static constexpr std::size_t kTagSize = 12;
    // Extra data in GCMv1 is laid out as the 0xFFFFFFFF marker,
    // followed by a schema version '\x01', then a uint64_t KeyId.
    static constexpr std::size_t kExtraSize = 13;
    static constexpr std::size_t kIVSize = 12;
};
#ifdef _GLIBCXX_DEBUG
constexpr size_t kMaxHeaderSize = 37;
static_assert(HeaderGCMV1::kTagSize + HeaderGCMV1::kExtraSize + HeaderGCMV1::kIVSize == 37);
static_assert(HeaderGCMV0::kTagSize + HeaderGCMV0::kExtraSize + HeaderGCMV0::kIVSize == 24);
static_assert(HeaderCBCV0::kTagSize + HeaderCBCV0::kExtraSize + HeaderCBCV0::kIVSize == 16);
#else
constexpr size_t kMaxHeaderSize = std::max<size_t>({
    HeaderCBCV0::kTagSize + HeaderCBCV0::kExtraSize + HeaderCBCV0::kIVSize,
    HeaderGCMV0::kTagSize + HeaderGCMV0::kExtraSize + HeaderGCMV0::kIVSize,
    HeaderGCMV1::kTagSize + HeaderGCMV1::kExtraSize + HeaderGCMV1::kIVSize,
});
#endif

template <typename T, class Header>
class EncryptedMemoryLayout {
public:
    using header_type = Header;

    static_assert(
        std::is_pointer<T>::value &&
            std::is_same<
                uint8_t,
                typename std::remove_const<typename std::remove_pointer<T>::type>::type>::value,
        "EncryptedMemoryLayout must be instantiated with a uint8_t*");

    EncryptedMemoryLayout(T basePtr, size_t baseSize) : _basePtr(basePtr), _baseSize(baseSize) {
        invariant(_basePtr);
    }

    /**
     * Ensure there is enough memory for:
     * A MAC authentication header + An IV + The data which fits into whole AES blocks +
     * A padded AES block containing remaining data.
     *
     * Return true if and only if this is true.
     */
    bool canFitPlaintext(size_t plaintextLen) const {
        return _baseSize >= getHeaderSize() + expectedCiphertextLen(plaintextLen);
    }

    /**
     * Return the expected size of the ciphertext given the length of this plaintext. This will
     * vary depending on the ciphermode.
     */
    static size_t expectedCiphertextLen(size_t plaintextLen) {
        if constexpr (Header::kMode == aesMode::cbc) {
            return crypto::aesBlockSize * (1 + plaintextLen / crypto::aesBlockSize);
        } else {
            invariant(Header::kMode == aesMode::gcm);
            return plaintextLen;
        }
    }

    /**
     * Returns the bounds on the smallest and largest possible plaintext for the ciphertext.
     */
    std::pair<size_t, size_t> expectedPlaintextLen() const {
        if (Header::kMode == aesMode::cbc) {
            return {getDataSize() - crypto::aesBlockSize, getDataSize()};
        } else {
            invariant(Header::kMode == aesMode::gcm);
            return {getDataSize(), getDataSize()};
        }
    }

    /**
     * Get a pointer to the tag
     */
    T getTag() {
        return _basePtr;
    }

    /**
     * Get the size of the tag
     */
    static constexpr std::size_t getTagSize() {
        return Header::kTagSize;
    }

    /**
     * Get a pointer to the extra data
     */
    T getExtra() {
        return _basePtr + Header::kTagSize;
    }

    /**
     * Get the size of the extra data
     */
    static constexpr std::size_t getExtraSize() {
        return Header::kExtraSize;
    }

    /**
     * Get a pointer to the IV
     */
    T getIV() {
        return _basePtr + Header::kTagSize + Header::kExtraSize;
    }

    /**
     * Get the size of the IV
     */
    static constexpr std::size_t getIVSize() {
        return Header::kIVSize;
    }

    /**
     * Get the size of all information prefixed to the ciphertext
     */
    static constexpr std::size_t getHeaderSize() {
        return Header::kTagSize + Header::kExtraSize + Header::kIVSize;
    }

    /**
     * Get a pointer to the ciphertext data
     */
    T getData() {
        return _basePtr + getHeaderSize();
    }

    /**
     * Get the size of the ciphertext payload.
     * The largest possible amount of ciphertext that can be stored in the memory region.
     */
    size_t getDataSize() const {
        return _baseSize - getHeaderSize();
    }

private:
    T _basePtr;             // Pointer to the memory buffer
    std::size_t _baseSize;  // Size of the buffer
};

template <class Header>
using ConstEncryptedMemoryLayout = EncryptedMemoryLayout<const uint8_t*, Header>;

template <class Header>
using MutableEncryptedMemoryLayout = EncryptedMemoryLayout<uint8_t*, Header>;

/**
 * Encrypts the plaintext 'in' using AES with the SymmetricKey in 'key'
 * using encryption mode 'mode'. CBC and GCM are the only supported modes at this time.
 *
 * The resulting ciphertext (and tag) are stored in the 'out' buffer.
 */
Status aesEncrypt(const SymmetricKey& key,
                  aesMode mode,
                  PageSchema schema,
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen,
                  bool ivProvided = false);

/**
 * Decrypts the plaintext stored in the 'in' buffer using AES with the SymmetricKey stored in 'key'
 * using encryption mode 'mode'. CBC and GCM are the only supported modes at this time.
 *
 * 'outLen' is an in-out parameter representing the size of the buffer 'out', and the
 * resulting length of the decrypted buffer.
 */
Status aesDecrypt(const SymmetricKey& key,
                  aesMode mode,
                  PageSchema schema,
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen);


}  // namespace crypto
}  // namespace mongo
