/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <set>
#include <string>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"

#include "symmetric_key.h"

namespace mongo {
namespace crypto {

/**
 * Encryption algorithm identifiers and block sizes
 */
constexpr uint8_t aesAlgorithm = 0x1;

/**
 * Block and key sizes
 */
constexpr size_t aesBlockSize = 16;
constexpr size_t sym256KeySize = 32;

/**
 * Min and max symmetric key lengths
 */
constexpr size_t minKeySize = 16;
constexpr size_t maxKeySize = 32;

/**
 * CBC fixed constants
 */
constexpr size_t aesCBCIVSize = aesBlockSize;

/**
 * GCM tunable parameters
 */
constexpr size_t aesGCMTagSize = 12;
constexpr size_t aesGCMIVSize = 12;

/**
 * Encryption mode identifiers
 */
enum class aesMode : uint8_t { cbc, gcm };

/**
 * Algorithm names which this module recognizes
 */
const std::string aes256CBCName = "AES256-CBC";
const std::string aes256GCMName = "AES256-GCM";

aesMode getCipherModeFromString(const std::string& mode);
std::string getStringFromCipherMode(aesMode);

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

constexpr size_t kMaxHeaderSize = std::max<size_t>({
    HeaderCBCV0::kTagSize + HeaderCBCV0::kExtraSize + HeaderCBCV0::kIVSize,
    HeaderGCMV0::kTagSize + HeaderGCMV0::kExtraSize + HeaderGCMV0::kIVSize,
    HeaderGCMV1::kTagSize + HeaderGCMV1::kExtraSize + HeaderGCMV1::kIVSize,
});

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
    size_t expectedCiphertextLen(size_t plaintextLen) const {
        if
            constexpr(Header::kMode == aesMode::cbc) {
                return crypto::aesBlockSize * (1 + plaintextLen / crypto::aesBlockSize);
            }
        else {
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
    constexpr std::size_t getTagSize() const {
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
    constexpr std::size_t getExtraSize() const {
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
    constexpr std::size_t getIVSize() const {
        return Header::kIVSize;
    }

    /**
     * Get the size of all information prefixed to the ciphertext
     */
    constexpr std::size_t getHeaderSize() const {
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

/**
 * Generates a new, random, symmetric key for use with AES.
 */
SymmetricKey aesGenerate(size_t keySize, SymmetricKeyId keyId);

/* Platform specific engines should implement these. */

/**
 * Interface to a symmetric cryptography engine.
 * For use with encrypting payloads.
 */
class SymmetricEncryptor {
public:
    virtual ~SymmetricEncryptor() = default;

    /**
     * Process a chunk of data from <in> and store the ciphertext in <out>.
     * Returns the number of bytes written to <out> which will not exceed <outLen>.
     * Because <inLen> for this and/or previous calls may not lie on a block boundary,
     * the number of bytes written to <out> may be more or less than <inLen>.
     */
    virtual StatusWith<size_t> update(const uint8_t* in,
                                      size_t inLen,
                                      uint8_t* out,
                                      size_t outLen) = 0;

    /**
     * Append Additional AuthenticatedData (AAD) to a GCM encryption stream.
     */
    virtual Status addAuthenticatedData(const uint8_t* in, size_t inLen) = 0;

    /**
     * Finish an encryption by flushing any buffered bytes for a partial cipherblock to <out>.
     * Returns the number of bytes written, not to exceed <outLen>.
     */
    virtual StatusWith<size_t> finalize(uint8_t* out, size_t outLen) = 0;

    /**
     * For aesMode::gcm, writes the GCM tag to <out>.
     * Returns the number of bytes used, not to exceed <outLen>.
     */
    virtual StatusWith<size_t> finalizeTag(uint8_t* out, size_t outLen) = 0;

    /**
     * Create an instance of a SymmetricEncryptor object from the currently available
     * cipher engine (e.g. OpenSSL).
     */
    static StatusWith<std::unique_ptr<SymmetricEncryptor>> create(const SymmetricKey& key,
                                                                  aesMode mode,
                                                                  const uint8_t* iv,
                                                                  size_t inLen);
};

/**
 * Interface to a symmetric cryptography engine.
 * For use with encrypting payloads.
 */
class SymmetricDecryptor {
public:
    virtual ~SymmetricDecryptor() = default;

    /**
     * Process a chunk of data from <in> and store the decrypted text in <out>.
     * Returns the number of bytes written to <out> which will not exceed <outLen>.
     * Because <inLen> for this and/or previous calls may not lie on a block boundary,
     * the number of bytes written to <out> may be more or less than <inLen>.
     */
    virtual StatusWith<size_t> update(const uint8_t* in,
                                      size_t inLen,
                                      uint8_t* out,
                                      size_t outLen) = 0;

    /**
     * For aesMode::gcm, inform the cipher engine of additional authenticated data (AAD).
     */
    virtual Status addAuthenticatedData(const uint8_t* in, size_t inLen) = 0;

    /**
     * For aesMode::gcm, informs the cipher engine of the GCM tag associated with this data stream.
     */
    virtual Status updateTag(const uint8_t* tag, size_t tagLen) = 0;

    /**
     * Finish an decryption by flushing any buffered bytes for a partial cipherblock to <out>.
     * Returns the number of bytes written, not to exceed <outLen>.
     */
    virtual StatusWith<size_t> finalize(uint8_t* out, size_t outLen) = 0;

    /**
     * Create an instance of a SymmetricDecryptor object from the currently available
     * cipher engine (e.g. OpenSSL).
     */
    static StatusWith<std::unique_ptr<SymmetricDecryptor>> create(const SymmetricKey& key,
                                                                  aesMode mode,
                                                                  const uint8_t* iv,
                                                                  size_t ivLen);
};

/**
 * Returns a list of cipher modes supported by the cipher engine.
 * e.g. {"AES256-CBC", "AES256-GCM"}
 */
std::set<std::string> getSupportedSymmetricAlgorithms();

/**
 * Generate a quantity of random bytes from the cipher engine.
 */
Status engineRandBytes(uint8_t* buffer, size_t len);

}  // namespace crypto
}  // namespace mongo
