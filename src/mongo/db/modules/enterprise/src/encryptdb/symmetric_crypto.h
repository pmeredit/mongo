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
const uint8_t aesAlgorithm = 0x1;

/**
 * Block and key sizes
 */
const size_t aesBlockSize = 16;
const size_t sym256KeySize = 32;

/**
 * Min and max symmetric key lengths
 */
const size_t minKeySize = 16;
const size_t maxKeySize = 32;

/**
 * CBC fixed constants
 */
const size_t aesCBCIVSize = aesBlockSize;

/**
 * GCM tunable parameters
 */
const size_t aesGCMTagSize = 12;
const size_t aesGCMIVSize = 12;

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
void aesGenerateIV(const SymmetricKey* key,
                   crypto::aesMode mode,
                   uint8_t* buffer,
                   size_t bufferLen);

/** Describes the in memory layout of encryption related data.
 *
 * This layout is as follows:
 * Tag - 12 bytes on GCM, 0 on CBC
 * IV - 12 bytes on GCM, 16 bytes on CBC
 * Ciphertext - Size of plaintext on GCM,
 *              Size of plaintext padded up to the nearest multiple of the AES blocksize on CBC
 */
template <typename T>
class EncryptedMemoryLayout {
    static_assert(
        std::is_pointer<T>::value &&
            std::is_same<
                uint8_t,
                typename std::remove_const<typename std::remove_pointer<T>::type>::type>::value,
        "EncryptedMemoryLayout must be instantiated with a uint8_t*");

public:
    EncryptedMemoryLayout(aesMode mode, T basePtr, size_t baseSize);

    /**
     * Ensure there is enough memory for:
     * A MAC authentication header + An IV + The data which fits into whole AES blocks +
     * A padded AES block containing remaining data.
     *
     * Return true if and only if this is true.
     */
    bool canFitPlaintext(size_t plaintextLen) const;

    /**
     * Return the expected size of the ciphertext given the length of this plaintext. This will
     * vary depending on the ciphermode.
     */
    size_t expectedCiphertextLen(size_t plaintextLen) const;

    /**
     * Returns the bounds on the smallest and largest possible plaintext for the ciphertext.
     */
    std::pair<size_t, size_t> expectedPlaintextLen() const;

    /**
     * Get a pointer to the tag
     */
    T getTag() const {
        return _basePtr;
    }

    /**
     * Get the size of the tag
     */
    size_t getTagSize() const {
        return _tagSize;
    }

    /**
     * Get a pointer to the IV
     */
    T getIV() const {
        return _basePtr + _tagSize;
    }

    /**
     * Get the size of the IV
     */
    size_t getIVSize() const {
        return _ivSize;
    }

    /**
     * Get the size of all information prefixed to the ciphertext
     */
    size_t getHeaderSize() const {
        return _headerSize;
    }

    /**
     * Get a pointer to the ciphertext data
     */
    T getData() const {
        return _basePtr + _headerSize;
    }

    /**
     * Get the size of the ciphertext payload.
     * The largest possible amount of ciphertext that can be stored in the memory region.
     */
    size_t getDataSize() const {
        return _baseSize - _headerSize;
    }

private:
    size_t _tagSize;     // Size of tag protecting IV and data
    size_t _ivSize;      // Size of the IV
    size_t _headerSize;  // Size of all metadata

    T _basePtr;        // Pointer to the memory buffer
    size_t _baseSize;  // Size of the buffer

    aesMode _aesMode;  // Cipher mode to report sizes on
};
using ConstEncryptedMemoryLayout = EncryptedMemoryLayout<const uint8_t*>;
using MutableEncryptedMemoryLayout = EncryptedMemoryLayout<uint8_t*>;

/**
 * Encrypts the plaintext 'in' using AES with the SymmetricKey in 'key'
 * using encryption mode 'mode'. CBC and GCM are the only supported modes at this time.
 *
 * The resulting ciphertext (and tag) are stored in the 'out' buffer.
 */
Status aesEncrypt(const SymmetricKey& key,
                  aesMode mode,
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
                  const uint8_t* in,
                  size_t inLen,
                  uint8_t* out,
                  size_t outLen,
                  size_t* resultLen);

/**
 * Generates a new, random, symmetric key for use with AES.
 */
SymmetricKey aesGenerate(size_t keySize, std::string keyId);

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
