/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>

#include "audit_file_header.h"
#include "audit_frame.h"
#include "audit_key_manager.h"

#include "encryptdb/symmetric_crypto.h"
#include "mongo/base/data_range.h"
#include "mongo/transport/message_compressor_zstd.h"

namespace mongo {
namespace audit {

/**
 * Contains functions for the AuditManager to call to perform
 * encryption and compression. Contains also functions for
 * generating and encrypting keys as well as for generating
 * an audit file header.
 */
class AuditEncryptionCompressionManager {
public:
    AuditEncryptionCompressionManager(std::unique_ptr<AuditKeyManager> keyManager,
                                      bool compress,
                                      const AuditKeyManager::WrappedKey* wrappedKey = nullptr);

    BSONObj compressAndEncrypt(const PlainAuditFrame& frame) const;
    BSONObj decryptAndDecompress(const EncryptedAuditFrame& frame) const;

    std::vector<std::uint8_t> compress(ConstDataRange toCompress) const;
    std::vector<std::uint8_t> decompress(ConstDataRange toDecompress) const;

    std::vector<std::uint8_t> encrypt(ConstDataRange toEncrypt, const Date_t& ts) const;
    std::vector<std::uint8_t> decrypt(ConstDataRange toDecrypt, const Date_t& ts) const;

    BSONObj encodeFileHeader() const;

private:
    std::size_t _encrypt(const Date_t& ts, ConstDataRange input, DataRange output) const;

    std::unique_ptr<ZstdMessageCompressor> _zstdCompressor;

    // File header containing startup options
    std::unique_ptr<AuditFileHeader> _fileHeader;

    // Key manager used if encryption is enabled
    std::unique_ptr<AuditKeyManager> _keyManager;

    // Key used for encrypting audit log lines
    std::unique_ptr<SymmetricKey> _encryptKey;

    // _encryptKey encrypted with the key encryption key
    AuditKeyManager::WrappedKey _wrappedKey;

    // Whether compression is performed prior to encrypt
    bool _compress;

    // Preallocated buffer for storing output of compress & encrypt
    mutable std::vector<uint8_t> _preallocate;
};

}  // namespace audit
}  // namespace mongo
