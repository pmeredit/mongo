/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>

#include "audit/audit_header_options_gen.h"
#include "audit_file_header.h"
#include "audit_frame.h"
#include "audit_key_manager.h"
#include "audit_sequence_id.h"

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

    BSONObj encryptAndEncode(ConstDataRange toEncrypt, const Date_t& ts) const;
    BSONObj decryptAndDecompress(const EncryptedAuditFrame& frame) const;

    std::vector<std::uint8_t> compress(ConstDataRange toCompress) const;
    std::vector<std::uint8_t> decompress(ConstDataRange toDecompress) const;

    std::vector<std::uint8_t> encrypt(ConstDataRange toEncrypt, const Date_t& ts) const;
    std::vector<std::uint8_t> decrypt(ConstDataRange toDecrypt, const Date_t& ts) const;

    std::vector<std::uint8_t> encrypt(ConstDataRange toEncrypt, ConstDataRange aad) const;
    std::vector<std::uint8_t> decrypt(ConstDataRange toDecrypt, ConstDataRange aad) const;

    BSONObj encodeFileHeader() const;
    Status verifyHeaderMAC(const AuditHeaderOptionsDocument& header) const;

    void setSequenceIDChecker(std::unique_ptr<AuditSequenceIDChecker> seqIDChecker);
    void setSequenceIDCheckerFromHeader(const AuditHeaderOptionsDocument& header);

private:
    std::size_t _encrypt(ConstDataRange aad, ConstDataRange input, DataRange output) const;

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

    // Checks the sequence of IVs are correct on decrypt
    mutable std::unique_ptr<AuditSequenceIDChecker> _seqIDChecker;
};

}  // namespace audit
}  // namespace mongo
