/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>

#include "audit_file_header.h"

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
    AuditEncryptionCompressionManager(StringData compressionMode,
                                      std::string keyStoreIdentifier,
                                      std::string encryptionKeyIdentifier);

    std::string compressAndEncrypt(ConstDataRange toCompressAndEncrypt) const;

    std::string compress(ConstDataRange toCompress) const;

    std::string decompress(const std::string& line);

    BSONObj encodeFileHeader() const;

private:
    std::unique_ptr<ZstdMessageCompressor> _zstdCompressor;

    // KMIP key store unique identifier
    std::string _kmipKeyStoreIdentifier;

    // KMIP unique identifier for existing key to use
    std::string _kmipEncryptionKeyIdentifier;

    // File header containing startup options
    std::unique_ptr<AuditFileHeader> _fileHeader;
};

}  // namespace audit
}  // namespace mongo
