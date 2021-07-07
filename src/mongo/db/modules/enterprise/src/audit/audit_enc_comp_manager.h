/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>

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
    AuditEncryptionCompressionManager();

    std::string compressAndEncrypt(ConstDataRange toCompressAndEncrypt) const;

    std::string compress(ConstDataRange toCompress) const;

    std::string decompress(const std::string& line);

private:
    // Store a reference to the chosen compressor on startup.
    // This assumes that the compressor's lifetime exceeds
    // the lifetime of all audit events.
    // This happens to be true, but is not guaranteed by code.
    std::unique_ptr<ZstdMessageCompressor> _zstdCompressor;
};

}  // namespace audit
}  // namespace mongo
