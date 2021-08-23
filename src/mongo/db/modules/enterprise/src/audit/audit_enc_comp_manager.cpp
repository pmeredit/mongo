/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_enc_comp_manager.h"
#include "audit_file_header.h"

#include "mongo/base/error_extra_info.h"
#include "mongo/base/string_data.h"
#include "mongo/logv2/log.h"
#include "mongo/util/base64.h"
#include "mongo/util/options_parser/environment.h"
#include <string>

namespace mongo {
namespace audit {

constexpr auto AUDIT_ENCRYPTION_VERSION = "0.0"_sd;

AuditEncryptionCompressionManager::AuditEncryptionCompressionManager(
    StringData compressionMode, std::string keyStoreIdentifier, std::string encryptionKeyIdentifier)
    : _kmipKeyStoreIdentifier(std::move(keyStoreIdentifier)),
      _kmipEncryptionKeyIdentifier(std::move(encryptionKeyIdentifier)) {
    // TODO -> Validate encryption key, keystore and set kmip conecction type based key
    _fileHeader = std::make_unique<AuditFileHeader>();

    _zstdCompressor = std::make_unique<ZstdMessageCompressor>();
}

BSONObj AuditEncryptionCompressionManager::encodeFileHeader() const {
    BSONObj toWriteFileHeader = _fileHeader->generateFileHeader(AUDIT_ENCRYPTION_VERSION,
                                                                _zstdCompressor->getName(),
                                                                _kmipKeyStoreIdentifier,
                                                                _kmipEncryptionKeyIdentifier);

    return toWriteFileHeader;
}

std::string AuditEncryptionCompressionManager::compress(ConstDataRange toCompress) const {
    std::size_t outBuffSize = _zstdCompressor->getMaxCompressedSize(toCompress.length());

    std::string toEncode;
    toEncode.resize(outBuffSize);

    auto outBuf = DataRange(toEncode.data(), toEncode.size());

    auto sws = _zstdCompressor->compressData(toCompress, outBuf);
    uassertStatusOK(sws.getStatus().withContext("Failed to compress audit line"));

    std::string toWriteCompressedEncoded = base64::encode(toEncode.data(), sws.getValue());

    return toWriteCompressedEncoded;
}

std::string AuditEncryptionCompressionManager::decompress(const std::string& line) {
    std::string binaryCompressed = base64::decode(line);

    size_t outBuffSize =
        _zstdCompressor->getMaxDecompressedSize(binaryCompressed.data(), binaryCompressed.length());

    // A compressed audit event is generated from BSON. The limit on an
    // extended BSON size is 48MB. Because of this, we can guarantee
    // that the compressed object is no larger than
    // BSONObj::LargeSizeTrait::MaxSize (48MB).
    uassert(ErrorCodes::Overflow,
            str::stream() << "Log line is larger than the allowed size of 48MB",
            outBuffSize <= BSONObj::LargeSizeTrait::MaxSize);

    std::string toWriteDecompressed;
    toWriteDecompressed.resize(outBuffSize);

    auto inBuf = ConstDataRange(binaryCompressed.data(), binaryCompressed.size());
    auto outBuf = DataRange(toWriteDecompressed.data(), toWriteDecompressed.size());

    auto sws = _zstdCompressor->decompressData(inBuf, outBuf);
    uassertStatusOK(sws.getStatus().withContext("Failed to decompress audit line"));

    toWriteDecompressed.resize(sws.getValue());

    return toWriteDecompressed;
}

std::string AuditEncryptionCompressionManager::compressAndEncrypt(
    ConstDataRange toCompressAndEncrypt) const {
    return compress(toCompressAndEncrypt) + "\n";
}


}  // namespace audit
}  // namespace mongo
