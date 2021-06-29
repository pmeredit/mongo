/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit_enc_comp_manager.h"

#include "mongo/base/string_data.h"
#include "mongo/util/base64.h"

namespace mongo {
namespace audit {

AuditEncryptionCompressionManager::AuditEncryptionCompressionManager() {
    auto& registry = MessageCompressorRegistry::get();

    // Hardcoded the compressing library, in the future the library will be given
    // in the server options
    _compressor = registry.getCompressor("zstd"_sd);
}

std::string AuditEncryptionCompressionManager::compress(ConstDataRange toCompress) const {
    std::size_t outBuffSize = _compressor->getMaxCompressedSize(toCompress.length());

    std::string toEncode;
    toEncode.resize(outBuffSize);

    auto outBuf = DataRange(toEncode.data(), toEncode.size());

    auto outSize = _compressor->compressData(toCompress, outBuf);

    std::string toWriteCompressedEncoded = base64::encode(toEncode.data(), outSize.getValue());

    return toWriteCompressedEncoded;
}

std::string AuditEncryptionCompressionManager::compressAndEncrypt(
    ConstDataRange toCompressAndEncrypt) const {
    return compress(toCompressAndEncrypt) + "\n";
}


}  // namespace audit
}  // namespace mongo
