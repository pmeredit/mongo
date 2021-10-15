/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_enc_comp_manager.h"
#include "audit_file_header.h"
#include "audit_key_manager_local.h"

#include "mongo/base/error_extra_info.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bson_validate.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/logv2/log.h"
#include "mongo/util/base64.h"
#include "mongo/util/options_parser/environment.h"
#include <string>

namespace mongo {
namespace audit {

constexpr auto AUDIT_ENCRYPTION_VERSION = "0.0"_sd;

namespace {

using ConstEncryptedAuditLayout = crypto::ConstEncryptedMemoryLayout<crypto::HeaderGCMV0>;
using EncryptedAuditLayout = crypto::MutableEncryptedMemoryLayout<crypto::HeaderGCMV0>;

std::vector<std::uint8_t> getTimestampAAD(const Date_t& ts) {
    auto aad = ts.toMillisSinceEpoch();
    std::vector<std::uint8_t> output(sizeof(aad));
    DataView(reinterpret_cast<char*>(output.data())).write(tagLittleEndian(aad));
    return output;
}

std::size_t getRequiredEncryptBufferSize(ConstDataRange toEncrypt) {
    return ConstEncryptedAuditLayout::getHeaderSize() +
        ConstEncryptedAuditLayout::expectedCiphertextLen(toEncrypt.length());
}
}  // namespace

AuditEncryptionCompressionManager::AuditEncryptionCompressionManager(
    std::unique_ptr<AuditKeyManager> keyManager,
    bool compress,
    const AuditKeyManager::WrappedKey* wrappedKey)
    : _keyManager(std::move(keyManager)), _compress(compress) {

    _fileHeader = std::make_unique<AuditFileHeader>();

    _zstdCompressor = std::make_unique<ZstdMessageCompressor>();

    if (wrappedKey) {
        _wrappedKey = *wrappedKey;
        auto symKey = _keyManager->unwrapKey(_wrappedKey);
        _encryptKey = std::make_unique<SymmetricKey>(std::move(symKey));
    } else {
        auto genKeys = _keyManager->generateWrappedKey();
        _encryptKey = std::make_unique<SymmetricKey>(std::move(genKeys.key));
        _wrappedKey = std::move(genKeys.wrappedKey);
    }
}

BSONObj AuditEncryptionCompressionManager::encodeFileHeader() const {
    constexpr auto kNoCompress = "none"_sd;
    Date_t ts = Date_t::now();

    // generate the MAC
    auto aad = _fileHeader->generateFileHeaderAuthenticatedData(ts, AUDIT_ENCRYPTION_VERSION);
    auto mac =
        encrypt(ConstDataRange(nullptr, nullptr), ConstDataRange(aad.objdata(), aad.objsize()));
    auto encodedMac = base64::encode(mac.data(), mac.size());

    BSONObj toWriteFileHeader =
        _fileHeader->generateFileHeader(ts,
                                        AUDIT_ENCRYPTION_VERSION,
                                        _compress ? _zstdCompressor->getName() : kNoCompress,
                                        encodedMac,
                                        _keyManager->getKeyStoreID(),
                                        _wrappedKey);
    return toWriteFileHeader;
}

std::vector<std::uint8_t> AuditEncryptionCompressionManager::compress(
    ConstDataRange toCompress) const {
    std::vector<std::uint8_t> outBuf(_zstdCompressor->getMaxCompressedSize(toCompress.length()));
    auto sws = _zstdCompressor->compressData(toCompress, {outBuf.data(), outBuf.size()});
    uassertStatusOK(sws.getStatus().withContext("Failed to compress audit line"));
    invariant(outBuf.size() >= sws.getValue());
    outBuf.resize(sws.getValue());
    return outBuf;
}

std::vector<std::uint8_t> AuditEncryptionCompressionManager::decompress(
    ConstDataRange toDecompress) const {
    size_t outBuffSize =
        _zstdCompressor->getMaxDecompressedSize(toDecompress.data(), toDecompress.length());

    // A compressed audit event is generated from BSON. The limit on an
    // extended BSON size is 48MB. Because of this, we can guarantee
    // that the compressed object is no larger than
    // BSONObj::LargeSizeTrait::MaxSize (48MB).
    uassert(ErrorCodes::Overflow,
            str::stream() << "Log line is larger than the allowed size of 48MB",
            outBuffSize <= BSONObj::LargeSizeTrait::MaxSize);

    std::vector<std::uint8_t> outBuf(outBuffSize);

    auto sws = _zstdCompressor->decompressData(toDecompress, {outBuf.data(), outBuf.size()});
    uassertStatusOK(sws.getStatus().withContext("Failed to decompress audit line"));
    invariant(outBuf.size() >= sws.getValue());
    outBuf.resize(sws.getValue());
    return outBuf;
}

BSONObj AuditEncryptionCompressionManager::decryptAndDecompress(
    const EncryptedAuditFrame& frame) const {
    StringData payload(reinterpret_cast<const char*>(frame.payload.data()), frame.payload.size());
    auto decoded = base64::decode(payload);
    auto decrypted = decrypt({decoded.data(), decoded.size()}, frame.ts);
    std::vector<std::uint8_t> decompressed;
    char* bsonString;
    size_t bsonLen;

    if (_compress) {
        decompressed = decompress({decrypted.data(), decrypted.size()});
        bsonString = reinterpret_cast<char*>(decompressed.data());
        bsonLen = decompressed.size();
    } else {
        bsonString = reinterpret_cast<char*>(decrypted.data());
        bsonLen = decrypted.size();
    }

    uassertStatusOK(validateBSON(bsonString, bsonLen));
    return BSONObj(bsonString).copy();
}

BSONObj AuditEncryptionCompressionManager::encryptAndEncode(ConstDataRange toEncrypt,
                                                            const Date_t& ts) const {
    auto encrypted = encrypt(toEncrypt, ts);

    BSONObjBuilder builder;

    builder.append(PlainAuditFrame::kTimestampField, ts);
    builder.append(PlainAuditFrame::kLogField, base64::encode(encrypted.data(), encrypted.size()));
    return builder.obj();
}

std::size_t AuditEncryptionCompressionManager::_encrypt(ConstDataRange aad,
                                                        ConstDataRange input,
                                                        DataRange output) const {
    EncryptedAuditLayout ctLayout(const_cast<std::uint8_t*>(output.data<std::uint8_t>()),
                                  output.length());

    uassert(ErrorCodes::InvalidLength,
            "Output buffer size is too small",
            ctLayout.canFitPlaintext(input.length()));

    // increment and get the IV
    AuditSequenceID iv(_encryptKey->getInitializationCount(),
                       _encryptKey->getAndIncrementInvocationCount());
    iv.serialize({ctLayout.getIV(), ctLayout.getIVSize()});

    // create encryptor & set AAD
    auto encryptor = uassertStatusOK(crypto::SymmetricEncryptor::create(
        *_encryptKey, crypto::aesMode::gcm, ctLayout.getIV(), ctLayout.getIVSize()));

    if (aad.length()) {
        uassertStatusOK(encryptor->addAuthenticatedData(aad.data<uint8_t>(), aad.length()));
    }

    // do the encrypt & finalize
    size_t ctLen = 0;
    if (input.length()) {
        invariant(input.data());
        ctLen = uassertStatusOK(encryptor->update(input.data<std::uint8_t>(),
                                                  input.length(),
                                                  ctLayout.getData(),
                                                  ctLayout.getDataSize()));
    }

    ctLen += uassertStatusOK(
        encryptor->finalize(ctLayout.getData() + ctLen, ctLayout.getDataSize() - ctLen));

    // make sure the uassert above still holds post-encrypt
    uassert(ErrorCodes::InvalidLength,
            "Output buffer size is too small",
            ctLen <= ctLayout.getDataSize());

    // set the tag
    uassertStatusOK(encryptor->finalizeTag(ctLayout.getTag(), ctLayout.getTagSize()));

    // return the size of header + size of ciphertext
    return ctLayout.getHeaderSize() + ctLen;
}

std::vector<std::uint8_t> AuditEncryptionCompressionManager::encrypt(ConstDataRange toEncrypt,
                                                                     const Date_t& ts) const {
    return encrypt(toEncrypt, getTimestampAAD(ts));
}

std::vector<std::uint8_t> AuditEncryptionCompressionManager::encrypt(ConstDataRange toEncrypt,
                                                                     ConstDataRange aad) const {
    std::vector<std::uint8_t> outBuf(getRequiredEncryptBufferSize(toEncrypt));
    auto encryptSize = _encrypt(aad, toEncrypt, outBuf);
    outBuf.resize(encryptSize);
    return outBuf;
}

std::vector<std::uint8_t> AuditEncryptionCompressionManager::decrypt(ConstDataRange toDecrypt,
                                                                     const Date_t& ts) const {
    return decrypt(toDecrypt, getTimestampAAD(ts));
}

std::vector<std::uint8_t> AuditEncryptionCompressionManager::decrypt(ConstDataRange toDecrypt,
                                                                     ConstDataRange aad) const {
    uassert(ErrorCodes::InvalidLength,
            "Audit log decrypt input cannot be empty",
            toDecrypt.length() > 0);

    ConstEncryptedAuditLayout ctLayout(toDecrypt.data<std::uint8_t>(), toDecrypt.length());

    // the input data range should at least be the header size in length
    uassert(ErrorCodes::InvalidLength,
            "Audit log decrypt input is too short",
            toDecrypt.length() >= ctLayout.getHeaderSize());

    // Check the IV matches the expected IV
    if (_seqIDChecker) {
        uassert(ErrorCodes::BadValue,
                "IV in audit log line does not match the expected IV",
                _seqIDChecker->matchAndIncrement({ctLayout.getIV(), ctLayout.getIVSize()}));
    }

    // set up the output buffer
    std::vector<std::uint8_t> outBuf(ctLayout.expectedPlaintextLen().second);
    if (!outBuf.data()) {
        // if the expected output size is 0 (e.g. GMAC),
        // ensure that the output buffer is not null, or else
        // windows will not authenticate the data correctly.
        outBuf.resize(1);
    }

    // create the decryptor & set AAD
    auto decryptor = uassertStatusOK(crypto::SymmetricDecryptor::create(
        *_encryptKey, crypto::aesMode::gcm, ctLayout.getIV(), ctLayout.getIVSize()));

    if (aad.length()) {
        uassertStatusOK(decryptor->addAuthenticatedData(aad.data<uint8_t>(), aad.length()));
    }

    // do the decrypt
    auto ptLen = uassertStatusOK(decryptor->update(
        ctLayout.getData(), ctLayout.getDataSize(), outBuf.data(), outBuf.size()));

    uassertStatusOK(decryptor->updateTag(ctLayout.getTag(), ctLayout.getTagSize()));

    ptLen += uassertStatusOK(decryptor->finalize(outBuf.data() + ptLen, outBuf.size() - ptLen));
    invariant(ptLen <= outBuf.size());

    outBuf.resize(ptLen);
    return outBuf;
}

Status AuditEncryptionCompressionManager::verifyHeaderMAC(
    const AuditHeaderOptionsDocument& header) const try {
    auto aad =
        _fileHeader->generateFileHeaderAuthenticatedData(header.getTs(), header.getVersion());
    auto macStr = base64::decode(header.getMAC());
    decrypt(ConstDataRange(macStr.data(), macStr.size()),
            ConstDataRange(aad.objdata(), aad.objsize()));

    return Status::OK();
} catch (...) {
    return exceptionToStatus().addContext("Audit log header MAC authentication failed");
}

void AuditEncryptionCompressionManager::setSequenceIDCheckerFromHeader(
    const AuditHeaderOptionsDocument& header) {
    auto mac = base64::decode(header.getMAC());
    EncryptedAuditLayout macLayout(reinterpret_cast<uint8_t*>(mac.data()), mac.size());
    uassert(ErrorCodes::BadValue,
            "Audit log header MAC has invalid length",
            mac.size() == macLayout.getHeaderSize());
    auto parsedIV = AuditSequenceID::deserialize({macLayout.getIV(), macLayout.getIVSize()});
    // increment IV to the next expected IV from the log lines
    ++parsedIV;
    _seqIDChecker = std::make_unique<AuditSequenceIDChecker>(parsedIV);
}

void AuditEncryptionCompressionManager::setSequenceIDChecker(
    std::unique_ptr<AuditSequenceIDChecker> seqIDChecker) {
    _seqIDChecker = std::move(seqIDChecker);
}

}  // namespace audit
}  // namespace mongo
