/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "audit_enc_comp_manager.h"
#include "audit_file_header.h"
#include "audit_key_manager_local.h"

#include "mongo/base/error_extra_info.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bson_validate.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/logv2/log.h"
#include "mongo/util/base64.h"
#include "mongo/util/options_parser/environment.h"
#include <string>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


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
    return builder.template obj<BSONObj::LargeSizeTrait>();
}

std::size_t AuditEncryptionCompressionManager::_encrypt(ConstDataRange aad,
                                                        ConstDataRange input,
                                                        DataRange output) const {
    EncryptedAuditLayout ctLayout(const_cast<std::uint8_t*>(output.data<std::uint8_t>()),
                                  output.length());

    // increment and get the IV
    AuditSequenceID seqID(_encryptKey->getInitializationCount(),
                          _encryptKey->getAndIncrementInvocationCount());
    auto iv = DataRange(ctLayout.getIV(), ctLayout.getIVSize());
    seqID.serialize(iv);

    // set up the output buffers
    auto ciphertext = DataRange(ctLayout.getData(), ctLayout.getDataSize());
    auto tag = DataRange(ctLayout.getTag(), ctLayout.getTagSize());

    // perform the encrypt
    size_t ctLen = aesEncryptGCM(*_encryptKey, iv, aad, input, ciphertext, tag);

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
    auto iv = ConstDataRange(ctLayout.getIV(), ctLayout.getIVSize());
    if (_seqIDChecker) {
        uassert(ErrorCodes::BadValue,
                "IV in audit log line does not match the expected IV",
                _seqIDChecker->matchAndIncrement(iv));
    }

    // set up the input buffers
    auto tag = ConstDataRange(ctLayout.getTag(), ctLayout.getTagSize());
    auto ciphertext = ConstDataRange(ctLayout.getData(), ctLayout.getDataSize());

    // set up the output buffer
    std::vector<std::uint8_t> outBuf(ctLayout.expectedPlaintextLen().second);
    auto plaintext = DataRange(outBuf.data(), outBuf.size());

    // perform the decrypt
    auto ptLen = aesDecryptGCM(*_encryptKey, iv, aad, ciphertext, tag, plaintext);

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

std::size_t AuditEncryptionCompressionManager::aesEncryptGCM(const SymmetricKey& key,
                                                             ConstDataRange iv,
                                                             ConstDataRange aad,
                                                             ConstDataRange plaintext,
                                                             DataRange ciphertext,
                                                             DataRange tag) {
    uassert(ErrorCodes::InvalidLength,
            "GCM encrypt initialization vector has invalid length",
            iv.length() == crypto::aesGCMIVSize);
    uassert(ErrorCodes::InvalidLength,
            "GCM encrypt tag output buffer has invalid length",
            tag.length() >= crypto::aesGCMTagSize);
    uassert(ErrorCodes::InvalidLength,
            "GCM encrypt ciphertext output buffer is too small",
            ciphertext.length() >= plaintext.length());

    // create encryptor & set AAD
    auto encryptor =
        uassertStatusOK(crypto::SymmetricEncryptor::create(key, crypto::aesMode::gcm, iv));

    if (aad.length()) {
        uassertStatusOK(encryptor->addAuthenticatedData(aad));
    }

    // do the encrypt & finalize
    DataRangeCursor ctCursor(ciphertext);
    size_t ctLen = 0;
    if (plaintext.length()) {
        ctLen = uassertStatusOK(encryptor->update(plaintext, ctCursor));
        ctCursor.advance(ctLen);
    }

    ctLen += uassertStatusOK(encryptor->finalize(ctCursor));

    // set the tag
    uassertStatusOK(encryptor->finalizeTag(tag));
    invariant(ctLen == plaintext.length());
    return ctLen;
}

std::size_t AuditEncryptionCompressionManager::aesDecryptGCM(const SymmetricKey& key,
                                                             ConstDataRange iv,
                                                             ConstDataRange aad,
                                                             ConstDataRange ciphertext,
                                                             ConstDataRange tag,
                                                             DataRange plaintext) {
    uassert(ErrorCodes::InvalidLength,
            "GCM decrypt initialization vector has invalid length",
            iv.length() == crypto::aesGCMIVSize);
    uassert(ErrorCodes::InvalidLength,
            "GCM decrypt tag has invalid length",
            tag.length() >= crypto::aesGCMTagSize);
    uassert(ErrorCodes::InvalidLength,
            "GCM decrypt plaintext output buffer is too small",
            plaintext.length() >= ciphertext.length());

    // create the decryptor & set AAD
    auto decryptor =
        uassertStatusOK(crypto::SymmetricDecryptor::create(key, crypto::aesMode::gcm, iv));

    if (aad.length()) {
        uassertStatusOK(decryptor->addAuthenticatedData(aad));
    }

    // do the decrypt
    DataRangeCursor ptCursor(plaintext);
    auto ptLen = uassertStatusOK(decryptor->update(ciphertext, ptCursor));
    ptCursor.advance(ptLen);

    uassertStatusOK(decryptor->updateTag(tag));

    ptLen += uassertStatusOK(decryptor->finalize(ptCursor));
    invariant(ptLen <= plaintext.length());
    return ptLen;
}


}  // namespace audit
}  // namespace mongo
