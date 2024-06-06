/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_enc_comp_manager.h"

#include "audit_key_manager_mock.h"
#include "mongo/base/init.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/base64.h"

namespace mongo {
namespace audit {

const std::string toCompressLog(
    R"({"atype" : "clientMetadata", "ts" : { "$date" : "2021-07-06T15:11:49.455+00:00" },)"
    R"( "uuid" : { "$binary" : "EskVUb80Tt6q9u0lM6aymw==", "$type" : "04" }, "local" : {)"
    R"( "ip" : "127.0.0.1", "port" : 27017 }, "remote" : { "ip" : "127.0.0.1", "port" : )"
    R"(53678 }, "users" : [], "roles" : [], "param" : { "localEndpoint" : { "ip" : )"
    R"("127.0.0.1", "port" : 27017 }, "clientMetadata" : { "application" : { "name" : )"
    R"("MongoDB Shell" }, "driver" : { "name" : "MongoDB Internal Client", "version" : )"
    R"("5.0.0-alpha0-1203-gaf64b76" }, "os" : { "type" : "Linux", "name" : "Ubuntu", )"
    R"("architecture" : "x86_64", "version" : "18.04" } } }, "result" : 0 })");

static ConstDataRange plaintextInput(toCompressLog.data(), toCompressLog.size());
static ConstDataRange nullCdr(nullptr, 0);
static ConstDataRange emptyCdr(toCompressLog.data(), 0);

static constexpr auto kCiphertextHeaderSize = 24;  // 12 byte tag + 12 byte IV

class AuditEncCompManagerTest : public mongo::unittest::Test {
public:
    AuditEncCompManagerTest() {
        // create the local key manager and the encrypt/compress manager uut
        auto keyManager = std::make_unique<AuditKeyManagerMock>();
        _encManager =
            std::make_unique<AuditEncryptionCompressionManager>(std::move(keyManager), true);

        const auto mode = crypto::aesMode::gcm;
        _gcmSupported =
            crypto::getSupportedSymmetricAlgorithms().count(getStringFromCipherMode(mode));
    }

    static constexpr StringData properties[] = {"ts"_sd,
                                                "version"_sd,
                                                "compressionMode"_sd,
                                                "keyStoreIdentifier"_sd,
                                                "encryptedKey"_sd,
                                                "MAC"_sd,
                                                "auditRecordType"_sd};

protected:
    std::unique_ptr<AuditEncryptionCompressionManager> _encManager;

    bool _gcmSupported;
};

TEST_F(AuditEncCompManagerTest, EncryptAndEncodeLargePayloadSucceeds) {
    if (!_gcmSupported) {
        return;
    }

    std::unique_ptr<char[]> data = std::make_unique<char[]>(BSONObj::DefaultSizeTrait::MaxSize);
    DataRange dr(data.get(), BSONObj::DefaultSizeTrait::MaxSize);
    BSONObj obj = _encManager->encryptAndEncode(dr, Date_t::now());
    ASSERT_GREATER_THAN(obj.objsize(), BSONObj::DefaultSizeTrait::MaxSize);
}

TEST_F(AuditEncCompManagerTest, CompressedSizeTest) {
    std::size_t sizeBeforeCompression = toCompressLog.size();
    auto compressedLog = _encManager->compress({toCompressLog.data(), toCompressLog.size()});
    std::size_t sizeAfterCompression = compressedLog.size();
    ASSERT_LESS_THAN_OR_EQUALS(sizeAfterCompression, sizeBeforeCompression);
}

TEST_F(AuditEncCompManagerTest, CompressedStringDifferentTest) {
    auto compressedLog = _encManager->compress({toCompressLog.data(), toCompressLog.size()});
    std::string compressedStr(reinterpret_cast<char*>(compressedLog.data()), compressedLog.size());
    ASSERT_NOT_EQUALS(toCompressLog, compressedStr);
}

TEST_F(AuditEncCompManagerTest, CompressDecompressRoundTrip) {
    auto compressedLog = _encManager->compress({toCompressLog.data(), toCompressLog.size()});
    auto decompressedLog = _encManager->decompress({compressedLog.data(), compressedLog.size()});
    std::string decompressedStr(reinterpret_cast<char*>(decompressedLog.data()),
                                decompressedLog.size());
    ASSERT_EQUALS(toCompressLog, decompressedStr);
}

TEST_F(AuditEncCompManagerTest, EncryptDecryptRoundTrip) {
    if (!_gcmSupported) {
        return;
    }

    auto aad = Date_t::now();
    auto ciphertext = _encManager->encrypt(plaintextInput, aad);
    auto plaintextOutput = _encManager->decrypt(ciphertext, aad);
    ASSERT_EQUALS(plaintextInput.length(), plaintextOutput.size());
    ASSERT_EQUALS(0,
                  memcmp(plaintextOutput.data(), plaintextInput.data(), plaintextInput.length()));
}

TEST_F(AuditEncCompManagerTest, EncryptDecryptRoundTripFail) {
    if (!_gcmSupported) {
        return;
    }

    auto aad = Date_t::now();
    auto ciphertext = _encManager->encrypt(plaintextInput, aad);

    // test decrypt with wrong aad
    ASSERT_THROWS(_encManager->decrypt(ciphertext, Date_t::max()), DBException);

    // test decrypt with bad ciphertext header
    ciphertext.front() ^= 1;
    ASSERT_THROWS(_encManager->decrypt(ciphertext, aad), DBException);
}

TEST_F(AuditEncCompManagerTest, EncryptDecryptRoundTripEmptyPlaintext) {
    if (!_gcmSupported) {
        return;
    }

    auto aad = Date_t::now();

    auto ciphertext = _encManager->encrypt(nullCdr, aad);
    ASSERT_EQUALS(ciphertext.size(), kCiphertextHeaderSize);

    auto plaintextOutput = _encManager->decrypt(ciphertext, aad);
    ASSERT_TRUE(plaintextOutput.empty());

    ciphertext = _encManager->encrypt(emptyCdr, aad);
    ASSERT_EQUALS(ciphertext.size(), kCiphertextHeaderSize);

    plaintextOutput = _encManager->decrypt(ciphertext, aad);
    ASSERT_TRUE(plaintextOutput.empty());
}

TEST_F(AuditEncCompManagerTest, EncryptDecryptRoundTripFailEmptyPlaintext) {
    if (!_gcmSupported) {
        return;
    }
    std::string aad = "Hello World";
    auto ciphertext = _encManager->encrypt(emptyCdr, aad);
    ASSERT_EQUALS(ciphertext.size(), kCiphertextHeaderSize);
    aad[0]++;
    ASSERT_THROWS(_encManager->decrypt(ciphertext, aad), DBException);
}

TEST_F(AuditEncCompManagerTest, EncryptDecryptRoundTripEmptyAAD) {
    if (!_gcmSupported) {
        return;
    }

    auto ciphertext = _encManager->encrypt(plaintextInput, emptyCdr);
    auto plaintextOutput = _encManager->decrypt(ciphertext, emptyCdr);
    ASSERT_EQUALS(plaintextInput.length(), plaintextOutput.size());
    ASSERT_EQUALS(0,
                  memcmp(plaintextOutput.data(), plaintextInput.data(), plaintextInput.length()));

    ciphertext = _encManager->encrypt(plaintextInput, nullCdr);
    plaintextOutput = _encManager->decrypt(ciphertext, nullCdr);
    ASSERT_EQUALS(plaintextInput.length(), plaintextOutput.size());
    ASSERT_EQUALS(0,
                  memcmp(plaintextOutput.data(), plaintextInput.data(), plaintextInput.length()));
}

TEST_F(AuditEncCompManagerTest, DecryptEmptyCiphertext) {
    if (!_gcmSupported) {
        return;
    }

    ASSERT_THROWS(_encManager->decrypt(nullCdr, Date_t::now()), DBException);
    ASSERT_THROWS(_encManager->decrypt(emptyCdr, Date_t::now()), DBException);
}

TEST_F(AuditEncCompManagerTest, EncodeFileHeaderTest) {
    if (!_gcmSupported) {
        return;
    }

    BSONObj fileHeader = _encManager->encodeFileHeader();

    for (auto& prop : properties) {
        ASSERT_TRUE(fileHeader.hasField(prop));
    }

    StringData macStr = fileHeader.getStringField("MAC"_sd);

    // the MAC has size 24 (12 tag + 12 IV) before it's encoded with base64.
    ASSERT_EQUALS(base64::encodedLength(24), macStr.size());
    ASSERT_TRUE(base64::validate(macStr));
}

TEST_F(AuditEncCompManagerTest, VerifyHeaderMACPassTest) {
    if (!_gcmSupported) {
        return;
    }

    BSONObj fileHeader = _encManager->encodeFileHeader();

    IDLParserContext ctx("audit file header");
    auto headerObj = AuditHeaderOptionsDocument::parse(ctx, fileHeader);

    // test unmodified header passes
    Status result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_TRUE(result.isOK());

    // test pass if fields other than ts, version, and MAC are changed
    std::vector<uint8_t> testBuf(48);
    headerObj.setCompressionMode("foo");
    headerObj.setKeyStoreIdentifier(fileHeader);
    headerObj.setEncryptedKey({testBuf.data(), testBuf.size()});
    headerObj.setAuditRecordType("bar");
    result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_TRUE(result.isOK());
}

TEST_F(AuditEncCompManagerTest, VerifyHeaderMACFailTest) {
    if (!_gcmSupported) {
        return;
    }

    BSONObj fileHeader = _encManager->encodeFileHeader();

    IDLParserContext ctx("audit file header");
    auto headerObj = AuditHeaderOptionsDocument::parse(ctx, fileHeader);
    auto validHeaderObj = headerObj;

    // test unmodified header passes
    Status result = _encManager->verifyHeaderMAC(validHeaderObj);
    ASSERT_TRUE(result.isOK());

    // test failure if version is changed
    headerObj.setVersion("4.2");
    result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_FALSE(result.isOK());
    headerObj.setVersion(validHeaderObj.getVersion());

    // test failure if timestamp is changed
    headerObj.setTs(Date_t::max());
    result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_FALSE(result.isOK());
    headerObj.setTs(validHeaderObj.getTs());

    // test failure if base64-encoded tag is changed
    std::string badMac = headerObj.getMAC().toString();
    badMac.front() ^= 1;
    headerObj.setMAC(badMac);
    result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_FALSE(result.isOK());
    headerObj.setMAC(validHeaderObj.getMAC());

    // test failure if base64-encoded IV is changed
    badMac = headerObj.getMAC().toString();
    badMac.back() ^= 1;
    headerObj.setMAC(badMac);
    result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_FALSE(result.isOK());
    headerObj.setMAC(validHeaderObj.getMAC());

    // test restored header passes
    result = _encManager->verifyHeaderMAC(headerObj);
    ASSERT_TRUE(result.isOK());
}

TEST_F(AuditEncCompManagerTest, CheckEncryptedLogIsBase64Test) {
    if (!_gcmSupported) {
        return;
    }

    bool isBase64 = base64::validate(toCompressLog);
    ASSERT_FALSE(isBase64);

    ConstDataRange toEncrypt(toCompressLog.data(), toCompressLog.size());

    auto compressedLog = _encManager->encryptAndEncode(toEncrypt, Date_t::now());
    StringData logField(compressedLog.getStringField("log"_sd));
    isBase64 = base64::validate(logField);
    ASSERT_TRUE(isBase64);
}

TEST_F(AuditEncCompManagerTest, EncryptAndEncodeThrowsWhenGCMUnsupported) {
    if (_gcmSupported) {
        return;
    }

    ConstDataRange toEncrypt(toCompressLog.data(), toCompressLog.size());

    ASSERT_THROWS(_encManager->encryptAndEncode(toEncrypt, Date_t::now()),
                  ExceptionFor<ErrorCodes::UnsupportedFormat>);
}

}  // namespace audit
}  // namespace mongo
