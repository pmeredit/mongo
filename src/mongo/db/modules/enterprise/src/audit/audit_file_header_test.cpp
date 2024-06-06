/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_file_header.h"

#include "audit/audit_config_gen.h"
#include "audit_enc_comp_manager.h"
#include "audit_key_manager_kmip.h"
#include "audit_key_manager_mock.h"
#include "encryptdb/encryption_options.h"
#include "mongo/base/error_extra_info.h"
#include "mongo/base/init.h"
#include "mongo/bson/json.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/base64.h"

namespace mongo {
namespace audit {

class AuditFileHeaderTest : public mongo::unittest::Test {
public:
    AuditFileHeaderTest() {}

    static constexpr StringData properties[] = {"ts"_sd,
                                                "version"_sd,
                                                "compressionMode"_sd,
                                                "keyStoreIdentifier"_sd,
                                                "encryptedKey"_sd,
                                                "MAC"_sd,
                                                "auditRecordType"_sd};

    static constexpr StringData kmipUid = "12345"_sd;

    static constexpr std::array<StringData, 5> keystoreIdKMIPProperties = {
        "provider"_sd, "uid"_sd, "kmipServerName"_sd, "kmipPort"_sd, "keyWrapMethod"_sd};

    static constexpr StringData kmsUid =
        "{'provider': 'aws', 'key': 'arn:aws:kms:us-east-1:abcd', 'region': 'us-east-1', 'endpoint': 'kms.us-east-1.amazonaws.com'}"_sd;

    static constexpr std::array<StringData, 4> keystoreIdKMSProperties = {
        "provider"_sd, "key"_sd, "region"_sd, "endpoint"_sd};

    static constexpr StringData version = "1.0"_sd;
    static constexpr StringData compressionMode = "zstd"_sd;
    static constexpr StringData hash = "dksTpAFI+wvTNqFAAQAAAAAAAAAAAAAA"_sd;

    template <size_t N>
    void testHeader(const AuditKeyManagerKMIPEncrypt& keyManager,
                    const std::array<StringData, N>& keystoreIdProperties) {
        const std::vector<uint8_t> wrappedKey = {1, 2, 3, 4};

        BSONObj fileHeader = _afh.generateFileHeader(
            Date_t::now(), version, compressionMode, hash, keyManager.getKeyStoreID(), wrappedKey);

        for (auto& prop : properties) {
            ASSERT_TRUE(fileHeader.hasField(prop));
        }

        BSONElement keyStoreIdElement = fileHeader.getField("keyStoreIdentifier");
        ASSERT_TRUE(keyStoreIdElement.isABSONObj());

        BSONObj keystoreId = keyStoreIdElement.Obj();

        for (auto& prop : keystoreIdProperties) {
            ASSERT_TRUE(keystoreId.hasField(prop));
        }

        ASSERT_EQUALS(static_cast<size_t>(keystoreId.nFields()), keystoreIdProperties.size());
    }

protected:
    AuditFileHeader _afh;
};

TEST_F(AuditFileHeaderTest, GenerateFileHeader) {
    AuditKeyManagerMock keyManager;
    auto keys = keyManager.generateWrappedKey();

    BSONObj fileHeader = _afh.generateFileHeader(
        Date_t::now(), version, compressionMode, hash, keyManager.getKeyStoreID(), keys.wrappedKey);

    for (auto& prop : properties) {
        ASSERT_TRUE(fileHeader.hasField(prop));
    }
}

TEST_F(AuditFileHeaderTest, GenerateFileHeaderAuthenticatedData) {
    auto aad = _afh.generateFileHeaderAuthenticatedData(Date_t::now(), "3.5");
    ASSERT_TRUE(aad.hasField("ts"_sd));
    ASSERT_TRUE(aad.hasField("version"_sd));
    ASSERT_EQUALS(2, aad.nFields());
}

TEST_F(AuditFileHeaderTest, generateFileHeaderKMIP) {
    encryptionGlobalParams.kmipParams.kmipServerName = {"localhost"};
    encryptionGlobalParams.kmipParams.kmipPort = 6666;


    AuditKeyManagerKMIPEncrypt keyManager{kmipUid.toString(), KeyStoreIDFormat::kmipKeyIdentifier};
    testHeader(keyManager, keystoreIdKMIPProperties);
}

TEST_F(AuditFileHeaderTest, generateFileHeaderKMS) {
    AuditKeyManagerKMIPEncrypt keyManager{kmsUid.toString(), KeyStoreIDFormat::kmsConfigStruct};
    testHeader(keyManager, keystoreIdKMSProperties);
}

}  // namespace audit
}  // namespace mongo
