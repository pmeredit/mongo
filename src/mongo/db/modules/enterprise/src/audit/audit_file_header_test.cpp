/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit_file_header.h"

#include "audit_enc_comp_manager.h"
#include "audit_key_manager_mock.h"
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

protected:
    AuditFileHeader _afh;
};

TEST_F(AuditFileHeaderTest, GenerateFileHeader) {
    AuditKeyManagerMock keyManager;
    auto keys = keyManager.generateWrappedKey();

    std::string version = "1.0";
    std::string compressionMode = "zstd";
    std::string hash = "dksTpAFI+wvTNqFAAQAAAAAAAAAAAAAA";

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

}  // namespace audit
}  // namespace mongo
