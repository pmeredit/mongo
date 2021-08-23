/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include <fstream>

#include "audit_file_header.h"

#include "audit/audit_feature_flag_gen.h"
#include "mongo/base/error_extra_info.h"
#include "mongo/base/init.h"
#include "mongo/bson/json.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace audit {

class AuditLogHeaderTest : public mongo::unittest::Test {
public:
    AuditLogHeaderTest() {}

protected:
    AuditFileHeader afh;
};

TEST_F(AuditLogHeaderTest, GenerateHeaderTest) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        std::string version = "1.0";
        std::string compressionMode = "zstd";
        std::string kmipKeyStoreIdentifier = "testKey";
        std::string kmipEncryptionKeyIdentifier = "testKeyIdentifier";

        std::string properties[] = {"ts",
                                    "version",
                                    "compressionMode",
                                    "keyStoreIdentifier",
                                    "encryptedKey",
                                    "auditRecordType"};

        BSONObj fileHeader = afh.generateFileHeader(
            version, compressionMode, kmipKeyStoreIdentifier, kmipEncryptionKeyIdentifier);

        for (std::string prop : properties) {
            ASSERT_TRUE(fileHeader.hasField(prop));
        }
    }
}

}  // namespace audit
}  // namespace mongo
