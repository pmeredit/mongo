/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include <cstddef>
#include <fstream>

#include "audit_enc_comp_manager.h"

#include "audit/audit_feature_flag_gen.h"
#include "audit_key_manager_mock.h"
#include "mongo/base/init.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace audit {

const std::string toCompressLog(
    "{ \"atype\" : \"clientMetadata\", \"ts\" : { \"$date\" : \"2021-07-06T15:11:49.455+00:00\" }, "
    "\"uuid\" : { \"$binary\" : \"EskVUb80Tt6q9u0lM6aymw==\", \"$type\" : \"04\" }, \"local\" : { "
    "\"ip\" : \"127.0.0.1\", \"port\" : 27017 }, \"remote\" : { \"ip\" : \"127.0.0.1\", \"port\" : "
    "53678 }, \"users\" : [], \"roles\" : [], \"param\" : { \"localEndpoint\" : { \"ip\" : "
    "\"127.0.0.1\", \"port\" : 27017 }, \"clientMetadata\" : { \"application\" : { \"name\" : "
    "\"MongoDB Shell\" }, \"driver\" : { \"name\" : \"MongoDB Internal Client\", \"version\" : "
    "\"5.0.0-alpha0-1203-gaf64b76\" }, \"os\" : { \"type\" : \"Linux\", \"name\" : \"Ubuntu\", "
    "\"architecture\" : \"x86_64\", \"version\" : \"18.04\" } } }, \"result\" : 0 }");

class AuditLogCompressTest : public mongo::unittest::Test {
public:
    AuditLogCompressTest() {
        // create the local key manager and the encrypt/compress manager uut
        auto keyManager = std::make_unique<AuditKeyManagerMock>();
        ac = std::make_unique<AuditEncryptionCompressionManager>(std::move(keyManager), true);
    }

protected:
    std::unique_ptr<AuditEncryptionCompressionManager> ac;
};

TEST_F(AuditLogCompressTest, CompressedSizeTest) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        std::size_t sizeBeforeCompression = toCompressLog.size();
        auto compressedLog = ac->compress({toCompressLog.data(), toCompressLog.size()});
        std::size_t sizeAfterCompression = compressedLog.size();
        ASSERT_LESS_THAN_OR_EQUALS(sizeAfterCompression, sizeBeforeCompression);
    }
}

TEST_F(AuditLogCompressTest, CompressedStringDifferentTest) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        auto compressedLog = ac->compress({toCompressLog.data(), toCompressLog.size()});
        std::string compressedStr(reinterpret_cast<char*>(compressedLog.data()),
                                  compressedLog.size());
        ASSERT_NOT_EQUALS(toCompressLog, compressedStr);
    }
}

TEST_F(AuditLogCompressTest, CompressDecompressRoundTrip) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        auto compressedLog = ac->compress({toCompressLog.data(), toCompressLog.size()});
        auto decompressedLog = ac->decompress({compressedLog.data(), compressedLog.size()});
        std::string decompressedStr(reinterpret_cast<char*>(decompressedLog.data()),
                                    decompressedLog.size());
        ASSERT_EQUALS(toCompressLog, decompressedStr);
    }
}

}  // namespace audit
}  // namespace mongo
