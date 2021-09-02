/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include <cstddef>
#include <fstream>

#include "audit_enc_comp_manager.h"

#include "audit/audit_feature_flag_gen.h"
#include "mongo/base/init.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace audit {

// toDecompressLog is the same log as toCompressLog after
// compression and being encoded to base64.
const std::string toCompressLog(
    "{ \"atype\" : \"clientMetadata\", \"ts\" : { \"$date\" : \"2021-07-06T15:11:49.455+00:00\" }, "
    "\"uuid\" : { \"$binary\" : \"EskVUb80Tt6q9u0lM6aymw==\", \"$type\" : \"04\" }, \"local\" : { "
    "\"ip\" : \"127.0.0.1\", \"port\" : 27017 }, \"remote\" : { \"ip\" : \"127.0.0.1\", \"port\" : "
    "53678 }, \"users\" : [], \"roles\" : [], \"param\" : { \"localEndpoint\" : { \"ip\" : "
    "\"127.0.0.1\", \"port\" : 27017 }, \"clientMetadata\" : { \"application\" : { \"name\" : "
    "\"MongoDB Shell\" }, \"driver\" : { \"name\" : \"MongoDB Internal Client\", \"version\" : "
    "\"5.0.0-alpha0-1203-gaf64b76\" }, \"os\" : { \"type\" : \"Linux\", \"name\" : \"Ubuntu\", "
    "\"architecture\" : \"x86_64\", \"version\" : \"18.04\" } } }, \"result\" : 0 }");

const std::string toDecompressLog(
    "KLUv/WByAeUKABYSPCUwi+gBhdwR4y1SrMBEsXYyk3jkXHSyXuu1XCv+zBcobAMHAwsIMAAyADIAiQqz7qgb4Hk"
    "cZuFErUVPe7heXi+t6eadQjC+rGecYP5O+UPHTxjlxDhiBivlnfEVtIL2tX7NW1ob00wzr2msRsaX9IlKpxEMhk"
    "NFceeQfRcN7h8NSZADBqvBKNZnlKh4L+NwIsDSp4U/CSycREtJCSUGHDBAI5M6Kp+Txvcpylr9Z6SVmo2rcusVS"
    "pqAYKF1oecUTxVujpi9vFaFGag5TsFBD/b+BD9KJuv355UgD3fGAuqe5LgFjyyKbtrSLqnSl4dZNiogMELAnLMD"
    "nCRWVjtwUYEfb4W6qIBygiZwifLIDXZYDQF7SPLAQLBaxAJFQ/iLbYwyu+8QV+mAeQPKEYaMM9aLgSNkzRySf6m"
    "tBVSG6J7OGanBEG96azWCgQiKxTZwGOA3CEHODgf8CQ==");

class AuditLogCompressTest : public mongo::unittest::Test {
public:
    AuditLogCompressTest() {}

protected:
    AuditEncryptionCompressionManager ac =
        AuditEncryptionCompressionManager("zstd", "testKey", "testKeyIdentifier");
};

TEST_F(AuditLogCompressTest, CompressedSizeTest) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        std::size_t sizeBeforeCompression = toCompressLog.size();
        std::string compressedLog = ac.compress(toCompressLog);
        std::size_t sizeAfterCompression = compressedLog.size();

        ASSERT_LESS_THAN_OR_EQUALS(sizeAfterCompression, sizeBeforeCompression);
    }
}

TEST_F(AuditLogCompressTest, CompressedStringDifferentTest) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        std::string compressedLog = ac.compress(toCompressLog);

        ASSERT_NOT_EQUALS(toCompressLog, compressedLog);
    }
}

TEST_F(AuditLogCompressTest, ToCompressLogSameAsToDecompressLogAfterCompress) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        std::string compressedLog = ac.compress(toCompressLog);

        ASSERT_EQUALS(compressedLog, toDecompressLog);
    }
}

TEST_F(AuditLogCompressTest, ToDecompressLogSameAsToCompressLogAfterDecompress) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        auto decompressedLog = ac.decompress(toDecompressLog);

        ASSERT_EQUALS(toCompressLog, decompressedLog.toString());
    }
}

}  // namespace audit
}  // namespace mongo
