/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include <fstream>

#include "audit_enc_comp_manager.h"

#include "audit/audit_feature_flag_gen.h"
#include "audit_key_manager_mock.h"
#include "mongo/base/init.h"
#include "mongo/bson/json.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/base64.h"

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

class AuditLogCompressBase64Test : public mongo::unittest::Test {
public:
    AuditLogCompressBase64Test() {
        auto keyManager = std::make_unique<AuditKeyManagerMock>();
        ac = std::make_unique<AuditEncryptionCompressionManager>(std::move(keyManager), true);
    }

protected:
    std::unique_ptr<AuditEncryptionCompressionManager> ac;
};

TEST_F(AuditLogCompressBase64Test, CheckStringIsBase64Test) {
    if (feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
        bool isBase64 = base64::validate(toCompressLog);
        ASSERT_FALSE(isBase64);

        BSONObj auditBson = fromjson(toCompressLog);

        auto compressedLog = ac->compressAndEncrypt({Date_t::now(), auditBson});

        StringData logField(compressedLog.getStringField("log"_sd));

        isBase64 = base64::validate(logField);
        ASSERT_TRUE(isBase64);
    }
}

}  // namespace audit
}  // namespace mongo
