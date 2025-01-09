/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <algorithm>
#include <boost/io/ios_state.hpp>
#include <string>
#include <utility>

#include "fle/lib/mongo_crypt.h"
#include "mongo/base/initializer.h"
#include "mongo/bson/json.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/logv2/log_domain_global.h"
#include "mongo/platform/shared_library.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/scopeguard.h"

#define MONGO_CRYPT_PREFIX "mongo_crypt_v1"

#if defined(_WIN32)
#define LIBEXT ".dll"
#elif defined(__APPLE__)
#define LIBEXT ".dylib"
#else
#define LIBEXT ".so"
#endif

#define MONGO_CRYPT_LIBNAME MONGO_CRYPT_PREFIX LIBEXT

#if !defined(MONGO_CRYPT_UNITTEST_DYNAMIC)
namespace mongo {
BSONObj analyzeQuery(BSONObj document, OperationContext* opCtx, NamespaceString ns);
}
#endif

namespace {

using mongo::ScopeGuard;

static const std::string kSchema =
    R"({
        "type": "object",
        "properties": {
            "ssn": {
                "encrypt": {
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    "keyId": [
                        {
                            "$uuid": "1362d0ed-6182-478e-bb8a-ebcc53b91aa1"
                        }
                    ],
                    "bsonType": "int"
                }
            },
            "user": {
                "type": "object",
                "properties": {
                    "account": {
                        "encrypt": {
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            "keyId": [
                                {
                                    "$uuid": "93282c77-9a6b-47cf-9c4c-beda02730881"
                                }
                            ],
                            "bsonType": "string"
                        }
                    }
                }
            }
        }
    })";

static const std::string kEncryptionInfo =
    R"({
        "schema": {
            "db.test": {
                "fields": [
                    {
                        "path": "ssn",
                        "keyId": {
                            "$uuid": "1362d0ed-6182-478e-bb8a-ebcc53b91aa1"
                        },
                        "bsonType": "int",
                        "queries" : { "queryType" : "equality", "contention": 0 }
                    },
                    {
                        "path": "user.account",
                        "keyId": {
                            "$uuid": "93282c77-9a6b-47cf-9c4c-beda02730881"
                        },
                        "bsonType": "string",
                        "queries" : { "queryType" : "equality", "contention": 0 }
                    }
                ]
            }
        }
    })";

void replace_str(std::string& str, mongo::StringData search, mongo::StringData replace) {
    auto pos = str.find(search.toString());
    if (pos == std::string::npos)
        return;
    str.replace(pos, search.size(), replace.toString());
}

void replace_str_and_comma(std::string& str, mongo::StringData search, mongo::StringData replace) {
    auto pos = str.find(search.toString());
    if (pos == std::string::npos)
        return;

    // Also replace the character immediately after `search` if it is a comma. If not, replace
    // everthing from the previous comma (included) to the end of `search`.
    if (pos + search.size() < str.size() && str[pos + search.size()] == ',') {
        str.replace(pos, search.size() + 1, replace.toString());
    } else if (pos > 1) {
        auto commaPos = str.rfind(',', pos - 1);
        if (commaPos == std::string::npos)
            return;
        uassert(mongo::ErrorCodes::BadValue,
                "only space and newline is expected between preceding comma and search pattern",
                std::all_of(str.begin() + commaPos + 1, str.begin() + pos, [](char c) {
                    return std::isspace(c);
                }));
        str.replace(commaPos, (pos - commaPos) + search.size(), replace.toString());
    }
}

void replace_encrypt_info(std::string& str) {
    if (str.find("\"bulkWrite\": 1") != std::string::npos) {
        // BulkWrite expects its encryptionInformation in nsInfo[0] unlike other comments.
        replace_str(
            str, "\"db.test\"", "\"db.test\", \"encryptionInformation\" : " + kEncryptionInfo);

        // addPlaceHoldersForBulkWrite uses BulkWriteCommandRequest::toBSON which causes
        // EncryptionInformation to be serialized, which includes the "type" field, see
        // its definition in fle_field_schema.idl.
        replace_str(str, "\"schema\"", "\"type\": 1, \"schema\"");

        // See above, BulkWrite expects encryptionInformation below nsInfo, so we have to replace
        // the placeholder used by other commands and corresponding comma
        // so the document remains well formed.
        replace_str_and_comma(str, "\"encryptionInformation\" : <ENCRYPTINFO>", "");
    } else {
        replace_str(str, "<ENCRYPTINFO>", kEncryptionInfo);
    }
}

#if defined(MONGO_CRYPT_UNITTEST_DYNAMIC)

uint64_t (*mongo_crypt_v1_get_version)(void);
const char* (*mongo_crypt_v1_get_version_str)(void);
mongo_crypt_v1_status* (*mongo_crypt_v1_status_create)(void);
void (*mongo_crypt_v1_status_destroy)(mongo_crypt_v1_status*);
int (*mongo_crypt_v1_status_get_error)(const mongo_crypt_v1_status*);
const char* (*mongo_crypt_v1_status_get_explanation)(const mongo_crypt_v1_status*);
int (*mongo_crypt_v1_status_get_code)(const mongo_crypt_v1_status*);

mongo_crypt_v1_lib* (*mongo_crypt_v1_lib_create)(mongo_crypt_v1_status*);
int (*mongo_crypt_v1_lib_destroy)(mongo_crypt_v1_lib*, mongo_crypt_v1_status*);

mongo_crypt_v1_query_analyzer* (*mongo_crypt_v1_query_analyzer_create)(mongo_crypt_v1_lib*,
                                                                       mongo_crypt_v1_status*);
void (*mongo_crypt_v1_query_analyzer_destroy)(mongo_crypt_v1_query_analyzer*);
uint8_t* (*mongo_crypt_v1_analyze_query)(mongo_crypt_v1_query_analyzer*,
                                         const uint8_t*,
                                         const char*,
                                         uint32_t,
                                         uint32_t*,
                                         mongo_crypt_v1_status*);
void (*mongo_crypt_v1_bson_free)(uint8_t*);

#define LOAD_API_FUNC(shlib, name)                                 \
    do {                                                           \
        auto swFunc = shlib->getFunctionAs<decltype(name)>(#name); \
        uassertStatusOK(swFunc.getStatus());                       \
        name = swFunc.getValue();                                  \
    } while (0)

static void mongo_crypt_v1_api_load(std::unique_ptr<mongo::SharedLibrary>& shLibHandle) {
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_get_version);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_get_version_str);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_status_create);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_status_destroy);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_status_get_error);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_status_get_explanation);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_status_get_code);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_lib_create);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_lib_destroy);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_query_analyzer_create);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_query_analyzer_destroy);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_analyze_query);
    LOAD_API_FUNC(shLibHandle, mongo_crypt_v1_bson_free);
}
#endif

class MongoCryptTest : public mongo::unittest::Test {
protected:
    void resetLoggingConfig() {
        auto& logManager = mongo::logv2::LogManager::global();
        mongo::logv2::LogDomainGlobal::ConfigurationOptions logConfig;
        uassertStatusOK(logManager.getGlobalDomainInternal().configure(logConfig));
    }

    void setUp() override {
        status = mongo_crypt_v1_status_create();
        ASSERT(status);

        lib = mongo_crypt_v1_lib_create(status);
        ASSERT(lib);

        // The library disables logging to console during mongo_crypt_v1_lib_create(),
        // which means that if this test is statically-linked with the library, the unit test
        // logs will never output to stdout. This re-enables logging to console.
        resetLoggingConfig();
    }

    void tearDown() override {
        if (lib) {
            int code = mongo_crypt_v1_lib_destroy(lib, status);
            ASSERT_EQ(code, MONGO_CRYPT_V1_SUCCESS);
            lib = nullptr;
        }

        mongo_crypt_v1_status_destroy(status);
        status = nullptr;
    }

    /**
     * toBSONForAPI is a custom converter from json which satisfies the unusual uint8_t type that
     * mongo_crypt uses for BSON. The intermediate BSONObj is also returned since its lifetime
     * governs the lifetime of the uint8_t*.
     */
    auto toBSONForAPI(const char* json) {
        auto bson = mongo::fromjson(json);
        return std::make_pair(static_cast<const uint8_t*>(static_cast<const void*>(bson.objdata())),
                              bson);
    }

    /**
     * fromBSONForAPI is a custom converter to json which satisfies the unusual uint8_t type that
     * mongo_crypt uses for BSON.
     */
    auto fromBSONForAPI(const uint8_t* bson) {
        return mongo::tojson(
            mongo::BSONObj(static_cast<const char*>(static_cast<const void*>(bson))),
            mongo::JsonStringFormat::LegacyStrict);
    }

    auto checkAnalysis(const char* inputJSON, uint32_t* outputLen) {
        auto matcher = mongo_crypt_v1_query_analyzer_create(lib, nullptr);
        ASSERT(matcher);
        ON_BLOCK_EXIT([matcher] { mongo_crypt_v1_query_analyzer_destroy(matcher); });

        auto inputBSON = toBSONForAPI(inputJSON);
        auto ret =
            mongo_crypt_v1_analyze_query(matcher, inputBSON.first, "db.test", 7, outputLen, status);
        return ret;
    }

    void checkAnalysisSuccess(const char* inputJSON, const char* outputJSON) {
        uint32_t bson_len = 0;
        auto ret = checkAnalysis(inputJSON, &bson_len);
        ON_BLOCK_EXIT([ret] { mongo_crypt_v1_bson_free(ret); });

        ASSERT(ret);

        auto expectedBSON = mongo::fromjson(outputJSON);
        auto outputBSON = mongo::BSONObj(reinterpret_cast<const char*>(ret));
        ASSERT_BSONOBJ_EQ(expectedBSON, outputBSON);
    }

    void checkAnalysisFailure(const char* inputJSON,
                              mongo_crypt_v1_error expectedError,
                              int expectedExceptionCode = 0) {
        uint32_t bson_len = 0;
        auto ret = checkAnalysis(inputJSON, &bson_len);
        ON_BLOCK_EXIT([ret] { mongo_crypt_v1_bson_free(ret); });

        ASSERT(!ret);
        ASSERT_EQ(bson_len, 0);
        ASSERT_EQ(mongo_crypt_v1_status_get_error(status), expectedError);
        if (expectedExceptionCode) {
            ASSERT_EQ(mongo_crypt_v1_status_get_code(status), expectedExceptionCode);
        }
    }

    void analyzeValidCommandCommon(const char* inputCmd,
                                   const char* transformedCmd,
                                   bool hasEncryptionPlaceholders = true,
                                   bool schemaRequiresEncryption = true) {
        std::string input =
            R"({
                <CMD>,
                "jsonSchema" : <SCHEMA>,
                "isRemoteSchema" : false,
                "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                "$db": "db"
            })";
        std::string output =
            R"({
                "hasEncryptionPlaceholders" : <HAS_ENC>,
                "schemaRequiresEncryption" : <REQ_ENC>,
                "result" : {
                    <CMD>,
                    "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
                }
            })";
        replace_str(input, "<CMD>", inputCmd);
        replace_str(input, "<SCHEMA>", kSchema);
        replace_str(output, "<HAS_ENC>", hasEncryptionPlaceholders ? "true" : "false");
        replace_str(output, "<REQ_ENC>", schemaRequiresEncryption ? "true" : "false");
        replace_str(output, "<CMD>", transformedCmd);
        checkAnalysisSuccess(input.c_str(), output.c_str());
    }

    void analyzeValidExplainCommandCommon(const char* inputCmd,
                                          const char* transformedCmd,
                                          bool hasEncryptionPlaceholders = true,
                                          bool schemaRequiresEncryption = true) {
        std::string input =
            R"({
                "explain" : {
                    <CMD>,
                    "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } }
                },
                "jsonSchema" : <SCHEMA>,
                "isRemoteSchema" : false,
                "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                "$db": "db"
            })";
        std::string output =
            R"({
                "hasEncryptionPlaceholders" : <HAS_ENC>,
                "schemaRequiresEncryption" : <REQ_ENC>,
                "result" : {
                    "explain" : {
                        <CMD>,
                        "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
                    },
                    "verbosity" : "allPlansExecution"
                }
            })";
        replace_str(input, "<CMD>", inputCmd);
        replace_str(input, "<SCHEMA>", kSchema);
        replace_str(output, "<HAS_ENC>", hasEncryptionPlaceholders ? "true" : "false");
        replace_str(output, "<REQ_ENC>", schemaRequiresEncryption ? "true" : "false");
        replace_str(output, "<CMD>", transformedCmd);
        checkAnalysisSuccess(input.c_str(), output.c_str());
    }

    void analyzeValidExplainCommandApiCommands(const char* inputCmd,
                                               const char* transformedCmd,
                                               bool hasEncryptionPlaceholders = true,
                                               bool schemaRequiresEncryption = true) {
        std::string input =
            R"({
                "explain" : {
                    <CMD>,
                    "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } }
                },
                "jsonSchema" : <SCHEMA>,
                "isRemoteSchema" : false,
                "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                "$db": "db",
                "apiVersion": "1",
                "apiDeprecationErrors": true,
                "apiStrict": true
            })";
        std::string output =
            R"({
                "hasEncryptionPlaceholders" : <HAS_ENC>,
                "schemaRequiresEncryption" : <REQ_ENC>,
                "result" : {
                    "explain" : {
                        <CMD>,
                        "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
                    },
                    "apiVersion": "1",
                    "apiDeprecationErrors": true,
                    "apiStrict": true,
                    "verbosity" : "allPlansExecution"
                }
            })";

        replace_str(input, "<CMD>", inputCmd);
        replace_str(input, "<SCHEMA>", kSchema);
        replace_str(output, "<HAS_ENC>", hasEncryptionPlaceholders ? "true" : "false");
        replace_str(output, "<REQ_ENC>", schemaRequiresEncryption ? "true" : "false");
        replace_str(output, "<CMD>", transformedCmd);
        checkAnalysisSuccess(input.c_str(), output.c_str());
    }

    void analyzeValidFLE2CommandCommon(const char* inputCmd,
                                       const char* transformedCmd,
                                       bool hasEncryptionPlaceholders = true,
                                       bool schemaRequiresEncryption = true) {
        std::string input =
            R"({
                <CMD>,
                "encryptionInformation" : <ENCRYPTINFO>,
                "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                "$db": "test"
            })";
        std::string output =
            R"({
                "hasEncryptionPlaceholders" : <HAS_ENC>,
                "schemaRequiresEncryption" : <REQ_ENC>,
                "result" : {
                    <CMD>,
                    "encryptionInformation" : <ENCRYPTINFO>,
                    "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
                }
            })";
        replace_str(input, "<CMD>", inputCmd);
        replace_encrypt_info(input);
        replace_str(output, "<HAS_ENC>", hasEncryptionPlaceholders ? "true" : "false");
        replace_str(output, "<REQ_ENC>", schemaRequiresEncryption ? "true" : "false");
        replace_str(output, "<CMD>", transformedCmd);
        replace_encrypt_info(output);
        checkAnalysisSuccess(input.c_str(), output.c_str());
    }

    void analyzeValidFLE2ExplainCommandCommon(const char* inputCmd,
                                              const char* transformedCmd,
                                              bool hasEncryptionPlaceholders = true,
                                              bool schemaRequiresEncryption = true) {
        std::string input =
            R"({
                "explain" : {
                    <CMD>,
                    "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } }
                },
                "encryptionInformation" : <ENCRYPTINFO>,
                "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                "$db": "test"
            })";
        std::string output =
            R"({
                "hasEncryptionPlaceholders" : <HAS_ENC>,
                "schemaRequiresEncryption" : <REQ_ENC>,
                "result" : {
                    "explain" : {
                        <CMD>,
                        "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } },
                        "encryptionInformation" : <ENCRYPTINFO>
                    },
                    "verbosity" : "allPlansExecution"
                }
            })";
        replace_str(input, "<CMD>", inputCmd);
        replace_encrypt_info(input);
        replace_str(output, "<HAS_ENC>", hasEncryptionPlaceholders ? "true" : "false");
        replace_str(output, "<REQ_ENC>", schemaRequiresEncryption ? "true" : "false");
        replace_str(output, "<CMD>", transformedCmd);
        replace_encrypt_info(output);
        checkAnalysisSuccess(input.c_str(), output.c_str());
    }

    mongo_crypt_v1_status* status = nullptr;
    mongo_crypt_v1_lib* lib = nullptr;
};

TEST_F(MongoCryptTest, GetVersionReturnsReasonableValues) {
    boost::io::ios_all_saver ias(std::cerr);

    uint64_t version = mongo_crypt_v1_get_version();
    const char* versionStr = mongo_crypt_v1_get_version_str();
    std::cerr << "Mongo Crypt Library Version: " << versionStr << ", " << std::hex << version
              << std::endl;
    ASSERT(version >= 0x0006'0000'0000'0000);
    ASSERT(versionStr);
}

TEST_F(MongoCryptTest, InitializationIsSuccessful) {
    ASSERT_EQ(MONGO_CRYPT_V1_SUCCESS, mongo_crypt_v1_status_get_error(status));
    ASSERT(lib);
}

TEST_F(MongoCryptTest, DoubleInitializationFails) {
    auto lib2 = mongo_crypt_v1_lib_create(status);

    ASSERT(!lib2);
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_LIBRARY_ALREADY_INITIALIZED,
              mongo_crypt_v1_status_get_error(status));
}

TEST_F(MongoCryptTest, DoubleDestructionFails) {
    ASSERT_EQ(MONGO_CRYPT_V1_SUCCESS, mongo_crypt_v1_lib_destroy(lib, status));
    ASSERT_EQ(MONGO_CRYPT_V1_SUCCESS, mongo_crypt_v1_status_get_error(status));

    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_crypt_v1_lib_destroy(lib, status));
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_crypt_v1_status_get_error(status));
    lib = nullptr;
}

TEST_F(MongoCryptTest, BadLibHandleDestructionFails) {
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE, mongo_crypt_v1_lib_destroy(nullptr, status));
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE, mongo_crypt_v1_status_get_error(status));

    int dummy;
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE,
              mongo_crypt_v1_lib_destroy(reinterpret_cast<mongo_crypt_v1_lib*>(&dummy), status));
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE, mongo_crypt_v1_status_get_error(status));
}

TEST_F(MongoCryptTest, QueryAnalyzerCreateWithUninitializedLibFails) {
    ASSERT_EQ(MONGO_CRYPT_V1_SUCCESS, mongo_crypt_v1_lib_destroy(lib, status));

    auto analyzer = mongo_crypt_v1_query_analyzer_create(lib, status);
    lib = nullptr;

    ASSERT(!analyzer);
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_crypt_v1_status_get_error(status));
}

TEST_F(MongoCryptTest, QueryAnalyzerCreateWithBadLibHandleFails) {
    int dummy;
    auto analyzer =
        mongo_crypt_v1_query_analyzer_create(reinterpret_cast<mongo_crypt_v1_lib*>(&dummy), status);
    ASSERT(!analyzer);
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE, mongo_crypt_v1_status_get_error(status));

    analyzer = mongo_crypt_v1_query_analyzer_create(nullptr, status);
    ASSERT(!analyzer);
    ASSERT_EQ(MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE, mongo_crypt_v1_status_get_error(status));
}

DEATH_TEST_F(MongoCryptTest, LibDestructionWithExistingAnalyzerFails, "") {
    auto analyzer = mongo_crypt_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_crypt_v1_lib_destroy(lib, status);
}

DEATH_TEST_F(MongoCryptTest, AnalyzeQueryNullHandleFails, "") {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "isRemoteSchema": false,
            "$db": "test"
        })";
    auto inputBSON = toBSONForAPI(input.c_str());
    uint32_t bsonLen = 0;
    mongo_crypt_v1_analyze_query(nullptr, inputBSON.first, "db.test", 7, &bsonLen, status);
}

DEATH_TEST_F(MongoCryptTest, AnalyzeQueryNullInputBSONFails, "") {
    uint32_t bsonLen = 0;
    auto analyzer = mongo_crypt_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_crypt_v1_analyze_query(analyzer, nullptr, "db.test", 7, &bsonLen, status);
}

DEATH_TEST_F(MongoCryptTest, AnalyzerQueryNullBSONLenFails, "") {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "isRemoteSchema": false,
            "$db": "test"
        })";
    auto inputBSON = toBSONForAPI(input.c_str());
    auto analyzer = mongo_crypt_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_crypt_v1_analyze_query(analyzer, inputBSON.first, "db.test", 7, nullptr, status);
}

TEST_F(MongoCryptTest, AnalyzeQueryNullNamespaceSuccessful) {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "isRemoteSchema": false,
            "$db": "test"
        })";
    auto inputBSON = toBSONForAPI(input.c_str());
    uint32_t bsonLen = 0;
    auto analyzer = mongo_crypt_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    ON_BLOCK_EXIT([analyzer] { mongo_crypt_v1_query_analyzer_destroy(analyzer); });

    auto output =
        mongo_crypt_v1_analyze_query(analyzer, inputBSON.first, nullptr, 0, &bsonLen, status);
    ON_BLOCK_EXIT([output] { mongo_crypt_v1_bson_free(output); });
    ASSERT(output);
}

TEST_F(MongoCryptTest, AnalyzeQueryInputMissingJsonSchema) {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "isRemoteSchema": false,
            "$db": "test"
        })";
    checkAnalysisFailure(
        input.c_str(), mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION, 51073);
}

TEST_F(MongoCryptTest, AnalyzeQueryInputMissingIsRemoteSchema) {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "$db": "test"
        })";
    checkAnalysisFailure(
        input.c_str(), mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION, 31104);
}

TEST_F(MongoCryptTest, AnalyzeQueryInputHasUnknownCommand) {
    std::string input =
        R"({
            "foo" : "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "isRemoteSchema": false,
            "$db": "test"
        })";
    checkAnalysisFailure(input.c_str(),
                         mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::CommandNotFound);
}

static const char* kFindCmd =
    R"(
        "find": "test",
        "filter": {
            "user.account": "secret"
        }
    )";

static const char* kTransformedFindCmd =
    R"(
        "find" : "test",
        "filter" : { "user.account" : {
            "$eq" : { "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" }
        } }
    )";

static const char* kTransformedFLE2FindCmd =
    R"(
        "find" : "test",
        "filter" : { "user.account" : {
            "$eq" : { "$binary" :
            "A18AAAAQdAACAAAAEGEAAgAAAAVraQAQAAAABJMoLHeaa0fPnEy+2gJzCIEFa3UAEAAAAASTKCx3mmtHz5xMvtoCcwiBAnYABwAAAHNlY3JldAASY20AAAAAAAAAAAAA", "$type" : "06" }
        } }
    )";

TEST_F(MongoCryptTest, AnalyzeValidFindCommand) {
    analyzeValidCommandCommon(kFindCmd, kTransformedFindCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainFindCommand) {
    analyzeValidExplainCommandCommon(kFindCmd, kTransformedFindCmd);
}

TEST_F(MongoCryptTest, analyzeValidExplainFindCommandApiCommands) {
    analyzeValidExplainCommandApiCommands(kFindCmd, kTransformedFindCmd, true, true);
}

TEST_F(MongoCryptTest, AnalyzeValidFLE2FindCommand) {
    analyzeValidFLE2CommandCommon(kFindCmd, kTransformedFLE2FindCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidFLE2ExplainFindCommand) {
    analyzeValidFLE2ExplainCommandCommon(kFindCmd, kTransformedFLE2FindCmd);
}

static const char* kAggregateCmd =
    R"(
        "aggregate": "test",
        "pipeline": [ { "$match": { "user.account": "secret" } } ],
        "cursor": {}
    )";

static const char* kTransformedAggregateCmd =
    R"(
        "aggregate" : "test",
        "pipeline" : [ { "$match" : {
            "user.account" : {
                "$eq" : { "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" }
            }
        } } ],
        "cursor" : {}
    )";


TEST_F(MongoCryptTest, AnalyzeValidAggregateCommand) {
    analyzeValidCommandCommon(kAggregateCmd, kTransformedAggregateCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainAggregateCommand) {
    analyzeValidExplainCommandCommon(kAggregateCmd, kTransformedAggregateCmd);
}

static const char* kFindAndModifyCmd =
    R"(
        "findAndModify": "test",
        "query": { "user.account": "secret" },
        "update": { "$set": { "ssn" : 4145 } }
    )";

static const char* kTransformedFindAndModifyCmd =
    R"(
        "findAndModify" : "test",
        "query" : {
            "user.account" : {
                "$eq" : { "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" }
            }
        },
        "update" : { "$set" : {
            "ssn" : { "$binary" : "ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ADEQAAAA", "$type" : "06" }
        } }
    )";

TEST_F(MongoCryptTest, AnalyzeValidFindAndModifyCommand) {
    analyzeValidCommandCommon(kFindAndModifyCmd, kTransformedFindAndModifyCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainFindAndModifyCommand) {
    analyzeValidExplainCommandCommon(kFindAndModifyCmd, kTransformedFindAndModifyCmd);
}

static const char* kCountCmd =
    R"(
        "count": "test",
        "query": { "user.account": "secret" }
    )";

static const char* kTransformedCountCmd =
    R"(
        "count" : "test",
        "query" : {
            "user.account" : {
                "$eq" : { "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" }
            }
        }
    )";

TEST_F(MongoCryptTest, AnalyzeValidCountCommand) {
    analyzeValidCommandCommon(kCountCmd, kTransformedCountCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainCountCommand) {
    analyzeValidExplainCommandCommon(kCountCmd, kTransformedCountCmd);
}

static const char* kDistinctCmd =
    R"(
        "distinct": "test",
        "key": "user.pin",
        "query": { "user.account" : "secret" }
    )";

static const char* kTransformedDistinctCmd =
    R"(
        "distinct" : "test",
        "key": "user.pin",
        "query": {
            "user.account" : {
                "$eq" : {
                    "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==",
                    "$type" : "06"
                }
            }
        }
    )";

TEST_F(MongoCryptTest, AnalyzeValidDistinctCommand) {
    analyzeValidCommandCommon(kDistinctCmd, kTransformedDistinctCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainDistinctCommand) {
    analyzeValidExplainCommandCommon(kDistinctCmd, kTransformedDistinctCmd);
}

static const char* kUpdateCmd =
    R"(
        "update": "test",
        "updates": [
            { "q": { "ssn": 1234567890 }, "u": { "$set" : { "user.account" : "secret" } } }
        ]
    )";

static const char* kTransformedUpdateCmd =
    R"(
        "update" : "test",
        "updates": [{
            "q" : {
                "ssn" : { "$eq" : { "$binary" : "ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ANIClkkA", "$type" : "06" } }
            },
            "u" : {
                "$set" : { "user.account" : {
                    "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06"
                } }
            },
            "multi" : false,
            "upsert" : false
        }]
    )";

TEST_F(MongoCryptTest, AnalyzeValidUpdateCommand) {
    analyzeValidCommandCommon(kUpdateCmd, kTransformedUpdateCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainUpdateCommand) {
    analyzeValidExplainCommandCommon(kUpdateCmd, kTransformedUpdateCmd);
}

static const char* kInsertCmd =
    R"(
        "insert" : "test",
        "documents": [{ "_id": 2, "ssn": 1234567890, "user": { "account": "secret" } }]
    )";

static const char* kTransformedInsertCmd =
    R"(
        "insert" : "test",
        "documents": [
            {
                "_id": 2,
                "ssn": { "$binary" : "ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ANIClkkA", "$type" : "06" },
                "user": { "account": { "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" }}
            }
        ]
    )";

TEST_F(MongoCryptTest, AnalyzeValidInsertCommand) {
    analyzeValidCommandCommon(kInsertCmd, kTransformedInsertCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainInsertCommand) {
    analyzeValidExplainCommandCommon(kInsertCmd, kTransformedInsertCmd);
}

static const char* kDeleteCmd =
    R"(
        "delete": "test",
        "deletes": [ { "q": { "ssn": 1234567890 }, "limit" : 1 } ]
    )";

static const char* kTransformedDeleteCmd =
    R"(
        "delete" : "test",
        "deletes": [ {
            "q" : {
                "ssn" : { "$eq" : { "$binary" : "ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ANIClkkA", "$type" : "06" } }
            },
            "limit" : 1
        } ]
    )";

TEST_F(MongoCryptTest, AnalyzeValidDeleteCommand) {
    analyzeValidCommandCommon(kDeleteCmd, kTransformedDeleteCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainDeleteCommand) {
    analyzeValidExplainCommandCommon(kDeleteCmd, kTransformedDeleteCmd);
}

static const char* kBulkWriteInsertCmd =
    R"(
        "bulkWrite": 1,
        "ops": [
            {
                "insert": 0,
                "document": {
                    "_id": 2, "ssn": 1234567890, "user": { "account": "secret" }
                }
            }
        ],
        "nsInfo": [{"ns": "db.test"}]
    )";

static const char* kTransformedBulkWriteInsertCmd =
    R"(
        "bulkWrite": 1,
        "ops": [
            {
                "insert": 0,
                "document": {
                    "_id": 2,
                    "ssn": { "$binary" : "A1gAAAAQdAABAAAAEGEAAgAAAAVraQAQAAAABBNi0O1hgkeOu4rrzFO5GqEFa3UAEAAAAAQTYtDtYYJHjruK68xTuRqhEHYA0gKWSRJjbQAAAAAAAAAAAAA=", "$type" : "06" },
                    "user": { "account": { "$binary" : "A18AAAAQdAABAAAAEGEAAgAAAAVraQAQAAAABJMoLHeaa0fPnEy+2gJzCIEFa3UAEAAAAASTKCx3mmtHz5xMvtoCcwiBAnYABwAAAHNlY3JldAASY20AAAAAAAAAAAAA", "$type" : "06" }}
                }
            }
        ],
        "nsInfo": [{"ns": "db.test"}]
    )";

TEST_F(MongoCryptTest, AnalyzeValidBulkWriteInsertCommand) {
    analyzeValidFLE2CommandCommon(kBulkWriteInsertCmd, kTransformedBulkWriteInsertCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainBulkWriteInsertCommand) {
    analyzeValidFLE2ExplainCommandCommon(kBulkWriteInsertCmd, kTransformedBulkWriteInsertCmd);
}

static const char* kBulkWriteUpdateCmd =
    R"(
        "bulkWrite": 1,
        "ops": [
            {
                "update": 0,
                "filter": {"ssn": 1234567890},
                "updateMods": {
                    $set: {
                        "ssn": 1234567890,
                        "user": {
                            "account": "secret"
                        }
                    }
                }
            }
        ],
        "nsInfo": [{"ns": "db.test"}]
    )";

static const char* kTransformedBulkWriteUpdateCmd =
    R"(
        "bulkWrite": 1,
        "ops": [
            {
                "update": 0,
                "filter": {"ssn": {"$eq": { "$binary" : "A1gAAAAQdAACAAAAEGEAAgAAAAVraQAQAAAABBNi0O1hgkeOu4rrzFO5GqEFa3UAEAAAAAQTYtDtYYJHjruK68xTuRqhEHYA0gKWSRJjbQAAAAAAAAAAAAA=", "$type" : "06" }}},
                "multi": false,
                "updateMods": {
                    $set: {
                        "ssn": { "$binary" : "A1gAAAAQdAABAAAAEGEAAgAAAAVraQAQAAAABBNi0O1hgkeOu4rrzFO5GqEFa3UAEAAAAAQTYtDtYYJHjruK68xTuRqhEHYA0gKWSRJjbQAAAAAAAAAAAAA=", "$type" : "06" },
                        "user": { "account": { "$binary" : "A18AAAAQdAABAAAAEGEAAgAAAAVraQAQAAAABJMoLHeaa0fPnEy+2gJzCIEFa3UAEAAAAASTKCx3mmtHz5xMvtoCcwiBAnYABwAAAHNlY3JldAASY20AAAAAAAAAAAAA", "$type" : "06" }}
                    }
                },
                "upsert": false
            }
        ],
        "nsInfo": [{"ns": "db.test"}]
    )";

TEST_F(MongoCryptTest, AnalyzeValidBulkWriteUpdateCommand) {
    analyzeValidFLE2CommandCommon(kBulkWriteUpdateCmd, kTransformedBulkWriteUpdateCmd);
}

TEST_F(MongoCryptTest, AnalyzeValidExplainBulkWriteUpdateCommand) {
    analyzeValidFLE2ExplainCommandCommon(kBulkWriteUpdateCmd, kTransformedBulkWriteUpdateCmd);
}

TEST_F(MongoCryptTest, AnalyzeExplainInsideExplain) {
    std::string input =
        R"({
            "explain": {
                "explain": {
                    "find" : "test",
                    "filter" : {
                        "user.account" : "secret"
                    }
                },
                "$db": "test"
            },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "$db": "test"
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(input.c_str(),
                         mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::IllegalOperation);
}

TEST_F(MongoCryptTest, AnalyzeExplainUnknownCommand) {
    std::string input =
        R"({
            "explain": {
                "foo" : "test",
                "filter" : {
                    "user.account" : "secret"
                },
                "$db": "test"
            },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "$db": "test"
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(input.c_str(),
                         mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::CommandNotFound);
}

TEST_F(MongoCryptTest, AnalyzeExplainWithoutOuterDb) {
    std::string input =
        R"({
            "explain" : {
                "find" : "test",
                "filter" : {
                    "user.account" : "secret"
                },
                "$db" : "test"
            },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(
        input.c_str(), mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION, 40414);
}

TEST_F(MongoCryptTest, AnalyzeExplainWithMismatchedDb) {
    std::string input =
        R"({
            "explain" : {
                "find" : "test",
                "filter" : {
                    "user.account" : "secret"
                },
                "$db" : "foo"
            },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "$db" : "test"
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(input.c_str(),
                         mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::InvalidNamespace);
}

TEST_F(MongoCryptTest, AnalyzeExplainWithInnerJsonSchema) {
    std::string input =
        R"({
            "explain" : {
                "find" : "test",
                "filter" : {
                    "user.account" : "secret"
                },
                "jsonSchema": <SCHEMA>,
                "$db" : "test"
            },
            "isRemoteSchema": false,
            "$db" : "test"
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(
        input.c_str(), mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION, 6206601);
}

TEST_F(MongoCryptTest, AnalyzeExplainWithInnerIsRemoteSchema) {
    std::string input =
        R"({
            "explain" : {
                "find" : "test",
                "filter" : {
                    "user.account" : "secret"
                },
                "isRemoteSchema": false,
                "$db" : "test"
            },
            "jsonSchema": <SCHEMA>,
            "$db" : "test"
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(
        input.c_str(), mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION, 6206602);
}

TEST_F(MongoCryptTest, AnalyzeExplainWithInnerAndOuterEncryptionInformation) {
    std::string input =
        R"({
            "explain" : {
                <CMD>,
                "encryptionInformation": <ENCRYPTINFO>
            },
            "encryptionInformation": <ENCRYPTINFO>,
            "$db" : "test"
        })";
    replace_str(input, "<CMD>", kFindCmd);
    replace_str(input, "<ENCRYPTINFO>", kEncryptionInfo);
    replace_str(input, "<ENCRYPTINFO>", kEncryptionInfo);
    checkAnalysisFailure(
        input.c_str(), mongo_crypt_v1_error::MONGO_CRYPT_V1_ERROR_EXCEPTION, 6650801);
}

TEST_F(MongoCryptTest, AnalyzeExplainWithInnerEncryptionInformation) {
    std::string input =
        R"({
            "explain" : {
                <CMD>,
                "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                "encryptionInformation": <ENCRYPTINFO>
            },
            "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
            "$db" : "test"
        })";
    std::string output =
        R"({
            "hasEncryptionPlaceholders" : true,
            "schemaRequiresEncryption" : true,
            "result" : {
                "explain" : {
                    <CMD>,
                    "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } },
                    "encryptionInformation" : <ENCRYPTINFO>
                },
                "verbosity" : "allPlansExecution"
            }
        })";
    replace_str(input, "<CMD>", kFindCmd);
    replace_str(input, "<ENCRYPTINFO>", kEncryptionInfo);
    replace_str(output, "<CMD>", kTransformedFLE2FindCmd);
    replace_str(output, "<ENCRYPTINFO>", kEncryptionInfo);
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

#if !defined(MONGO_CRYPT_UNITTEST_DYNAMIC)
class OpmsgProcessTest : public mongo::ServiceContextTest {
protected:
    OpmsgProcessTest() = default;

    mongo::ServiceContext::UniqueOperationContext _opCtx{makeOperationContext()};
};

TEST_F(OpmsgProcessTest, Basic) {
    auto test1 = R"({
        "find": "test",
        "filter": {
            "user.account": "secret"
        },
        "jsonSchema": {
            "type": "object",
            "properties": {
                "ssn": {
                    "encrypt": {
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        "keyId": [
                            {
                                "$uuid": "1362d0ed-6182-478e-bb8a-ebcc53b91aa1"
                            }
                        ],
                        "bsonType": "long"
                    }
                },
                "user": {
                    "type": "object",
                    "properties": {
                        "account": {
                            "encrypt": {
                                "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                                "keyId": [
                                    {
                                        "$uuid": "93282c77-9a6b-47cf-9c4c-beda02730881"
                                    }
                                ],
                                "bsonType": "string"
                            }
                        }
                    }
                }
            }
        },
        "isRemoteSchema": false,
        "lsid": {
            "id": {
                "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
            }
        },
        "$db": "test"
    })";

    auto ret =
        mongo::analyzeQuery(mongo::fromjson(test1),
                            _opCtx.get(),
                            mongo::NamespaceString::createNamespaceString_forTest("db.test"));

    ASSERT_TRUE(!ret.isEmpty());

    std::string json =
        R"({ "hasEncryptionPlaceholders" : true, "schemaRequiresEncryption" : true, "result" : )"
        R"({ "find" : "test", "filter" : { "user.account" : { "$eq" : { "$binary" : )"
        R"("ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : )"
        R"("06" } } }, "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", )"
        R"("$type" : "04" } } } })";

    auto retJson = mongo::tojson(ret, mongo::JsonStringFormat::LegacyStrict);

    ASSERT_EQ(json, retJson);
}
#endif

}  // namespace

// Define main function as an entry to these tests.
//
// Note that we don't use the main() defined for most other unit tests so that we can avoid double
// calling runGlobalInitializers(), which is called both from the regular unit test main() and from
// the Mongo Crypt Shared Library intializer function that gets tested here.
int main(const int argc, const char* const* const argv) {
    // See comment by the same code block in mongo_embedded_test.cpp
    auto ret = mongo::runGlobalInitializers(std::vector<std::string>{argv, argv + argc});
    if (!ret.isOK()) {
        std::cerr << "Global initilization failed";
        return static_cast<int>(mongo::ExitCode::fail);
    }

    ret = mongo::runGlobalDeinitializers();
    if (!ret.isOK()) {
        std::cerr << "Global deinitilization failed";
        return static_cast<int>(mongo::ExitCode::fail);
    }

    int result;
#if !defined(MONGO_CRYPT_UNITTEST_DYNAMIC)
    result = ::mongo::unittest::Suite::run(std::vector<std::string>(), "", "", 1);
#else
    try {
        auto swShLib = mongo::SharedLibrary::create(MONGO_CRYPT_LIBNAME);
        uassertStatusOK(swShLib.getStatus());
        std::cout << "Successfully loaded library " << MONGO_CRYPT_LIBNAME << std::endl;

        mongo_crypt_v1_api_load(swShLib.getValue());

        result = ::mongo::unittest::Suite::run(std::vector<std::string>(), "", "", 1);
    } catch (...) {
        auto status = mongo::exceptionToStatus();
        std::cerr << "Failed to load the Mongo Crypt Shared library - " << status << std::endl;
        return static_cast<int>(mongo::ExitCode::fail);
    }
#endif

    // This is the standard exit path for Mongo processes. See the mongo::quickExit() declaration
    // for more information.
    mongo::quickExit(result);
}
