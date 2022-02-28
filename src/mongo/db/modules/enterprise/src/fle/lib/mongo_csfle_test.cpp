/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 */

#include <algorithm>
#include <string>
#include <utility>

#include "fle/lib/mongo_csfle.h"

#include "mongo/base/initializer.h"
#include "mongo/bson/json.h"
#include "mongo/db/concurrency/locker_noop_client_observer.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/platform/shared_library.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/scopeguard.h"

#define MONGO_CSFLE_PREFIX "mongo_csfle_v1"

#if defined(_WIN32)
#define LIBEXT ".dll"
#elif defined(__APPLE__)
#define LIBEXT ".dylib"
#else
#define LIBEXT ".so"
#endif

#define MONGO_CSFLE_LIBNAME MONGO_CSFLE_PREFIX LIBEXT

#if !defined(CSFLE_UNITTEST_DYNAMIC)
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

void replace_str(std::string& str, mongo::StringData search, mongo::StringData replace) {
    auto pos = str.find(search.toString());
    if (pos == std::string::npos)
        return;
    str.replace(pos, search.size(), replace.toString());
}

#if defined(CSFLE_UNITTEST_DYNAMIC)

uint64_t (*mongo_csfle_v1_get_version)(void);
const char* (*mongo_csfle_v1_get_version_str)(void);
mongo_csfle_v1_status* (*mongo_csfle_v1_status_create)(void);
void (*mongo_csfle_v1_status_destroy)(mongo_csfle_v1_status*);
int (*mongo_csfle_v1_status_get_error)(const mongo_csfle_v1_status*);
const char* (*mongo_csfle_v1_status_get_explanation)(const mongo_csfle_v1_status*);
int (*mongo_csfle_v1_status_get_code)(const mongo_csfle_v1_status*);

mongo_csfle_v1_lib* (*mongo_csfle_v1_lib_create)(mongo_csfle_v1_status*);
int (*mongo_csfle_v1_lib_destroy)(mongo_csfle_v1_lib*, mongo_csfle_v1_status*);

mongo_csfle_v1_query_analyzer* (*mongo_csfle_v1_query_analyzer_create)(mongo_csfle_v1_lib*,
                                                                       mongo_csfle_v1_status*);
void (*mongo_csfle_v1_query_analyzer_destroy)(mongo_csfle_v1_query_analyzer*);
uint8_t* (*mongo_csfle_v1_analyze_query)(mongo_csfle_v1_query_analyzer*,
                                         const uint8_t*,
                                         const char*,
                                         uint32_t,
                                         uint32_t*,
                                         mongo_csfle_v1_status*);
void (*mongo_csfle_v1_bson_free)(uint8_t*);

#define LOAD_API_FUNC(shlib, name)                                 \
    do {                                                           \
        auto swFunc = shlib->getFunctionAs<decltype(name)>(#name); \
        uassertStatusOK(swFunc.getStatus());                       \
        name = swFunc.getValue();                                  \
    } while (0)

static void mongo_csfle_v1_api_load(std::unique_ptr<mongo::SharedLibrary>& shLibHandle) {
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_get_version);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_get_version_str);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_status_create);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_status_destroy);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_status_get_error);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_status_get_explanation);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_status_get_code);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_lib_create);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_lib_destroy);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_query_analyzer_create);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_query_analyzer_destroy);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_analyze_query);
    LOAD_API_FUNC(shLibHandle, mongo_csfle_v1_bson_free);
}
#endif

class CsfleTest : public mongo::unittest::Test {
protected:
    void setUp() override {
        status = mongo_csfle_v1_status_create();
        ASSERT(status);

        lib = mongo_csfle_v1_lib_create(status);
        ASSERT(lib);
    }

    void tearDown() override {
        if (lib) {
            int code = mongo_csfle_v1_lib_destroy(lib, status);
            ASSERT_EQ(code, MONGO_CSFLE_V1_SUCCESS);
            lib = nullptr;
        }

        mongo_csfle_v1_status_destroy(status);
        status = nullptr;
    }

    /**
     * toBSONForAPI is a custom converter from json which satisfies the unusual uint8_t type that
     * mongo_csfle uses for BSON. The intermediate BSONObj is also returned since its lifetime
     * governs the lifetime of the uint8_t*.
     */
    auto toBSONForAPI(const char* json) {
        auto bson = mongo::fromjson(json);
        return std::make_pair(static_cast<const uint8_t*>(static_cast<const void*>(bson.objdata())),
                              bson);
    }

    /**
     * fromBSONForAPI is a custom converter to json which satisfies the unusual uint8_t type that
     * mongo_csfle uses for BSON.
     */
    auto fromBSONForAPI(const uint8_t* bson) {
        return mongo::tojson(
            mongo::BSONObj(static_cast<const char*>(static_cast<const void*>(bson))),
            mongo::JsonStringFormat::LegacyStrict);
    }

    auto checkAnalysis(const char* inputJSON, uint32_t* outputLen) {
        auto matcher = mongo_csfle_v1_query_analyzer_create(lib, nullptr);
        ASSERT(matcher);
        ON_BLOCK_EXIT([matcher] { mongo_csfle_v1_query_analyzer_destroy(matcher); });

        auto inputBSON = toBSONForAPI(inputJSON);
        auto ret =
            mongo_csfle_v1_analyze_query(matcher, inputBSON.first, "db.test", 7, outputLen, status);
        return ret;
    }

    void checkAnalysisSuccess(const char* inputJSON, const char* outputJSON) {
        uint32_t bson_len = 0;
        auto ret = checkAnalysis(inputJSON, &bson_len);
        ON_BLOCK_EXIT([ret] { mongo_csfle_v1_bson_free(ret); });

        ASSERT(ret);

        auto expectedBSON = mongo::fromjson(outputJSON);
        auto outputBSON = mongo::BSONObj(reinterpret_cast<const char*>(ret));
        ASSERT_BSONOBJ_EQ(expectedBSON, outputBSON);
    }

    void checkAnalysisFailure(const char* inputJSON,
                              mongo_csfle_v1_error expectedError,
                              int expectedExceptionCode = 0) {
        uint32_t bson_len = 0;
        auto ret = checkAnalysis(inputJSON, &bson_len);
        ON_BLOCK_EXIT([ret] { mongo_csfle_v1_bson_free(ret); });

        ASSERT(!ret);
        ASSERT_EQ(bson_len, 0);
        ASSERT_EQ(mongo_csfle_v1_status_get_error(status), expectedError);
        if (expectedExceptionCode) {
            ASSERT_EQ(mongo_csfle_v1_status_get_code(status), expectedExceptionCode);
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
                "$db": "test"
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
                    "lsid" : { "id": { "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7" } },
                    "$db" : "test"
                },
                "jsonSchema" : <SCHEMA>,
                "isRemoteSchema" : false,
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

    mongo_csfle_v1_status* status = nullptr;
    mongo_csfle_v1_lib* lib = nullptr;
};

TEST_F(CsfleTest, GetVersionReturnsReasonableValues) {
    uint64_t version = mongo_csfle_v1_get_version();
    const char* versionStr = mongo_csfle_v1_get_version_str();
    std::cerr << "CSFLE Version: " << versionStr << ", " << std::hex << version << std::endl;
    ASSERT(version >= 0x05030000);
    ASSERT(versionStr);
}

TEST_F(CsfleTest, InitializationIsSuccessful) {
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_status_get_error(status));
    ASSERT(lib);
}

TEST_F(CsfleTest, DoubleInitializationFails) {
    auto lib2 = mongo_csfle_v1_lib_create(status);

    ASSERT(!lib2);
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_ALREADY_INITIALIZED,
              mongo_csfle_v1_status_get_error(status));
}

TEST_F(CsfleTest, DoubleDestructionFails) {
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_lib_destroy(lib, status));
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_status_get_error(status));

    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_csfle_v1_lib_destroy(lib, status));
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_csfle_v1_status_get_error(status));
    lib = nullptr;
}

TEST_F(CsfleTest, BadLibHandleDestructionFails) {
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_lib_destroy(nullptr, status));
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_status_get_error(status));

    int dummy;
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE,
              mongo_csfle_v1_lib_destroy(reinterpret_cast<mongo_csfle_v1_lib*>(&dummy), status));
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_status_get_error(status));
}

TEST_F(CsfleTest, QueryAnalyzerCreateWithUninitializedLibFails) {
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_lib_destroy(lib, status));

    auto analyzer = mongo_csfle_v1_query_analyzer_create(lib, status);
    lib = nullptr;

    ASSERT(!analyzer);
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_csfle_v1_status_get_error(status));
}

TEST_F(CsfleTest, QueryAnalyzerCreateWithBadLibHandleFails) {
    int dummy;
    auto analyzer =
        mongo_csfle_v1_query_analyzer_create(reinterpret_cast<mongo_csfle_v1_lib*>(&dummy), status);
    ASSERT(!analyzer);
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_status_get_error(status));

    analyzer = mongo_csfle_v1_query_analyzer_create(nullptr, status);
    ASSERT(!analyzer);
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_status_get_error(status));
}

DEATH_TEST_F(CsfleTest, LibDestructionWithExistingAnalyzerFails, "invariant") {
    auto analyzer = mongo_csfle_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_csfle_v1_lib_destroy(lib, status);
}

DEATH_TEST_F(CsfleTest, AnalyzeQueryNullHandleFails, "invariant") {
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
    mongo_csfle_v1_analyze_query(nullptr, inputBSON.first, "db.test", 7, &bsonLen, status);
}

DEATH_TEST_F(CsfleTest, AnalyzeQueryNullInputBSONFails, "invariant") {
    uint32_t bsonLen = 0;
    auto analyzer = mongo_csfle_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_csfle_v1_analyze_query(analyzer, nullptr, "db.test", 7, &bsonLen, status);
}

DEATH_TEST_F(CsfleTest, AnalyzerQueryNullBSONLenFails, "invariant") {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "isRemoteSchema": false,
            "$db": "test"
        })";
    auto inputBSON = toBSONForAPI(input.c_str());
    auto analyzer = mongo_csfle_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_csfle_v1_analyze_query(analyzer, inputBSON.first, "db.test", 7, nullptr, status);
}

TEST_F(CsfleTest, AnalyzeQueryNullNamespaceSuccessful) {
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
    auto analyzer = mongo_csfle_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    ON_BLOCK_EXIT([analyzer] { mongo_csfle_v1_query_analyzer_destroy(analyzer); });

    auto output =
        mongo_csfle_v1_analyze_query(analyzer, inputBSON.first, nullptr, 0, &bsonLen, status);
    ON_BLOCK_EXIT([output] { mongo_csfle_v1_bson_free(output); });
    ASSERT(output);
}

TEST_F(CsfleTest, AnalyzeQueryInputMissingJsonSchema) {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "isRemoteSchema": false,
            "$db": "test"
        })";
    checkAnalysisFailure(input.c_str(), mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION);
}

TEST_F(CsfleTest, AnalyzeQueryInputMissingIsRemoteSchema) {
    std::string input =
        R"({
            "find": "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "$db": "test"
        })";
    checkAnalysisFailure(input.c_str(), mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION);
}

TEST_F(CsfleTest, AnalyzeQueryInputHasUnknownCommand) {
    std::string input =
        R"({
            "foo" : "test",
            "filter": { "user.account": "secret" },
            "jsonSchema": {},
            "isRemoteSchema": false,
            "$db": "test"
        })";
    checkAnalysisFailure(input.c_str(),
                         mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION,
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

TEST_F(CsfleTest, AnalyzeValidFindCommand) {
    analyzeValidCommandCommon(kFindCmd, kTransformedFindCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainFindCommand) {
    analyzeValidExplainCommandCommon(kFindCmd, kTransformedFindCmd);
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


TEST_F(CsfleTest, AnalyzeValidAggregateCommand) {
    analyzeValidCommandCommon(kAggregateCmd, kTransformedAggregateCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainAggregateCommand) {
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

TEST_F(CsfleTest, AnalyzeValidFindAndModifyCommand) {
    analyzeValidCommandCommon(kFindAndModifyCmd, kTransformedFindAndModifyCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainFindAndModifyCommand) {
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

TEST_F(CsfleTest, AnalyzeValidCountCommand) {
    analyzeValidCommandCommon(kCountCmd, kTransformedCountCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainCountCommand) {
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

TEST_F(CsfleTest, AnalyzeValidDistinctCommand) {
    analyzeValidCommandCommon(kDistinctCmd, kTransformedDistinctCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainDistinctCommand) {
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

TEST_F(CsfleTest, AnalyzeValidUpdateCommand) {
    analyzeValidCommandCommon(kUpdateCmd, kTransformedUpdateCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainUpdateCommand) {
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

TEST_F(CsfleTest, AnalyzeValidInsertCommand) {
    analyzeValidCommandCommon(kInsertCmd, kTransformedInsertCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainInsertCommand) {
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

TEST_F(CsfleTest, AnalyzeValidDeleteCommand) {
    analyzeValidCommandCommon(kDeleteCmd, kTransformedDeleteCmd);
}

TEST_F(CsfleTest, AnalyzeValidExplainDeleteCommand) {
    analyzeValidExplainCommandCommon(kDeleteCmd, kTransformedDeleteCmd);
}

TEST_F(CsfleTest, AnalyzeExplainInsideExplain) {
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
                         mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::IllegalOperation);
}

TEST_F(CsfleTest, AnalyzeExplainUnknownCommand) {
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
                         mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::CommandNotFound);
}

TEST_F(CsfleTest, AnalyzeExplainWithoutOuterDb) {
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
        input.c_str(), mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION, 40414);
}

TEST_F(CsfleTest, AnalyzeExplainWithoutInnerDb) {
    std::string input =
        R"({
            "explain" : {
                "find" : "test",
                "filter" : {
                    "user.account" : "secret"
                }
            },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "$db" : "test"
        })";
    replace_str(input, "<SCHEMA>", kSchema);
    checkAnalysisFailure(
        input.c_str(), mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION, 40414);
}

TEST_F(CsfleTest, AnalyzeExplainWithMismatchedDb) {
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
                         mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION,
                         mongo::ErrorCodes::InvalidNamespace);
}

TEST_F(CsfleTest, AnalyzeExplainWithInnerJsonSchema) {
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
        input.c_str(), mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION, 6206601);
}

TEST_F(CsfleTest, AnalyzeExplainWithInnerIsRemoteSchema) {
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
        input.c_str(), mongo_csfle_v1_error::MONGO_CSFLE_V1_ERROR_EXCEPTION, 6206602);
}

#if !defined(CSFLE_UNITTEST_DYNAMIC)
class OpmsgProcessTest : public mongo::ServiceContextTest {
public:
    OpmsgProcessTest() = default;
    void setUp() final;
    void tearDown() final;

protected:
    mongo::ServiceContext::UniqueOperationContext _opCtx;
};

void OpmsgProcessTest::setUp() {
    mongo::ServiceContextTest::setUp();
    getServiceContext()->registerClientObserver(
        std::make_unique<mongo::LockerNoopClientObserver>());
    _opCtx = getClient()->makeOperationContext();
}

void OpmsgProcessTest::tearDown() {
    _opCtx = {};
    mongo::ServiceContextTest::tearDown();
}

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

    auto ret = mongo::analyzeQuery(
        mongo::fromjson(test1), _opCtx.get(), mongo::NamespaceString("db.test"));

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
// the CSFLE Library intializer function that gets tested here.
int main(const int argc, const char* const* const argv) {
    // See comment by the same code block in mongo_embedded_test.cpp
    auto ret = mongo::runGlobalInitializers(std::vector<std::string>{argv, argv + argc});
    if (!ret.isOK()) {
        std::cerr << "Global initilization failed";
        return EXIT_FAILURE;
    }

    ret = mongo::runGlobalDeinitializers();
    if (!ret.isOK()) {
        std::cerr << "Global deinitilization failed";
        return EXIT_FAILURE;
    }

    int result;
#if !defined(CSFLE_UNITTEST_DYNAMIC)
    result = ::mongo::unittest::Suite::run(std::vector<std::string>(), "", "", 1);
#else
    try {
        auto swShLib = mongo::SharedLibrary::create(MONGO_CSFLE_LIBNAME);
        uassertStatusOK(swShLib.getStatus());
        std::cout << "Successfully loaded library " << MONGO_CSFLE_LIBNAME << std::endl;

        mongo_csfle_v1_api_load(swShLib.getValue());

        result = ::mongo::unittest::Suite::run(std::vector<std::string>(), "", "", 1);
    } catch (...) {
        auto status = mongo::exceptionToStatus();
        std::cerr << "Failed to load the CSFLE library - " << status << std::endl;
        return EXIT_FAILURE;
    }
#endif

    // This is the standard exit path for Mongo processes. See the mongo::quickExit() declaration
    // for more information.
    mongo::quickExit(result);
}
