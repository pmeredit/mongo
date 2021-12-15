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
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
BSONObj analyzeQuery(BSONObj document, OperationContext* opCtx, NamespaceString ns);
}

namespace {

using mongo::ScopeGuard;

class CsfleTest : public mongo::unittest::Test {
protected:
    void setUp() override {
        status = mongo_csfle_v1_status_create();
        ASSERT(status);

        lib = mongo_csfle_v1_create(status);
        ASSERT(lib);
    }

    void tearDown() override {
        if (lib) {
            int code = mongo_csfle_v1_destroy(lib, status);
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

    mongo_csfle_v1_status* status = nullptr;
    mongo_csfle_v1_lib* lib = nullptr;
};

TEST_F(CsfleTest, InitializationIsSuccessful) {
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_status_get_error(status));
    ASSERT(lib);
}

TEST_F(CsfleTest, DoubleInitializationFails) {
    auto lib2 = mongo_csfle_v1_create(status);

    ASSERT(!lib2);
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_ALREADY_INITIALIZED,
              mongo_csfle_v1_status_get_error(status));
}

TEST_F(CsfleTest, DoubleDestructionFails) {
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_destroy(lib, status));
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_status_get_error(status));

    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_NOT_INITIALIZED, mongo_csfle_v1_destroy(lib, status));
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_LIBRARY_NOT_INITIALIZED,
              mongo_csfle_v1_status_get_error(status));
    lib = nullptr;
}

TEST_F(CsfleTest, BadLibHandleDestructionFails) {
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_destroy(nullptr, status));
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_status_get_error(status));

    int dummy;
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE,
              mongo_csfle_v1_destroy(reinterpret_cast<mongo_csfle_v1_lib*>(&dummy), status));
    ASSERT_EQ(MONGO_CSFLE_V1_ERROR_INVALID_LIB_HANDLE, mongo_csfle_v1_status_get_error(status));
}

TEST_F(CsfleTest, QueryAnalyzerCreateWithUninitializedLibFails) {
    ASSERT_EQ(MONGO_CSFLE_V1_SUCCESS, mongo_csfle_v1_destroy(lib, status));

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

static const std::string kSchema = R"({
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

DEATH_TEST_F(CsfleTest, LibDestructionWithExistingAnalyzerFails, "invariant") {
    auto analyzer = mongo_csfle_v1_query_analyzer_create(lib, status);
    ASSERT(analyzer);
    mongo_csfle_v1_destroy(lib, status);
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

TEST_F(CsfleTest, CheckMatchWorksWithDefaults) {
    checkAnalysisSuccess(
        R"({
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
    })",
        R"({ "hasEncryptionPlaceholders" : true, "schemaRequiresEncryption" : true, "result" : )"
        R"({ "find" : "test", "filter" : { "user.account" : { "$eq" : { "$binary" : )"
        R"("ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : )"
        R"("06" } } }, "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", )"
        R"("$type" : "04" } } } })");
}

TEST_F(CsfleTest, AnalyzeValidAggregateCommand) {
    std::string input =
        R"({
            "aggregate": "test",
            "pipeline": [ { "$match": { "user.account": "secret" } } ],
            "cursor": {},
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
            "$db": "test"
        })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({ "hasEncryptionPlaceholders" : true, "schemaRequiresEncryption" : true, "result" : )"
        R"({ "aggregate" : "test", "pipeline" : [ { "$match" : { "user.account" : { "$eq" : { "$binary" : )"
        R"("ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" } } } } ], )"
        R"("cursor" : {}, "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } } } })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

TEST_F(CsfleTest, AnalyzeValidFindAndModifyCommand) {
    std::string input =
        R"({
            "findAndModify": "test",
            "query": { "user.account": "secret" },
            "update": { "$set": { "ssn" : 4145 } },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
            "$db": "test"
        })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({ "hasEncryptionPlaceholders" : true, "schemaRequiresEncryption" : true, "result" : )"
        R"({ "findAndModify" : "test", "query" : { "user.account" : { "$eq" : { "$binary" : )"
        R"("ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" } } }, )"
        R"("update" : { "$set" : { "ssn" : { "$binary" : "ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ADEQAAAA", )"
        R"("$type" : "06" } } }, "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } } } })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

TEST_F(CsfleTest, AnalyzeValidCountCommand) {
    std::string input =
        R"({
            "count": "test",
            "query": { "user.account": "secret" },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
            "$db": "test"
        })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({ "hasEncryptionPlaceholders" : true, "schemaRequiresEncryption" : true, "result" : )"
        R"({ "count" : "test", "query" : { "user.account" : { "$eq" : { "$binary" : )"
        R"("ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" } } }, )"
        R"("lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } } } })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

TEST_F(CsfleTest, AnalyzeValidDistinctCommand) {
    std::string input =
        R"({
            "distinct": "test",
            "key": "user.pin",
            "query": { "user.account" : "secret" },
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
            "$db": "test"
        })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({
            "hasEncryptionPlaceholders" : true,
            "schemaRequiresEncryption" : true,
            "result" : {
                "distinct" : "test",
                "key": "user.pin",
                "query": {
                    "user.account" : {
                        "$eq" : {
                            "$binary" : "ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==",
                            "$type" : "06"
                        }
                    }
                },
                "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
            }
        })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

TEST_F(CsfleTest, AnalyzeValidUpdateCommand) {
    std::string input =
        R"({
            "update": "test",
            "updates": [
                { "q": { "ssn": 1234567890 }, "u": { "$set" : { "user.account" : "secret" } } }
            ],
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
           "$db": "test"
    })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({
            "hasEncryptionPlaceholders" : true,
            "schemaRequiresEncryption" : true,
            "result" : {
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
                }],
                "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
            }
        })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

TEST_F(CsfleTest, AnalyzeValidInsertCommand) {
    std::string input =
        R"({
            "insert": "test",
            "documents": [
                { "_id": 2, "ssn": 1234567890, "user": { "account": "secret" } }
            ],
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
           "$db": "test"
    })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({ "hasEncryptionPlaceholders" : true, "schemaRequiresEncryption" : true, "result" : )"
        R"({ "insert" : "test", "documents" : [ { "_id" : 2, "ssn" : { "$binary" : )"
        R"("ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ANIClkkA", "$type" : "06" }, )"
        R"("user" : { "account" : { "$binary" : )"
        R"("ADMAAAAQYQABAAAABWtpABAAAAAEkygsd5prR8+cTL7aAnMIgQJ2AAcAAABzZWNyZXQAAA==", "$type" : "06" })"
        R"( } } ], "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } } } })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

TEST_F(CsfleTest, AnalyzeValidDeleteCommand) {
    std::string input =
        R"({
            "delete": "test",
            "deletes": [
                { "q": { "ssn": 1234567890 }, "limit" : 1 }
            ],
            "jsonSchema": <SCHEMA>,
            "isRemoteSchema": false,
            "lsid": {
                "id": {
                    "$uuid": "32de9140-7ade-46bf-a72a-49442e4e93d7"
                }
            },
           "$db": "test"
    })";
    input.replace(input.find("<SCHEMA>"), 8, kSchema);
    std::string output =
        R"({
            "hasEncryptionPlaceholders" : true,
            "schemaRequiresEncryption" : true,
            "result" : {
                "delete" : "test",
                "deletes": [{
                    "q" : {
                        "ssn" : { "$eq" : { "$binary" : "ACwAAAAQYQABAAAABWtpABAAAAAEE2LQ7WGCR467iuvMU7kaoRB2ANIClkkA", "$type" : "06" } }
                    },
                    "limit" : 1
                }],
                "lsid" : { "id" : { "$binary" : "Mt6RQHreRr+nKklELk6T1w==", "$type" : "04" } }
            }
        })";
    checkAnalysisSuccess(input.c_str(), output.c_str());
}

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

    const auto result = ::mongo::unittest::Suite::run(std::vector<std::string>(), "", "", 1);

    // This is the standard exit path for Mongo processes. See the mongo::quickExit() declaration
    // for more information.
    mongo::quickExit(result);
}
