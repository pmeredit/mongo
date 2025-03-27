/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <cstdint>
#include <string>
#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/bsontypes_util.h"
#include "mongo/bson/json.h"
#include "mongo/bson/oid.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/platform/decimal128.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/time_support.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

TEST(UtilTest, ShouldDetermineConfluentBrokerForOneServer) {
    auto isConfluentBrokerRslt = isConfluentBroker("is-a.confluent.cloud:broker");
    ASSERT_TRUE(isConfluentBrokerRslt);
}

TEST(UtilTest, ShouldDetermineConfluentBrokerForMultipleServers) {
    auto isConfluentBrokerRslt =
        isConfluentBroker("is-a.confluent.cloud:broker,is-also-a.confluent.cloud:broker");
    ASSERT_TRUE(isConfluentBrokerRslt);
}

TEST(UtilTest, ShouldDetermineIsNotAConfluentBroker) {
    auto isConfluentBrokerRslt =
        isConfluentBroker("is.not.confluent.broker:3000,is.not.confluent.broker:3001");
    ASSERT_FALSE(isConfluentBrokerRslt);
}

TEST(UtilTest, ParseJsonStrings) {
    auto doc = mongo::Document(mongo::fromjson(
        R"({"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}})"));
    auto expected = mongo::Document(
        mongo::fromjson(R"({"content": {"foo": "bar"}, "nested": { "data": {"abc": "xyz"}}})"));
    auto result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"([{"content": "{\"foo\": \"bar\"}"},{"nested": { "data": "{\"abc\": \"xyz\"}"}}])"));
    expected = mongo::Document(
        mongo::fromjson(R"([{"content": {"foo": "bar"}},{ "nested": { "data": {"abc": "xyz"}}}])"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"({
            "content": {
                "foo": "bar"
            },
            "nested": [
                {
                    "data": "{\"abc\": \"xyz\"}"
                },
                {
                    "data": [
                        {
                            "data": "{\"abc\": \"xyz\"}"
                        }
                    ]
                }
            ]
        })"));
    expected = mongo::Document(mongo::fromjson(
        R"({
            "content": {
                "foo": "bar"
            },
            "nested": [
                {
                    "data": {"abc": "xyz"}
                },
                {
                    "data": [
                        {
                            "data": {"abc": "xyz"}
                        }
                    ]
                }
            ]
        })"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"([{"content": " {\"foo\": \"bar\"}"},{"nested": { "data": "     {\"abc\": \"xyz\"}"}}])"));
    expected = mongo::Document(
        mongo::fromjson(R"([{"content": {"foo": "bar"}},{ "nested": { "data": {"abc": "xyz"}}}])"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(R"({"content": "{notjson"})"));
    expected = mongo::Document(mongo::fromjson(R"({"content": "{notjson"})"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"([{"content": " {\"foo\": \"bar\"}"},{"nested": { "data": "\n{\"abc\": \"xyz\"}"}}])"));
    expected = mongo::Document(
        mongo::fromjson(R"([{"content": {"foo": "bar"}},{ "nested": { "data": {"abc": "xyz"}}}])"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"([{"content": " {\"foo\": \"bar\"}"},{"nested": { "data": "\t{\"abc\": \"xyz\"}"}}])"));
    expected = mongo::Document(
        mongo::fromjson(R"([{"content": {"foo": "bar"}},{ "nested": { "data": {"abc": "xyz"}}}])"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"([{"content": " {\"foo\": \"bar\"}"},{"nested": { "data": "\r{\"abc\": \"xyz\"}"}}])"));
    expected = mongo::Document(
        mongo::fromjson(R"([{"content": {"foo": "bar"}},{ "nested": { "data": {"abc": "xyz"}}}])"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));


    doc = mongo::Document(mongo::fromjson(
        R"({"content": "[{\"foo\": \"bar\"}]", "nested": { "data": "[{\"abc\": \"xyz\"}]"}})"));
    expected = mongo::Document(
        mongo::fromjson(R"({"content": [{"foo": "bar"}], "nested": { "data": [{"abc": "xyz"}]}})"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"([{"content": "[{\"foo\": \"bar\"}]"},{"nested": { "data": "[{\"abc\": \"xyz\"}]"}}])"));
    expected = mongo::Document(mongo::fromjson(
        R"([{"content": [{"foo": "bar"}]},{ "nested": { "data": [{"abc": "xyz"}]}}])"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));

    doc = mongo::Document(mongo::fromjson(
        R"({
            "content": {
                "foo": "bar"
            },
            "nested": [
                {
                    "data": "[{\"abc\": \"xyz\"}, {\"abc\": \"xyz\"}]"
                },
                {
                    "data": [
                        {
                            "data": "[{\"abc\": {\"xyz\": \"1\"}}]"
                        }
                    ]
                }
            ]
        })"));
    expected = mongo::Document(mongo::fromjson(
        R"({
            "content": {
                "foo": "bar"
            },
            "nested": [
                {
                    "data": [{"abc": "xyz"}, {"abc": "xyz"}]
                },
                {
                    "data": [
                        {
                            "data": [{"abc": {"xyz": "1"}}]
                        }
                    ]
                }
            ]
        })"));
    result = convertJsonStringsToJson(doc);
    ASSERT_VALUE_EQ(mongo::Value(result), mongo::Value(expected));
}

TEST(UtilTest, ModifyForBasicJson) {
    const uint8_t binDataGeneralBytes[] = {0x81,
                                           0xfd,
                                           0x54,
                                           0x73,
                                           0x17,
                                           0x47,
                                           0x4c,
                                           0x9d,
                                           0x87,
                                           0x43,
                                           0xf1,
                                           0x06,
                                           0x42,
                                           0xb3,
                                           0xbb,
                                           0x99};

    const uint8_t uuidBytes[]{0x29,
                              0xab,
                              0x7e,
                              0x7c,
                              0x79,
                              0x3a,
                              0x46,
                              0x4b,
                              0x80,
                              0x9a,
                              0x8b,
                              0x54,
                              0x9a,
                              0xc5,
                              0x3b,
                              0x80};

    struct TestCase {
        const std::string description;
        const BSONObj inputBson;
        std::string expectedJson;
    };

    TestCase testCases[]{
        {
            .description = "General case",
            .inputBson = BSON(
                "_id" << mongo::OID("6717fcbba18c8a8f74b6d977") << "binary"
                      << mongo::BSONBinData(
                             binDataGeneralBytes, 16, mongo::BinDataType::BinDataGeneral)
                      << "date" << mongo::Date_t::fromMillisSinceEpoch(1729625275856) << "timestamp"
                      << mongo::Timestamp(1729625275, 1) << "decimal" << mongo::Decimal128{9.09}
                      << "uuid" << mongo::BSONBinData(uuidBytes, 16, mongo::BinDataType::newUUID)
                      << "posInf" << std::numeric_limits<double>::infinity() << "negInf"
                      << -std::numeric_limits<double>::infinity() << "regex"
                      << mongo::BSONRegEx("ab+c", "i") << "int" << 32 << "double" << 9.9 << "int64"
                      << int64_t{50} << "string"
                      << "foobar"),
            .expectedJson = R"({"_id":"6717fcbba18c8a8f74b6d977",
                    "binary":"gf1UcxdHTJ2HQ/EGQrO7mQ==",
                    "date":1729625275856,
                    "timestamp":1729625275000,)" +
                fmt::format(R"("decimal":"{}",)", mongo::Decimal128{9.09}.toString()) +
                R"("uuid":"29ab7e7c-793a-464b-809a-8b549ac53b80",
                "posInf":"Infinity",
                "negInf":"-Infinity",
                "regex":{"pattern":"ab+c","options":"i"},
                "int":32,"double":9.9,
                "int64":50,"string":"foobar"})",
        },
        {
            .description = "Decimal128 is NAN",
            .inputBson = BSON("decimal" << mongo::Decimal128{"absolutelynotanumber"}),
            .expectedJson = R"({"decimal":"NaN"})",
        },
        {
            .description = "Decimal128 is infinite",
            .inputBson = BSON("posInf" << mongo::Decimal128::kPositiveInfinity << "negInf"
                                       << mongo::Decimal128::kNegativeInfinity),
            .expectedJson = R"({"posInf":"Infinity", "negInf":"-Infinity"})",
        }};

    for (auto tc : testCases) {
        LOGV2_DEBUG(9997604, 1, "Running test case", "description"_attr = tc.description);
        auto convertedDoc = modifyDocumentForBasicJson(mongo::Document{tc.inputBson});
        auto actualJson = tojson(convertedDoc.toBson(), mongo::ExtendedRelaxedV2_0_0);

        tc.expectedJson.erase(
            std::remove_if(tc.expectedJson.begin(), tc.expectedJson.end(), ::isspace),
            tc.expectedJson.end());
        ASSERT_EQUALS(actualJson, tc.expectedJson);
    }
}

TEST(UtilTest, SerializeJson) {
    struct TestCase {
        const std::string description;
        const BSONObj inputBSON;
        const JsonStringFormat format;
        const std::string expectedJson;
    };

    TestCase testCases[]{
        {.description = "serializes to relaxed JSON format",
         .inputBSON = BSON("someDate" << mongo::Date_t::fromMillisSinceEpoch(1729625275856)),
         .format = JsonStringFormat::Relaxed,
         .expectedJson = "{\"someDate\":{\"$date\":\"2024-10-22T19:27:55.856Z\"}}"},
        {.description = "serializes to canonical JSON format",
         .inputBSON = BSON("someDate" << mongo::Date_t::fromMillisSinceEpoch(1729625275856)),
         .format = JsonStringFormat::Canonical,
         .expectedJson = "{\"someDate\":{\"$date\":{\"$numberLong\":\"1729625275856\"}}}"},
        {.description = "serializes to basic JSON format",
         .inputBSON = BSON("someDate" << mongo::Date_t::fromMillisSinceEpoch(1729625275856)),
         .format = JsonStringFormat::Basic,
         .expectedJson = "{\"someDate\":1729625275856}"},
    };

    for (const auto& tc : testCases) {
        LOGV2_DEBUG(9997605, 1, "Running test case", "description"_attr = tc.description);
        ASSERT_EQ(serializeJson(tc.inputBSON, tc.format), tc.expectedJson);
    }
}

TEST(UtilTest, ParseAndDeserializeJsonResponse) {
    struct TestCase {
        std::string description;
        std::string input;
        bool parseFields;
        Value expectedValue;
    };

    std::vector<TestCase> testCases{
        {"Should transform an object",
         R"({"field1": "value1", "field2": {"field3": "value3"}})",
         {},
         Value(BSON("field1" << "value1"
                             << "field2" << BSON("field3" << "value3")))},
        {"Should transform array",
         R"([{"field1": "value1"}, {"field1": "value2"}])",
         false,
         Value(BSON_ARRAY(BSON("field1" << "value1") << BSON("field1" << "value2")))},
        {
            "Should parse fields in object when flagged to do so",
            R"({"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}})",
            true,
            Value(BSON("content" << BSON("foo" << "bar") << "nested"
                                 << BSON("data" << BSON("abc" << "xyz")))),
        },
        {
            "Should leave fields unparsed in object when not flagged to do so",
            R"({"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}})",
            false,
            Value(BSON("content" << "{\"foo\": \"bar\"}"
                                 << "nested" << BSON("data" << "{\"abc\": \"xyz\"}"))),
        },
        {
            "Should parse fields in array when flagged to do so",
            R"([{"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}}, {"content": "{\"foo\": \"bar2\"}", "nested": { "data": "{\"abc\": \"xyz2\"}"}}])",
            true,
            Value(BSON_ARRAY(BSON("content" << BSON("foo" << "bar") << "nested"
                                            << BSON("data" << BSON("abc" << "xyz")))
                             << BSON("content" << BSON("foo" << "bar2") << "nested"
                                               << BSON("data" << BSON("abc" << "xyz2"))))),
        },
        {
            "Should leave fields in array unparsed when not flagged to do so",
            R"([{"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}}, {"content": "{\"foo\": \"bar2\"}", "nested": { "data": "{\"abc\": \"xyz2\"}"}}])",
            false,
            Value(BSON_ARRAY(BSON("content" << "{\"foo\": \"bar\"}"
                                            << "nested" << BSON("data" << "{\"abc\": \"xyz\"}"))
                             << BSON("content" << "{\"foo\": \"bar2\"}"
                                               << "nested"
                                               << BSON("data" << "{\"abc\": \"xyz2\"}")))),
        }};

    for (const TestCase& tc : testCases) {
        LOGV2_DEBUG(9929701, 1, "Running test case", "description"_attr = tc.description);
        auto value = parseAndDeserializeJsonResponse(tc.input, tc.parseFields);
        ASSERT_EQ(value.toString(), tc.expectedValue.toString());
    }
}

}  // namespace streams
