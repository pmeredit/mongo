/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <string>
#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/unittest/unittest.h"
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
         Value(BSON("field1"
                    << "value1"
                    << "field2"
                    << BSON("field3"
                            << "value3")))},
        {"Should transform array",
         R"([{"field1": "value1"}, {"field1": "value2"}])",
         false,
         Value(BSON_ARRAY(BSON("field1"
                               << "value1")
                          << BSON("field1"
                                  << "value2")))},
        {
            "Should parse fields in object when flagged to do so",
            R"({"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}})",
            true,
            Value(BSON("content" << BSON("foo"
                                         << "bar")
                                 << "nested"
                                 << BSON("data" << BSON("abc"
                                                        << "xyz")))),
        },
        {
            "Should leave fields unparsed in object when not flagged to do so",
            R"({"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}})",
            false,
            Value(BSON("content"
                       << "{\"foo\": \"bar\"}"
                       << "nested"
                       << BSON("data"
                               << "{\"abc\": \"xyz\"}"))),
        },
        {
            "Should parse fields in array when flagged to do so",
            R"([{"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}}, {"content": "{\"foo\": \"bar2\"}", "nested": { "data": "{\"abc\": \"xyz2\"}"}}])",
            true,
            Value(BSON_ARRAY(BSON("content" << BSON("foo"
                                                    << "bar")
                                            << "nested"
                                            << BSON("data" << BSON("abc"
                                                                   << "xyz")))
                             << BSON("content" << BSON("foo"
                                                       << "bar2")
                                               << "nested"
                                               << BSON("data" << BSON("abc"
                                                                      << "xyz2"))))),
        },
        {
            "Should leave fields in array unparsed when not flagged to do so",
            R"([{"content": "{\"foo\": \"bar\"}", "nested": { "data": "{\"abc\": \"xyz\"}"}}, {"content": "{\"foo\": \"bar2\"}", "nested": { "data": "{\"abc\": \"xyz2\"}"}}])",
            false,
            Value(BSON_ARRAY(BSON("content"
                                  << "{\"foo\": \"bar\"}"
                                  << "nested"
                                  << BSON("data"
                                          << "{\"abc\": \"xyz\"}"))
                             << BSON("content"
                                     << "{\"foo\": \"bar2\"}"
                                     << "nested"
                                     << BSON("data"
                                             << "{\"abc\": \"xyz2\"}")))),
        }};

    for (const TestCase& tc : testCases) {
        LOGV2_DEBUG(9929701, 1, "Running test case", "description"_attr = tc.description);
        auto value = parseAndDeserializeJsonResponse(tc.input, tc.parseFields);
        ASSERT_EQ(value.toString(), tc.expectedValue.toString());
    }
}

}  // namespace streams
