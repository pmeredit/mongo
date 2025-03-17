#include <string>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/util.h"

namespace streams {

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

TEST(UtilsTest, ParseJsonStrings) {
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

}  // namespace streams
