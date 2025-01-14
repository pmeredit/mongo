/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include <bsoncxx/exception/exception.hpp>
#include <exception>

#include "mongo/bson/bsonobj.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/json_event_deserializer.h"

namespace streams {
namespace {

using namespace mongo;

TEST(JsonEventDeserializerTest, Unicode) {
    JsonEventDeserializer deserializer;
    // JSON strings can contain UTF-16 escape sequences, according to
    // https://datatracker.ietf.org/doc/html/rfc8259#section-7.
    std::string stringWithUTF16Escapes =
        R"({ "comment": "\u5b59\u7b11\u5ddd\ud83d\udc74\u5112\ud83e\udd86\u96a8\u548c" })";
    auto bson =
        deserializer.deserialize(stringWithUTF16Escapes.c_str(), stringWithUTF16Escapes.length());
    ASSERT_EQ(std::string{"孙笑川👴儒🦆隨和"}, bson.getStringField("comment").toString());
}

TEST(JsonEventDeserializerTest, Empty) {
    JsonEventDeserializer deserializer;
    std::string empty = "";
    auto bson = deserializer.deserialize(empty.c_str(), empty.length());
    ASSERT_BSONOBJ_EQ(BSONObj::kEmptyObject, bson);
    ASSERT(bson.isEmpty());
}

TEST(JsonEventDeserializerTest, Whitespace) {
    JsonEventDeserializer deserializer;
    std::string empty = " ";
    ASSERT_THROWS_WHAT(deserializer.deserialize(empty.c_str(), empty.length()),
                       std::exception,
                       "Incomplete JSON: could not parse JSON document");
}

TEST(JsonEventDeserializerTest, ValidWithTrailingWhitespace) {
    JsonEventDeserializer deserializer;
    std::string val = R"({ "a": 1 } )";
    auto bson = deserializer.deserialize(val.c_str(), val.length());
    ASSERT(!bson.isEmpty());
    ASSERT(bson.nFields() == 1);
    ASSERT(bson.getField("a").numberInt() == 1);
}

TEST(JsonEventDeserializerTest, ValidWithPrecedingWhitespace) {
    JsonEventDeserializer deserializer;
    std::string val = R"( { "a": 1 })";
    auto bson = deserializer.deserialize(val.c_str(), val.length());
    ASSERT(!bson.isEmpty());
    ASSERT(bson.nFields() == 1);
    ASSERT(bson.getField("a").numberInt() == 1);
}

TEST(JsonEventDeserializerTest, ValidWithSurroundingWhitespace) {
    JsonEventDeserializer deserializer;
    std::string val = R"( { "a": 1 } )";
    auto bson = deserializer.deserialize(val.c_str(), val.length());
    ASSERT(!bson.isEmpty());
    ASSERT(bson.nFields() == 1);
    ASSERT(bson.getField("a").numberInt() == 1);
}

TEST(JsonEventDeserializerTest, ConfluentJsonParsing) {
    auto makePrefix = []() {
        std::vector<char> bytes;
        bytes.push_back(0);
        // Append the Schema ID.
        bytes.push_back(1);
        bytes.push_back(2);
        bytes.push_back(3);
        bytes.push_back(4);
        return bytes;
    };

    auto innerTest = [&](const std::string& in, BSONObj expected) {
        JsonEventDeserializer deserializer;
        auto bytes = makePrefix();
        for (size_t i = 0; i < in.length(); ++i) {
            bytes.push_back(in.c_str()[i]);
        }
        auto bson = deserializer.deserialize(bytes.data(), bytes.size());
        ASSERT_BSONOBJ_EQ(bson, expected);
    };

    // Typical case.
    innerTest(R"({"a": 1})", BSON("a" << 1));
    // Prefix followed by empty object.
    innerTest(R"({})", BSONObj{});

    // Prefix with no data.
    auto expectFail = [](auto func) {
        try {
            func();
            ASSERT(false);
        } catch (const bsoncxx::exception&) {
        }
    };
    {
        JsonEventDeserializer deserializer;
        auto bytes = makePrefix();
        expectFail([&]() { deserializer.deserialize(bytes.data(), bytes.size()); });
    }
    // Bad prefix (only 3 bytes).
    {
        JsonEventDeserializer deserializer;
        std::vector<char> bytes;
        bytes.push_back(0);
        // Append the Schema ID.
        bytes.push_back(1);
        bytes.push_back(2);
        expectFail([&]() { deserializer.deserialize(bytes.data(), bytes.size()); });
    }
    // Bad prefix (magic byte != 0);
    {
        JsonEventDeserializer deserializer;
        auto bytes = makePrefix();
        bytes[0] = 1;
        expectFail([&]() { deserializer.deserialize(bytes.data(), bytes.size()); });
    }
}

}  // namespace
}  // namespace streams
