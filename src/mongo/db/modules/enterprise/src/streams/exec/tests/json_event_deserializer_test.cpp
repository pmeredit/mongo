/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/framework.h"
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

}  // namespace
}  // namespace streams
