/**
 *    Copyright (C) 2024-present MongoDB, Inc.
 */

#include "mongo/unittest/unittest.h"
#include "streams/exec/kafka_consumer_operator.h"


namespace streams {

using namespace mongo;

class KafkaKeyDeserializationTest : public unittest::Test {
public:
    static const std::vector<mongo::KafkaKeyFormatEnum> keyFormats;

    std::variant<std::vector<std::uint8_t>, std::string, mongo::BSONObj, std::int32_t, std::int64_t>
    deserializeKafkaKey(std::vector<std::uint8_t> key, mongo::KafkaKeyFormatEnum keyFormat) {
        return KafkaConsumerOperator::deserializeKafkaKey(std::move(key), keyFormat);
    }
};

const std::vector<mongo::KafkaKeyFormatEnum> KafkaKeyDeserializationTest::keyFormats{
    mongo::KafkaKeyFormatEnum::BinData,
    mongo::KafkaKeyFormatEnum::String,
    mongo::KafkaKeyFormatEnum::Json,
    mongo::KafkaKeyFormatEnum::Int,
    mongo::KafkaKeyFormatEnum::Long,
};

TEST_F(KafkaKeyDeserializationTest, BinData) {
    std::vector<std::uint8_t> key{0, 1, 2, 3};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::BinData);
    ASSERT(holds_alternative<std::vector<std::uint8_t>>(deserializedKey));
    auto binDataKey = std::get<std::vector<std::uint8_t>>(deserializedKey);
    ASSERT_EQ(binDataKey, key);
}

TEST_F(KafkaKeyDeserializationTest, String) {
    std::vector<std::uint8_t> key{'a', 'b', 'c', 'd'};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::String);
    ASSERT(holds_alternative<std::string>(deserializedKey));
    auto stringKey = std::get<std::string>(deserializedKey);
    ASSERT_EQ(stringKey, "abcd");
}

TEST_F(KafkaKeyDeserializationTest, StringNonUTF) {
    std::vector<std::uint8_t> key{0xc3, 0x28};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::String);
    ASSERT(holds_alternative<std::vector<std::uint8_t>>(deserializedKey));
    auto binDataKey = std::get<std::vector<std::uint8_t>>(deserializedKey);
    ASSERT_EQ(binDataKey, key);
}

TEST_F(KafkaKeyDeserializationTest, Json) {
    std::vector<std::uint8_t> key{'{', '"', 'a', '"', ':', '1', '}'};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::Json);
    ASSERT(holds_alternative<BSONObj>(deserializedKey));
    auto objKey = std::get<BSONObj>(deserializedKey);
    ASSERT_BSONOBJ_EQ(objKey, BSON("a" << 1));
}

TEST_F(KafkaKeyDeserializationTest, JsonInvalid) {
    std::vector<std::uint8_t> key{'{', '"', 'a', '"', '1', '}'};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::Json);
    ASSERT(holds_alternative<std::vector<std::uint8_t>>(deserializedKey));
    auto binDataKey = std::get<std::vector<std::uint8_t>>(deserializedKey);
    ASSERT_EQ(binDataKey, key);
}

TEST_F(KafkaKeyDeserializationTest, Int) {
    std::vector<std::uint8_t> key{0x0, 0x0, 0x6, 0xc1};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::Int);
    ASSERT(holds_alternative<int32_t>(deserializedKey));
    auto intKey = std::get<int32_t>(deserializedKey);
    ASSERT_EQ(intKey, 1729);
}

TEST_F(KafkaKeyDeserializationTest, IntIncorrectLength) {
    std::vector<std::uint8_t> key{0x0};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::Int);
    ASSERT(holds_alternative<std::vector<std::uint8_t>>(deserializedKey));
    auto binDataKey = std::get<std::vector<std::uint8_t>>(deserializedKey);
    ASSERT_EQ(binDataKey, key);
}

TEST_F(KafkaKeyDeserializationTest, Long) {
    std::vector<std::uint8_t> key{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0xc1};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::Long);
    ASSERT(holds_alternative<int64_t>(deserializedKey));
    auto intKey = std::get<int64_t>(deserializedKey);
    ASSERT_EQ(intKey, 1729);
}

TEST_F(KafkaKeyDeserializationTest, LongIncorrectLength) {
    std::vector<std::uint8_t> key{0x0};
    auto deserializedKey = deserializeKafkaKey(key, mongo::KafkaKeyFormatEnum::Long);
    ASSERT(holds_alternative<std::vector<std::uint8_t>>(deserializedKey));
    auto binDataKey = std::get<std::vector<std::uint8_t>>(deserializedKey);
    ASSERT_EQ(binDataKey, key);
}

}  // namespace streams
