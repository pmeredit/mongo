#pragma once

#include "mongo/base/string_data.h"

using namespace mongo::literals;

namespace streams {

constexpr auto kTestKafkaToken = "__testKafka"_sd;
constexpr auto kTestTypeLogToken = "__testLog"_sd;
constexpr auto kTestTypeMemoryToken = "__testMemory"_sd;

};  // namespace streams
