#pragma once

#include "mongo/base/string_data.h"

using namespace mongo::literals;

namespace streams {

constexpr auto kTestKafkaConnectionName = "__testKafka"_sd;
constexpr auto kTestLogConnectionName = "__testLog"_sd;
constexpr auto kTestMemoryConnectionName = "__testMemory"_sd;

};  // namespace streams
