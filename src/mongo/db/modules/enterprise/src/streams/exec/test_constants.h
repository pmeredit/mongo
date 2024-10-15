/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/string_data.h"

using namespace mongo::literals;

namespace streams {

constexpr auto kTestKafkaConnectionName = "__testKafka"_sd;
constexpr auto kTestLogConnectionName = "__testLog"_sd;
constexpr auto kTestMemoryConnectionName = "__testMemory"_sd;
constexpr auto kNoOpSinkOperatorConnectionName = "__noopSink"_sd;
constexpr auto kTestWebAPIConnectionName = "__testWebAPI"_sd;

};  // namespace streams
