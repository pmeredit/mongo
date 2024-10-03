/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/kafka_utils.h"

#include <fmt/format.h>
#include <rdkafkacpp.h>
#include <string>

namespace streams {

std::string kafkaErrToString(const std::string& wrappingErrMsg, const RdKafka::ErrorCode err) {
    return fmt::format("{}: {} ({})", wrappingErrMsg, RdKafka::err2str(err), err);
}

}  // namespace streams
