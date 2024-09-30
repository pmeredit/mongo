#pragma once

#include <fmt/format.h>
#include <rdkafkacpp.h>
#include <string>

namespace streams {

// Constructs an error message given an Kafka error code and a wrapping message
inline std::string kafkaErrToString(const std::string& wrappingErrMsg,
                                    const RdKafka::ErrorCode err) {
    return fmt::format("{}: {} ({})", wrappingErrMsg, RdKafka::err2str(err), err);
}

}  // namespace streams
