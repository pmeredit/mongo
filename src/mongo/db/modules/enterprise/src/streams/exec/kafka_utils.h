#pragma once

#include <rdkafkacpp.h>
#include <string>

namespace streams {

// Constructs an error message given an Kafka error code and a wrapping message
std::string kafkaErrToString(const std::string& wrappingErrMsg, RdKafka::ErrorCode err);

}  // namespace streams
