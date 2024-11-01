/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#pragma once

#include <boost/optional.hpp>
#include <rdkafkacpp.h>
#include <string>

#include "streams/exec/stream_processor_feature_flags.h"

namespace streams {

// Constructs an error message given an Kafka error code and a wrapping message
std::string kafkaErrToString(const std::string& wrappingErrMsg, RdKafka::ErrorCode err);

// Get queued.max.messages.kbytes for a rdkafka partition, based on feature flags and total number
// of partitions.
boost::optional<int64_t> getRdKafkaQueuedMaxMessagesKBytes(
    const boost::optional<StreamProcessorFeatureFlags>& flags, int32_t numPartitions);

}  // namespace streams
