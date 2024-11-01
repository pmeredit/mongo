/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/kafka_utils.h"

#include <fmt/format.h>
#include <rdkafkacpp.h>
#include <string>

#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/util/units.h"

namespace streams {

std::string kafkaErrToString(const std::string& wrappingErrMsg, const RdKafka::ErrorCode err) {
    return fmt::format("{}: {} ({})", wrappingErrMsg, RdKafka::err2str(err), err);
}

boost::optional<int64_t> getRdKafkaQueuedMaxMessagesKBytes(
    const boost::optional<StreamProcessorFeatureFlags>& flags, int32_t numPartitions) {
    boost::optional<int64_t> total = getKafkaTotalQueuedBytes(flags);
    if (!total) {
        return boost::none;
    }

    // Maximum of rdkafka's default setting of 64MB.
    constexpr int64_t rdkafkaDefaultSize = 64_MiB;
    int64_t bytes = std::min(rdkafkaDefaultSize, *total / numPartitions);
    // Minimum of 128kB. Convert to KB for rdkafka setting.
    constexpr int64_t rdkafkaMinQueueSize = 128_KiB;
    return std::max(rdkafkaMinQueueSize, bytes) / 1024;
}

}  // namespace streams
