/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/kafka_utils.h"

#include <fmt/format.h>
#include <functional>
#include <rdkafkacpp.h>
#include <string>

#include "mongo/logv2/log.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/util/units.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

mongo::stdx::unordered_set<std::string> specialHandledConfigurations = {"group.id",
                                                                        "auto.offset.reset"};

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

void setKafkaConnectionConfigurations(
    mongo::BSONObj configurations,
    std::function<void(const std::string& field, const std::string& value)> setConf,
    const mongo::stdx::unordered_set<std::string>& allowedConfigurations) {
    for (const auto& config : configurations) {
        if (!allowedConfigurations.contains(config.fieldName())) {
            // Avoid logging a warning for configurations that are not in the allowedConfigurations
            // list because they have special handling i.e. group.id and auto.offset.reset
            if (!specialHandledConfigurations.contains(config.fieldName())) {
                LOGV2_WARNING(9863000,
                              "A kafka configuration defined in the connection is not in the "
                              "allowedConfigurations list",
                              "config"_attr = config.fieldName());
            }
            continue;
        }
        auto fieldValue = (config.type() == mongo::String)
            ? config.String()
            : config.toString(false /* includeFieldName */);
        setConf(config.fieldName(), fieldValue);
    }
}

}  // namespace streams
