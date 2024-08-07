/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/feature_flag.h"
#include "mongo/bson/bsontypes.h"
#include "streams/exec/operator.h"

namespace streams {

boost::optional<int64_t> FeatureFlagValue::getInt() const {
    boost::optional<int64_t> returnValue;
    if (_value.getType() != mongo::BSONType::NumberInt &&
        _value.getType() != mongo::BSONType::NumberLong) {
        return returnValue;
    }
    returnValue.emplace(_value.coerceToLong());
    return returnValue;
}

boost::optional<bool> FeatureFlagValue::getBool() const {
    boost::optional<bool> returnValue;
    if (_value.getType() != mongo::BSONType::Bool) {
        return returnValue;
    }
    returnValue.emplace(_value.getBool());
    return returnValue;
}

boost::optional<std::string> FeatureFlagValue::getString() const {
    boost::optional<std::string> returnValue;
    if (_value.getType() != mongo::BSONType::String) {
        return returnValue;
    }
    returnValue.emplace(_value.getString());
    return returnValue;
}

boost::optional<double> FeatureFlagValue::getDouble() const {
    boost::optional<double> returnValue;
    if (_value.getType() != mongo::BSONType::NumberDouble) {
        return returnValue;
    }
    returnValue.emplace(_value.getDouble());
    return returnValue;
}

const FeatureFlagDefinition FeatureFlags::kCheckpointDurationInMs{
    "checkpointDuration", "checkpoint Duration in ms", mongo::Value(60 * 60 * 1000)};

const FeatureFlagDefinition FeatureFlags::kKafkaMaxPrefetchByteSize{
    "kafkaMaxPrefetchByteSize",
    "Maximum buffer size (in bytes) for each Kafka $source partition.",
    mongo::Value(kDataMsgMaxByteSize * 10)};

const FeatureFlagDefinition FeatureFlags::kUseExecutionPlanFromCheckpoint{
    "useExecutionPlanFromCheckpoint",
    "Use the Execution plan stored in the checkpoint metadata.",
    mongo::Value(false)};

const FeatureFlagDefinition FeatureFlags::kMaxQueueSizeBytes{
    "maxQueueSizeBytes",
    "Maximum buffer size (in bytes) for a sink queue.",
    // 128 MB default
    mongo::Value::createIntOrLong(128 * 1024 * 1024)};

const FeatureFlagDefinition FeatureFlags::kKafkaEmitUseDeliveryCallback{
    "kafkaEmitUserDeliveryCallback",
    "If true, Kafka $emit uses a delivery callback to detect connection errors.",
    // Enabled by default.
    mongo::Value(true)};

const FeatureFlagDefinition FeatureFlags::kEnableSessionWindow{
    "enableSessionWindow", "If true, the $sessionWindow stage is enabled.", mongo::Value(false)};

}  // namespace streams
