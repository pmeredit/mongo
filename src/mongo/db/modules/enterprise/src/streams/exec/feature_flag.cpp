/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <string>

#include "mongo/bson/bsontypes.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/feature_flag.h"
#include "streams/exec/config_gen.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/operator.h"

namespace streams {

const std::string kStreamsSppTierSP10 = "SP10";

mongo::Value FeatureFlagDefinition::getDefaultValue() const {
    auto it = tierDefaultValues.find(mongo::streams::gStreamsSppTier);
    if (it != tierDefaultValues.end()) {
        return it->second;
    }
    return defaultValue;
}

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
    mongo::Value(kDataMsgMaxByteSize * 10),
    {}};

const FeatureFlagDefinition FeatureFlags::kUseExecutionPlanFromCheckpoint{
    "useExecutionPlanFromCheckpoint",
    "Use the Execution plan stored in the checkpoint metadata.",
    mongo::Value(false),
    {}};

const FeatureFlagDefinition FeatureFlags::kMaxQueueSizeBytes{
    "maxQueueSizeBytes",
    "Maximum buffer size (in bytes) for a sink queue.",
    // 128 MB default
    mongo::Value::createIntOrLong(128L * 1024 * 1024),
    {}};

const FeatureFlagDefinition FeatureFlags::kKafkaEmitUseDeliveryCallback{
    "kafkaEmitUserDeliveryCallback",
    "If true, Kafka $emit uses a delivery callback to detect connection errors.",
    // Enabled by default.
    mongo::Value(true),
    {}};

const FeatureFlagDefinition FeatureFlags::kEnableSessionWindow{
    "enableSessionWindow",
    "If true, the $sessionWindow stage is enabled.",
    mongo::Value(false),
    {}};

const FeatureFlagDefinition FeatureFlags::kSourceBufferTotalSize{
    "sourceBufferTotalSize",
    "Specifies value for SourceBufferManager::Options::bufferTotalSize.",
    // 800 MB default
    mongo::Value::createIntOrLong(800L * 1024 * 1024),
    {{kStreamsSppTierSP10, mongo::Value::createIntOrLong(160L * 1024 * 1024)}}};

const FeatureFlagDefinition FeatureFlags::kSourceBufferPreallocationFraction{
    "sourceBufferPreallocationFraction",
    "Specifies value for SourceBufferManager::Options::bufferPreallocationFraction.",
    // 128 MB default
    mongo::Value(0.5),
    {}};

const FeatureFlagDefinition FeatureFlags::kSourceBufferMaxSize{
    "sourceBufferMaxSize",
    "Specifies value for SourceBufferManager::Options::maxSourceBufferSize.",
    // 160 MB default
    mongo::Value::createIntOrLong(160L * 1024 * 1024),
    {{kStreamsSppTierSP10, mongo::Value::createIntOrLong(32L * 1024 * 1024)}}};

const FeatureFlagDefinition FeatureFlags::kSourceBufferPageSize{
    "sourceBufferPageSize",
    "Specifies value for SourceBufferManager::Options::pageSize.",
    // 4 MB default
    mongo::Value::createIntOrLong(4L * 1024 * 1024),
    {}};

const FeatureFlagDefinition FeatureFlags::kMaxConcurrentCheckpoints{
    "maxConcurrentCheckpoints",
    "Ensures that only N checkpoints are in progress simultaneously. This is \
    shared across all stream processors running on this process ",
    mongo::Value(1),
    {}};

const FeatureFlagDefinition FeatureFlags::kTestOnlyStringType{
    "stringFlag",
    "testOnly flag to set string validation",
    mongo::Value{std::string("string")},
    {}};

const FeatureFlagDefinition FeatureFlags::kEnableExternalAPIOperator{
    "enableExternalAPIOperator",
    "If true, the $externalAPI operator is enabled.",
    mongo::Value(false),
    {}};

const FeatureFlagDefinition FeatureFlags::kExternalAPIRateLimitPerSecond{
    "externalAPIRateLimitPerSecond",
    "Specifies rate limit to be used by $externalAPI",
    mongo::Value::createIntOrLong(10L * 1000)};

const FeatureFlagDefinition FeatureFlags::kUseWatchToInitClusterChangestream{
    "useWatchToInitClusterChangestream",
    "If true, use a dummy session and watch command to get the operationTime to initialize a "
    "whole-cluster change stream $source",
    mongo::Value(true)};

const FeatureFlagDefinition FeatureFlags::kKafkaProduceTimeout{
    "kafkaProduceTimeout",
    "The produce timeout in milliseconds",
    mongo::Value::createIntOrLong(10L * 60 * 1000)};

mongo::stdx::unordered_map<std::string, FeatureFlagDefinition> featureFlagDefinitions = {
    {FeatureFlags::kCheckpointDurationInMs.name, FeatureFlags::kCheckpointDurationInMs},
    {FeatureFlags::kKafkaMaxPrefetchByteSize.name, FeatureFlags::kKafkaMaxPrefetchByteSize},
    {FeatureFlags::kUseExecutionPlanFromCheckpoint.name,
     FeatureFlags::kUseExecutionPlanFromCheckpoint},
    {FeatureFlags::kMaxQueueSizeBytes.name, FeatureFlags::kMaxQueueSizeBytes},
    {FeatureFlags::kKafkaEmitUseDeliveryCallback.name, FeatureFlags::kKafkaEmitUseDeliveryCallback},
    {FeatureFlags::kSourceBufferTotalSize.name, FeatureFlags::kSourceBufferTotalSize},
    {FeatureFlags::kSourceBufferPreallocationFraction.name,
     FeatureFlags::kSourceBufferPreallocationFraction},
    {FeatureFlags::kSourceBufferMaxSize.name, FeatureFlags::kSourceBufferMaxSize},
    {FeatureFlags::kEnableSessionWindow.name, FeatureFlags::kEnableSessionWindow},
    {FeatureFlags::kSourceBufferPageSize.name, FeatureFlags::kSourceBufferPageSize},
    {FeatureFlags::kEnableExternalAPIOperator.name, FeatureFlags::kEnableExternalAPIOperator},
    {FeatureFlags::kExternalAPIRateLimitPerSecond.name,
     FeatureFlags::kExternalAPIRateLimitPerSecond},
    {FeatureFlags::kTestOnlyStringType.name, FeatureFlags::kTestOnlyStringType},
    {FeatureFlags::kMaxConcurrentCheckpoints.name, FeatureFlags::kMaxConcurrentCheckpoints}};

bool FeatureFlags::validateFeatureFlag(const std::string& name, const mongo::Value& value) {
    auto definition = featureFlagDefinitions.find(name);
    if (definition != featureFlagDefinitions.end()) {
        switch (value.getType()) {
            case mongo::BSONType::NumberInt:
            case mongo::BSONType::NumberLong:
                return definition->second.defaultValue.getType() == mongo::BSONType::NumberInt ||
                    definition->second.defaultValue.getType() == mongo::BSONType::NumberLong;
            default:
                return value.getType() == definition->second.defaultValue.getType();
        }
    }
    // if feature flag is not found, validate does not care.
    // Feature will not be found for feature flags meant for other components.
    return true;
}

}  // namespace streams
