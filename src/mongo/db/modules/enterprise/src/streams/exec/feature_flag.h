/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once
#include <string>

#include "mongo/db/exec/document_value/value.h"
#include "mongo/stdx/unordered_map.h"


namespace streams {

// FeatureFlagDefinition class used to fetch a feature flag value.
struct FeatureFlagDefinition {
    std::string name;
    std::string description;
    mongo::Value defaultValue;
    mongo::stdx::unordered_map<std::string, mongo::Value> tierDefaultValues;
    mongo::Value getDefaultValue() const;
};

// FeatureFlagValue is wrapper to mongo to return only expected type values or default value.
class FeatureFlagValue {
public:
    FeatureFlagValue(const mongo::Value& val) : _value(val) {}
    boost::optional<int64_t> getInt() const;
    boost::optional<bool> getBool() const;
    boost::optional<double> getDouble() const;
    boost::optional<std::string> getString() const;
    boost::optional<std::vector<std::string>> getVectorString() const;

private:
    mongo::Value _value;
};

// Empty class to limit scope of global featureFlagDefinitions.
class FeatureFlags {
public:
    static bool validateFeatureFlag(const std::string& name, const mongo::Value& value);
    static const FeatureFlagDefinition kCheckpointDurationInMs;
    // TODO: Remove this feature flag after the next prod deploy.
    static const FeatureFlagDefinition kKafkaMaxPrefetchByteSize;
    static const FeatureFlagDefinition kUseExecutionPlanFromCheckpoint;
    static const FeatureFlagDefinition kMaxSinkQueueSizeBytes;
    static const FeatureFlagDefinition kMaxSinkQueueSize;
    static const FeatureFlagDefinition kKafkaEmitUseDeliveryCallback;
    static const FeatureFlagDefinition kEnableSessionWindow;
    static const FeatureFlagDefinition kSourceBufferTotalSize;
    static const FeatureFlagDefinition kSourceBufferPreallocationFraction;
    static const FeatureFlagDefinition kSourceBufferMaxSize;
    static const FeatureFlagDefinition kSourceBufferMinPageSize;
    static const FeatureFlagDefinition kSourceBufferMaxPageSize;
    static const FeatureFlagDefinition kTestOnlyStringType;
    static const FeatureFlagDefinition kEnableExternalAPIOperator;
    static const FeatureFlagDefinition kExternalAPIRateLimitPerSecond;
    static const FeatureFlagDefinition kKafkaProduceTimeout;
    static const FeatureFlagDefinition kUseWatchToInitClusterChangestream;
    static const FeatureFlagDefinition kChangestreamSourceStalenessMonitorPeriod;
    static const FeatureFlagDefinition kMaxConcurrentCheckpoints;
    static const FeatureFlagDefinition kCidrDenyList;
};

}  // namespace streams
