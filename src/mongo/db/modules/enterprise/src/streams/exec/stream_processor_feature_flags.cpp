/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/stream_processor_feature_flags.h"

StreamProcessorFeatureFlags::StreamProcessorFeatureFlags(
    mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags,
    std::chrono::time_point<std::chrono::system_clock> updateTime)
    : _featureFlags(std::move(featureFlags)), _featureFlagsUpdatedTime(updateTime) {}

void StreamProcessorFeatureFlags::updateFeatureFlags(StreamProcessorFeatureFlags featureFlags) {
    if (featureFlags._featureFlagsUpdatedTime > _featureFlagsUpdatedTime) {
        _featureFlags = std::move(featureFlags._featureFlags);
        _featureFlagsUpdatedTime = featureFlags._featureFlagsUpdatedTime;
    }
}
