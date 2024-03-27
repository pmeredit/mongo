/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/stream_processor_feature_flags.h"
#include "mongo/bson/bsontypes.h"
#include "streams/exec/feature_flag.h"

namespace streams {

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

StreamProcessorFeatureFlags StreamProcessorFeatureFlags::parseFeatureFlags(
    const mongo::BSONObj& bsonObj) {
    mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags;
    mongo::Document doc(bsonObj);
    auto it = doc.fieldIterator();
    while (it.more()) {
        auto fld = it.next();
        featureFlags[fld.first.toString()] = fld.second;
    }
    StreamProcessorFeatureFlags spff{featureFlags,
                                     std::chrono::time_point<std::chrono::system_clock>::min()};
    return spff;
}

FeatureFlagValue StreamProcessorFeatureFlags::getFeatureFlagValue(
    const FeatureFlagDefinition& featureFlag) const {
    auto it = _featureFlags.find(featureFlag.name);
    if (it != _featureFlags.end()) {
        return it->second;
    }
    return featureFlag.defaultValue;
}

}  // namespace streams
