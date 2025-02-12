/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/stream_processor_feature_flags.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/util/str.h"
#include "streams/exec/feature_flag.h"

namespace streams {

StreamProcessorFeatureFlags::StreamProcessorFeatureFlags(
    mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags,
    std::chrono::time_point<std::chrono::system_clock> updateTime)
    : _featureFlags(std::move(featureFlags)), _featureFlagsUpdatedTime(updateTime) {}

void StreamProcessorFeatureFlags::updateFeatureFlags(StreamProcessorFeatureFlags featureFlags) {
    if (featureFlags._featureFlagsUpdatedTime >= _featureFlagsUpdatedTime) {
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
        uassert(9273402,
                mongo::str::stream()
                    << "feature flag " << fld.first.toString() << " type mismatched",
                FeatureFlags::validateFeatureFlag(fld.first.toString(), fld.second));
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
    return featureFlag.getDefaultValue();
}

int64_t getMaxQueueSizeBytes(boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(8748200, "Feature flags should be set", featureFlags);
    return *featureFlags->getFeatureFlagValue(FeatureFlags::kMaxQueueSizeBytes).getInt();
}

int64_t getKafkaProduceTimeoutMs(boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(8748203, "Feature flags should be set", featureFlags);
    auto val = featureFlags->getFeatureFlagValue(FeatureFlags::kKafkaProduceTimeout).getInt();
    if (val) {
        return *val;
    }
    return FeatureFlags::kKafkaProduceTimeout.defaultValue.getInt();
}

bool shouldUseWatchToInitClusterChangestream(
    boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(8748201, "Feature flags should be set", featureFlags);
    return *featureFlags->getFeatureFlagValue(FeatureFlags::kUseWatchToInitClusterChangestream)
                .getBool();
}

boost::optional<mongo::Seconds> getChangestreamSourceStalenessMonitorPeriod(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags) {
    tassert(9588811, "Feature flags should be set", featureFlags);

    boost::optional<mongo::Seconds> ret;
    auto val =
        featureFlags->getFeatureFlagValue(FeatureFlags::kChangestreamSourceStalenessMonitorPeriod)
            .getInt();
    if (val && *val > 0) {
        ret = mongo::Seconds{*val};
    }
    return ret;
}

boost::optional<int64_t> getKafkaTotalQueuedBytes(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags) {
    tassert(9588812, "Feature flags should be set", featureFlags);
    auto val = featureFlags->getFeatureFlagValue(FeatureFlags::kKafkaTotalQueuedBytes).getValue();
    if (val.missing()) {
        return boost::none;
    }
    return val.coerceToLong();
}

bool getOldStreamMetaEnabled(const boost::optional<StreamProcessorFeatureFlags>& featureFlags) {
    return *featureFlags->getFeatureFlagValue(FeatureFlags::kOldStreamMeta).getBool();
}

bool enableMetadataRefreshInterval(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags) {
    return *featureFlags->getFeatureFlagValue(FeatureFlags::kEnableMetadataRefreshInterval)
                .getBool();
}

int64_t getHttpsRateLimitPerSec(boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(9503701, "Feature flags should be set", featureFlags);
    auto val = featureFlags->getFeatureFlagValue(FeatureFlags::kHttpsRateLimitPerSecond).getInt();
    return *val;
}

int64_t getExternalFunctionRateLimitPerSec(
    boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(9929408, "Feature flags should be set", featureFlags);
    auto val = featureFlags->getFeatureFlagValue(FeatureFlags::kExternalFunctionRateLimitPerSecond)
                   .getInt();
    return *val;
}

boost::optional<int64_t> getKafkaMessageMaxBytes(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags) {
    auto val =
        featureFlags->getFeatureFlagValue(FeatureFlags::kKafkaEmitMessageMaxBytes).getValue();
    if (val.missing()) {
        return boost::none;
    }

    return val.coerceToLong();
}

}  // namespace streams
