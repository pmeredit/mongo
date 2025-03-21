/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <chrono>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "streams/exec/feature_flag.h"

namespace streams {

struct LoggingContext;

// Class holding all feature flags for a stream processor.
class StreamProcessorFeatureFlags {
public:
    StreamProcessorFeatureFlags(mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags,
                                std::chrono::time_point<std::chrono::system_clock> updateTime);

    // Update feature flags if they are different.
    void updateFeatureFlags(StreamProcessorFeatureFlags);

    mongo::stdx::unordered_map<std::string, mongo::Value> testOnlyGetFeatureFlags() {
        return _featureFlags;
    }
    static StreamProcessorFeatureFlags parseFeatureFlags(const mongo::BSONObj& bsonObj,
                                                         const LoggingContext& context);

    // gets feature flag value for feature flag.
    FeatureFlagValue getFeatureFlagValue(const FeatureFlagDefinition& featureFlag) const;

    // checks if the feature flag has overridden value.
    bool isOverridden(const FeatureFlagDefinition& ff) const {
        return _featureFlags.find(ff.name) != _featureFlags.end();
    }

private:
    mongo::stdx::unordered_map<std::string, mongo::Value> _featureFlags;
    std::chrono::time_point<std::chrono::system_clock> _featureFlagsUpdatedTime{
        std::chrono::time_point<std::chrono::system_clock>::min()};
};

int64_t getMaxQueueSizeBytes(boost::optional<StreamProcessorFeatureFlags> featureFlags);
int64_t getKafkaProduceTimeoutMs(boost::optional<StreamProcessorFeatureFlags> featureFlags);
bool shouldUseWatchToInitClusterChangestream(
    boost::optional<StreamProcessorFeatureFlags> featureFlags);
boost::optional<mongo::Seconds> getChangestreamSourceStalenessMonitorPeriod(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags);
boost::optional<int64_t> getKafkaTotalQueuedBytes(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags);
bool getOldStreamMetaEnabled(const boost::optional<StreamProcessorFeatureFlags>& featureFlags);
bool enableMetadataRefreshInterval(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags);
int64_t getHttpsRateLimitPerSec(boost::optional<StreamProcessorFeatureFlags> featureFlags);
int64_t getExternalFunctionRateLimitPerSec(
    boost::optional<StreamProcessorFeatureFlags> featureFlags);
boost::optional<int64_t> getKafkaMessageMaxBytes(
    const boost::optional<StreamProcessorFeatureFlags>& featureFlags);

}  // namespace streams
