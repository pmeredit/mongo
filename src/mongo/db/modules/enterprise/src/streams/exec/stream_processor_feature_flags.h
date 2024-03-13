#pragma once

#include <chrono>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"

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
    static StreamProcessorFeatureFlags parseFeatureFlags(const mongo::BSONObj& bsonObj);

private:
    mongo::stdx::unordered_map<std::string, mongo::Value> _featureFlags;
    std::chrono::time_point<std::chrono::system_clock> _featureFlagsUpdatedTime{
        std::chrono::time_point<std::chrono::system_clock>::min()};
};
