#pragma once
#include <chrono>

#include "mongo/bson/bsonobj.h"
#include "mongo/platform/mutex.h"
#include "streams/exec/stream_processor_feature_flags.h"

namespace streams {

// Singleton Instance holding all feature flags for this tenant.
/* Sample BSON document parsed by this class.
 { feature_flag_a :
    {
        value: "value_for_tenant2",
        streamProcessors: { sp1: "c_value" },
    }
  }
 */

class TenantFeatureFlags {
public:
    TenantFeatureFlags() {}
    TenantFeatureFlags(TenantFeatureFlags const&) = delete;
    TenantFeatureFlags(TenantFeatureFlags&&) = delete;
    TenantFeatureFlags operator=(TenantFeatureFlags&&) = delete;
    TenantFeatureFlags operator=(TenantFeatureFlags const&) = delete;

    mongo::BSONObj testOnlyGetFeatureFlags() const;
    void updateFeatureFlags(const mongo::BSONObj&);
    std::chrono::time_point<std::chrono::system_clock> getLastModifiedTime() const;
    StreamProcessorFeatureFlags getStreamProcessorFeatureFlags(const std::string&) const;

private:
    mongo::BSONObj _tenantFeatureFlags;
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("TenantFeatureFlags::mutex");
    std::chrono::time_point<std::chrono::system_clock> _updateTimestamp;
};

}  // namespace streams
