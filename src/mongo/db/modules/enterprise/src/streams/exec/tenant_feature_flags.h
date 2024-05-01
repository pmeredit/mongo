#pragma once
#include <chrono>

#include "mongo/bson/bsonobj.h"
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
    TenantFeatureFlags(const mongo::BSONObj& featureFlags);

    StreamProcessorFeatureFlags getStreamProcessorFeatureFlags(const std::string&) const;

private:
    mongo::BSONObj _tenantFeatureFlags;
    std::chrono::time_point<std::chrono::system_clock> _updateTimestamp;
};

}  // namespace streams
