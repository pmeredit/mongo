/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/tenant_feature_flags.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

TenantFeatureFlags::TenantFeatureFlags(const mongo::BSONObj& featureFlags) {
    _tenantFeatureFlags = featureFlags.copy();
    _updateTimestamp = std::chrono::system_clock::now();
}

StreamProcessorFeatureFlags TenantFeatureFlags::getStreamProcessorFeatureFlags(
    const std::string& spName, const LoggingContext& context) const {
    mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags;
    const std::string kStringStreamProcessors = "streamProcessors";
    const std::string kStringValue = "value";
    mongo::Document doc(_tenantFeatureFlags);
    auto it = doc.fieldIterator();
    while (it.more()) {
        auto fld = it.next();
        auto val = fld.second.getDocument().getField(kStringValue);
        auto sp = fld.second.getDocument().getField(kStringStreamProcessors);
        if (!sp.missing()) {
            auto currentSp = sp.getDocument().getField(spName);
            if (!currentSp.missing()) {
                val = currentSp;
            }
        }

        if (!val.missing()) {
            const auto& name = fld.first.toString();
            if (FeatureFlags::validateFeatureFlag(name, val)) {
                featureFlags[name] = val;
            } else {
                LOGV2_WARNING(10262900,
                              "Invalid feature flag",
                              "context"_attr = context,
                              "flagName"_attr = name,
                              "flagValue"_attr = val);
            }
        }
    }
    return StreamProcessorFeatureFlags(std::move(featureFlags), _updateTimestamp);
}

}  // namespace streams
