/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/tenant_feature_flags.h"

#include "mongo/db/exec/document_value/document.h"

namespace streams {

using namespace mongo;

TenantFeatureFlags::TenantFeatureFlags(const mongo::BSONObj& featureFlags) {
    _tenantFeatureFlags = featureFlags.copy();
    _updateTimestamp = std::chrono::system_clock::now();
}

StreamProcessorFeatureFlags TenantFeatureFlags::getStreamProcessorFeatureFlags(
    const std::string& spName) const {
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
            featureFlags[fld.first.toString()] = val;
        }
    }
    return StreamProcessorFeatureFlags(std::move(featureFlags), _updateTimestamp);
}

}  // namespace streams
