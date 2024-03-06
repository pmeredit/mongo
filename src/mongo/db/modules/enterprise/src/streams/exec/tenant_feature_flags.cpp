/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/tenant_feature_flags.h"

#include "mongo/db/exec/document_value/document.h"

namespace streams {

using namespace mongo;

BSONObj TenantFeatureFlags::testOnlyGetFeatureFlags() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _tenantFeatureFlags.copy();
}

void TenantFeatureFlags::updateFeatureFlags(const mongo::BSONObj& featureFlags) {
    stdx::lock_guard<Latch> lock(_mutex);
    _tenantFeatureFlags = featureFlags.copy();
    _updateTimestamp = std::chrono::system_clock::now();
}

std::chrono::time_point<std::chrono::system_clock> TenantFeatureFlags::getLastModifiedTime() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _updateTimestamp;
}

StreamProcessorFeatureFlags TenantFeatureFlags::getStreamProcessorFeatureFlags(
    const std::string& spName) const {
    stdx::lock_guard<Latch> lock(_mutex);
    mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags;
    const std::string kStringStreamProcessors = "streamProcessors";
    const std::string kStringValue = "value";
    mongo::Document doc(_tenantFeatureFlags);
    auto it = doc.fieldIterator();
    while (it.more()) {
        auto fld = it.next();
        auto val = fld.second.getDocument().getField(kStringValue);
        if (!val.missing()) {
            auto sp = fld.second.getDocument().getField(kStringStreamProcessors);
            if (!sp.missing()) {
                auto currentSp = sp.getDocument().getField(spName);
                if (!currentSp.missing()) {
                    val = currentSp;
                }
            }
        }
        featureFlags[fld.first.toString()] = val;
    }
    return StreamProcessorFeatureFlags(std::move(featureFlags), _updateTimestamp);
}

}  // namespace streams
