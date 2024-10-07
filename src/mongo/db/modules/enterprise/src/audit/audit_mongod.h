/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/base/string_data.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
class OperationContext;

namespace audit {
class AuditConfigDocument;

// Durable storage for configuration.
// config.settings collection, _id=='audit'
constexpr auto kConfigDB = "config"_sd;
constexpr auto kSettingsCollection = "settings"_sd;
constexpr auto kAuditDocID = "audit"_sd;
}  // namespace audit
}  // namespace mongo
