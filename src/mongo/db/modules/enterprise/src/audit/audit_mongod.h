/**
 *    Copyright (C) 2021 MongoDB Inc.
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
extern const NamespaceString kSettingsNS;

void upsertConfig(OperationContext* opCtx, const AuditConfigDocument& doc);

}  // namespace audit
}  // namespace mongo
