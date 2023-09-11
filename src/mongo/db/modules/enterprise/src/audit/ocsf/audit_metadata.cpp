/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/ocsf/audit_ocsf.h"

#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/rpc/metadata/client_metadata.h"

namespace mongo::audit {
namespace {
constexpr auto kUnmappedField = "unmapped"_sd;
constexpr auto kClientMetadata = "clientMetadata"_sd;
constexpr auto kActivityOther = 99;
constexpr auto kSeverityInformational = 1;

}  // namespace

void audit::AuditOCSF::logClientMetadata(Client* client) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kNetworkActivity,
         ocsf::OCSFEventClass::kNetworkActivity,
         kActivityOther,
         kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             if (auto clientMetadata = ClientMetadata::getForClient(client)) {
                 builder->append(kUnmappedField, clientMetadata->getDocument());
             }
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
