/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/rpc/metadata/client_metadata.h"

namespace mongo::audit {
namespace {
constexpr auto kClientMetadata = "clientMetadata"_sd;
constexpr auto kActivityOther = 99;

}  // namespace

void audit::AuditOCSF::logClientMetadata(Client* client) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kNetworkActivity,
         ocsf::OCSFEventClass::kNetworkActivity,
         kActivityOther,
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             if (auto clientMetadata = ClientMetadata::getForClient(client)) {
                 builder->append(ocsf::kUnmappedFieldName, clientMetadata->getDocument());
             }
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
