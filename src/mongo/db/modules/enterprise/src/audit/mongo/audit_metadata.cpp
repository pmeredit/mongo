/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_mongo.h"

#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/rpc/metadata/client_metadata.h"

namespace mongo {
namespace {
constexpr auto kLocalEndpointField = "localEndpoint"_sd;
constexpr auto kClientMetadata = "clientMetadata"_sd;
}  // namespace

void audit::AuditMongo::logClientMetadata(Client* client) const {
    auto serializer = [&](BSONObjBuilder* bob) {
        if (auto session = client->session()) {
            auto local = session->localAddr();
            invariant(local.isValid());
            // local: {ip: '127.0.0.1', port: 27017} or {unix: '/var/run/mongodb.sock'}
            local.serializeToBSON(kLocalEndpointField, bob);
        }

        if (auto clientMetadata = ClientMetadata::getForClient(client)) {
            bob->append(kClientMetadata, clientMetadata->getDocument());
        }
    };
    tryLogEvent(client, AuditEventType::kClientMetadata, std::move(serializer), ErrorCodes::OK);
}

}  // namespace mongo
