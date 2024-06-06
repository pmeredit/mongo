/**
 * Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/mongo/audit_mongo.h"

#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/rpc/metadata/client_metadata.h"
#include "mongo/transport/asio/asio_session_impl.h"

namespace mongo {
namespace {
constexpr auto kLocalEndpointField = "localEndpoint"_sd;
constexpr auto kClientMetadata = "clientMetadata"_sd;
}  // namespace

void audit::AuditMongo::logClientMetadata(Client* client) const {
    auto serializer = [&](BSONObjBuilder* bob) {
        if (auto session = client->session()) {
            if (auto asio = dynamic_cast<transport::CommonAsioSession*>(session.get())) {
                auto local = asio->localAddr();
                invariant(local.isValid());
                // local: {ip: '127.0.0.1', port: 27017} or {unix: '/var/run/mongodb.sock'}
                local.serializeToBSON(kLocalEndpointField, bob);
            }
        }

        if (auto clientMetadata = ClientMetadata::getForClient(client)) {
            bob->append(kClientMetadata, clientMetadata->getDocument());
        }
    };
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client, AuditEventType::kClientMetadata, std::move(serializer), ErrorCodes::OK});
}

}  // namespace mongo
