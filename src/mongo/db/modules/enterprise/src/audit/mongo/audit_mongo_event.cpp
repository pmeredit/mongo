/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_manager.h"
#include "audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/multitenancy.h"
#include "mongo/transport/asio/asio_session_impl.h"

namespace mongo {
namespace audit {

namespace {
template <typename Iter>
void serializeNamesToBSON(Iter names, BSONObjBuilder* builder, StringData nameType) {
    BSONArrayBuilder namesBuilder(builder->subarrayStart(nameType));
    while (names.more()) {
        const auto& name = names.next();
        name.serializeToBSON(&namesBuilder);
    }
}

constexpr auto kATypeField = "atype"_sd;
constexpr auto kTimestampField = "ts"_sd;
constexpr auto kLocalEndpointField = "local"_sd;
constexpr auto kRemoteEndpointField = "remote"_sd;
constexpr auto kTenantField = "tenant"_sd;
constexpr auto kUsersField = "users"_sd;
constexpr auto kRolesField = "roles"_sd;
constexpr auto kParamField = "param"_sd;
constexpr auto kResultField = "result"_sd;
constexpr auto kUuid = "uuid"_sd;
constexpr auto kIsSystemUser = "isSystemUser"_sd;
}  // namespace

AuditMongo::AuditEventMongo::AuditEventMongo(TryLogEventParamsMongo tryLogParams) {
    _init(tryLogParams.client,
          tryLogParams.eventType,
          tryLogParams.serializer,
          tryLogParams.code,
          tryLogParams.tenantId);
}

void AuditMongo::AuditEventMongo::_init(Client* client,
                                        AuditEventType aType,
                                        Serializer serializer,
                                        ErrorCodes::Error result,
                                        const boost::optional<TenantId>& tenantId) {
    AuditInterface::AuditEvent::_ts = Date_t::now();
    BSONObjBuilder builder;

    builder.append(kATypeField, AuditEventType_serializer(aType));
    if (!getGlobalAuditManager()->getEncryptionEnabled()) {
        builder.append(kTimestampField, AuditMongo::AuditEventMongo::_ts);
    }

    if (tenantId) {
        tenantId->serializeToBSON(kTenantField, &builder);
    }

    // NOTE(SERVER-63142): This is done to allow for logging in contexts where the
    // client is not available, such as in the signal handlers
    if (client != nullptr) {
        serializeClient(client, &builder);
    }

    BSONObjBuilder paramBuilder(builder.subobjStart(kParamField));
    if (serializer) {
        serializer(&paramBuilder);
    }
    paramBuilder.doneFast();

    builder.append(kResultField, result);

    AuditInterface::AuditEvent::_obj = builder.obj<BSONObj::LargeSizeTrait>();
}

void AuditMongo::AuditEventMongo::serializeClient(Client* client, BSONObjBuilder* builder) {
    invariant(client);

    client->getUUID().appendToBuilder(builder, kUuid);

    if (client->isFromSystemConnection()) {
        {
            auto localBob = BSONObjBuilder(builder->subobjStart(kLocalEndpointField));
            localBob.appendBool(kIsSystemUser, true);
        }

        {
            auto remoteBob = BSONObjBuilder(builder->subobjStart(kRemoteEndpointField));
            remoteBob.appendBool(kIsSystemUser, true);
        }
    } else if (auto session = client->session()) {
        {
            auto local = dynamic_cast<transport::CommonAsioSession*>(session.get())->localAddr();
            invariant(local.isValid());
            // local: {ip: '127.0.0.1', port: 27017} or {unix: '/var/run/mongodb.sock'}
            local.serializeToBSON(kLocalEndpointField, builder);
        }

        {
            auto remote = dynamic_cast<transport::CommonAsioSession*>(session.get())->remoteAddr();
            invariant(remote.isValid());
            // remote: {ip: '::1', port: 12345} or {unix: '/var/run/mongodb.sock'}
            remote.serializeToBSON(kRemoteEndpointField, builder);
        }
    }

    if (AuthorizationSession::exists(client)) {
        auto as = AuthorizationSession::get(client);
        auto userName = as->getImpersonatedUserName();
        RoleNameIterator roleNames;

        if (userName) {
            roleNames = as->getImpersonatedRoleNames();
        } else {
            userName = as->getAuthenticatedUserName();
            roleNames = as->getAuthenticatedRoleNames();
        }

        // users: [{db: dbname, user: username}]
        BSONArrayBuilder namesBuilder(builder->subarrayStart(kUsersField));
        if (userName) {
            userName->serializeToBSON(&namesBuilder);
        }
        namesBuilder.doneFast();

        // roles: [{db: dbname, role: rolename}, ...]
        serializeNamesToBSON(roleNames, builder, kRolesField);
    }
}

StringData AuditMongo::AuditEventMongo::getTimestampFieldName() const {
    return "ts"_sd;
}

}  // namespace audit
}  // namespace mongo
