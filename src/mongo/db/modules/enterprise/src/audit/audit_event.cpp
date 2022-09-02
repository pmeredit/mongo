/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"

#include "audit_manager.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/multitenancy.h"

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

ImpersonatedClientAttrs::ImpersonatedClientAttrs(Client* client) {
    if (auto as = AuthorizationSession::get(client)) {
        auto userName = as->getImpersonatedUserName();
        auto roleNamesIt = as->getImpersonatedRoleNames();
        if (!userName) {
            userName = as->getAuthenticatedUserName();
            roleNamesIt = as->getAuthenticatedRoleNames();
        }
        if (userName) {
            this->userName = std::move(userName.value());
        }
        for (; roleNamesIt.more(); roleNamesIt.next()) {
            this->roleNames.emplace_back(roleNamesIt.get());
        }
    }
}

AuditEvent::AuditEvent(Client* client,
                       AuditEventType aType,
                       Serializer serializer,
                       ErrorCodes::Error result)
    : _ts(Date_t::now()) {
    BSONObjBuilder builder;

    builder.append(kATypeField, AuditEventType_serializer(aType));
    if (!getGlobalAuditManager()->getEncryptionEnabled()) {
        builder.append(kTimestampField, _ts);
    }

    // NOTE(SERVER-63142): This is done to allow for logging in contexts where the
    // client is not available, such as in the signal handlers
    if (client != nullptr) {
        if (auto opCtx = client->getOperationContext()) {
            if (auto tenant = getActiveTenant(opCtx)) {
                tenant.value().serializeToBSON(kTenantField, &builder);
            }
        }

        serializeClient(client, &builder);
    }

    BSONObjBuilder paramBuilder(builder.subobjStart(kParamField));
    if (serializer) {
        serializer(&paramBuilder);
    }
    paramBuilder.doneFast();

    builder.append(kResultField, result);

    _obj = builder.obj<BSONObj::LargeSizeTrait>();
}

void AuditEvent::serializeClient(Client* client, BSONObjBuilder* builder) {
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
            auto local = session->localAddr();
            invariant(local.isValid());
            // local: {ip: '127.0.0.1', port: 27017} or {unix: '/var/run/mongodb.sock'}
            local.serializeToBSON(kLocalEndpointField, builder);
        }

        {
            auto remote = session->remoteAddr();
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

ElementIterator* AuditEvent::allocateIterator(const ElementPath* path) const {
    if (_iteratorUsed) {
        return new BSONElementIterator(path, _obj);
    }

    _iteratorUsed = true;
    _iterator.reset(path, _obj);
    return &_iterator;
}

void AuditEvent::releaseIterator(ElementIterator* iterator) const {
    if (iterator == &_iterator) {
        _iteratorUsed = false;
    } else {
        delete iterator;
    }
}

}  // namespace audit
}  // namespace mongo
